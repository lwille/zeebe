/*
 * Copyright © 2017 camunda services GmbH (info@camunda.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.zeebe.client.impl;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.google.common.io.CharStreams;
import io.grpc.Metadata;
import io.grpc.Metadata.Key;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.zeebe.client.CredentialsProvider;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OAuthCredentialsProvider implements CredentialsProvider {
  private static final ObjectMapper YAML_MAPPER = new ObjectMapper(new YAMLFactory());
  private static final ObjectMapper JSON_MAPPER = new ObjectMapper();
  private static final ObjectReader CREDENTIALS_READER =
      JSON_MAPPER.readerFor(ZeebeClientCredentials.class);
  private static final Logger LOG = LoggerFactory.getLogger(OAuthCredentialsProvider.class);
  private static final Key<String> HEADER_AUTH_KEY =
      Key.of("Authorization", Metadata.ASCII_STRING_MARSHALLER);

  private final URL authorizationServerUrl;
  private final String jsonPayload;
  private final String endpoint;
  private final File credentialsCache;

  private ZeebeClientCredentials credentials;

  OAuthCredentialsProvider(OAuthCredentialsProviderBuilder builder) {
    authorizationServerUrl = builder.getAuthorizationServer();
    credentialsCache = builder.getCredentialsCache();
    endpoint = builder.getAudience();
    jsonPayload = createJsonPayload(builder);
  }

  /** Adds an access token to the Authorization header of a gRPC call. */
  @Override
  public void applyCredentials(Metadata headers) {
    try {
      if (credentials == null) {
        loadCredentials();
      }

      headers.put(
          HEADER_AUTH_KEY,
          String.format(
              "%s %s", credentials.getTokenType().trim(), credentials.getAccessToken().trim()));
    } catch (IOException | NullPointerException e) {
      LOG.warn("Failed while fetching credentials, will not add credentials to rpc: ", e);
    }
  }

  /**
   * @return true if the Throwable was caused by an UNAUTHENTICATED response and a new access token
   *     could be fetched; otherwise returns false.
   */
  @Override
  public boolean shouldRetryRequest(Throwable throwable) {
    try {
      return throwable instanceof StatusRuntimeException
          && ((StatusRuntimeException) throwable).getStatus() == Status.UNAUTHENTICATED
          && forceRefresh();
    } catch (IOException e) {
      LOG.error("Failed while fetching credentials: ", e);
      return false;
    }
  }

  /** Attempt to load credentials from cache and, if unsuccessful, fetch new credentials. */
  private void loadCredentials() throws IOException {
    final ZeebeClientCredentials cachedCredentials = getCredentialsFromCache();

    if (cachedCredentials == null) {
      forceRefresh();
    } else {
      this.credentials = cachedCredentials;
    }
  }

  /**
   * Fetch new credentials from authorization server and store them in cache.
   *
   * @return true if the fetched credentials are different from the previously stored ones,
   *     otherwise returns false.
   */
  private boolean forceRefresh() throws IOException {
    final ZeebeClientCredentials fetchedCredentials = fetchCredentials();
    updateCache(fetchedCredentials);

    if (fetchedCredentials.equals(credentials)) {
      return false;
    }

    credentials = fetchedCredentials;
    LOG.debug("Refreshed credentials.");
    return true;
  }

  /**
   * Reads the credentials stored in the cache.
   *
   * @return client credentials if any were present and well-formed, otherwise returns null.
   */
  private ZeebeClientCredentials getCredentialsFromCache() {
    if (!credentialsCache.exists()) {
      return null;
    }

    try {
      final JsonNode rootNode = YAML_MAPPER.readTree(credentialsCache);

      final JsonNode endpointNode = rootNode.get(endpoint);
      if (endpointNode == null) {
        LOG.info("Couldn't find endpoint '{}' in cache.", endpoint);
        return null;
      }

      final JsonNode credentialsNode = endpointNode.findValue("credentials");
      if (credentialsNode == null) {
        LOG.debug(
            "Cache for endpoint '{}' was malformed: doesn't contain 'credentials' field.",
            endpoint);
        return null;
      }

      return YAML_MAPPER.readValue(credentialsNode.toString(), ZeebeClientCredentials.class);
    } catch (IOException e) {
      LOG.debug("Couldn't load cached credentials:", e);
    }

    return null;
  }

  private void updateCache(ZeebeClientCredentials credentials) {
    try {
      ensureCacheExists();

      JsonNode root = YAML_MAPPER.readTree(credentialsCache);
      if (root.isMissingNode()) {
        root = YAML_MAPPER.createObjectNode();
      }

      ((ObjectNode) root).remove(endpoint);

      final JsonNode creds =
          YAML_MAPPER.createObjectNode().set("credentials", YAML_MAPPER.valueToTree(credentials));
      final JsonNode auth = YAML_MAPPER.createObjectNode().set("auth", creds);
      ((ObjectNode) root).set(endpoint, auth);

      YAML_MAPPER.writeValue(credentialsCache, root);
    } catch (IOException e) {
      LOG.debug("Couldn't update cache: ", e);
    }
  }

  private void ensureCacheExists() throws IOException {
    if (!credentialsCache.exists()) {
      credentialsCache.getParentFile().mkdirs();

      if (!credentialsCache.createNewFile()) {
        throw new IOException(
            String.format(
                "Cache file doesn't exist and couldn't be created at '%s'.",
                credentialsCache.getAbsolutePath()));
      }
    }
  }

  private static String createJsonPayload(OAuthCredentialsProviderBuilder builder) {
    try {
      final Map<String, String> payload = new HashMap<>();
      payload.put("client_id", builder.getClientId());
      payload.put("client_secret", builder.getClientSecret());
      payload.put("audience", builder.getAudience());
      payload.put("grant_type", "client_credentials");

      return JSON_MAPPER.writeValueAsString(payload);
    } catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }

  private ZeebeClientCredentials fetchCredentials() throws IOException {
    final HttpURLConnection connection =
        (HttpURLConnection) authorizationServerUrl.openConnection();
    connection.setRequestMethod("POST");
    connection.setRequestProperty("Content-Type", "application/json");
    connection.setRequestProperty("Accept", "application/json");
    connection.setDoOutput(true);

    try (OutputStream os = connection.getOutputStream()) {
      final byte[] input = jsonPayload.getBytes(StandardCharsets.UTF_8);
      os.write(input, 0, input.length);
    }

    if (connection.getResponseCode() != 200) {
      throw new IOException(
          String.format(
              "Failed while requesting access token with status code %d and message %s.",
              connection.getResponseCode(), connection.getResponseMessage()));
    }

    try (InputStream in = connection.getInputStream();
        InputStreamReader reader = new InputStreamReader(in, StandardCharsets.UTF_8)) {

      final ZeebeClientCredentials fetchedCredentials =
          CREDENTIALS_READER.readValue(CharStreams.toString(reader));

      if (fetchedCredentials == null) {
        throw new IOException("Expected valid credentials but got null instead.");
      }

      return fetchedCredentials;
    }
  }
}
