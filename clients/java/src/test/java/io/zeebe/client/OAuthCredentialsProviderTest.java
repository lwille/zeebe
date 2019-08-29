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
package io.zeebe.client;

import static com.github.tomakehurst.wiremock.client.WireMock.equalTo;
import static com.github.tomakehurst.wiremock.client.WireMock.equalToJson;
import static com.github.tomakehurst.wiremock.client.WireMock.postRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.verify;
import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.times;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.github.tomakehurst.wiremock.client.WireMock;
import com.github.tomakehurst.wiremock.junit.WireMockRule;
import io.grpc.Metadata;
import io.grpc.Metadata.Key;
import io.grpc.ServerCall;
import io.grpc.ServerInterceptors;
import io.grpc.Status;
import io.grpc.testing.GrpcServerRule;
import io.zeebe.client.api.command.ClientException;
import io.zeebe.client.impl.OAuthCredentialsProviderBuilder;
import io.zeebe.client.impl.ZeebeClientBuilderImpl;
import io.zeebe.client.impl.ZeebeClientImpl;
import io.zeebe.client.util.RecordingGatewayService;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.net.MalformedURLException;
import java.time.Duration;
import java.util.function.BiConsumer;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

public class OAuthCredentialsProviderTest {

  private static final ObjectMapper YAML_MAPPER = new ObjectMapper(new YAMLFactory());
  private static final Key<String> AUTH_KEY =
      Key.of("Authorization", Metadata.ASCII_STRING_MARSHALLER);
  private static final String SCOPE = "grpc";
  private static final long EXPIRES_IN = Duration.ofDays(1).getSeconds();
  private static final String SECRET = "secret";
  private static final String AUDIENCE = "endpoint";
  private static final String ACCESS_TOKEN = "someToken";
  private static final String TOKEN_TYPE = "Bearer";
  private static final String CLIENT_ID = "client";
  @Rule public final GrpcServerRule serverRule = new GrpcServerRule();
  @Rule public final TemporaryFolder tempFolder = new TemporaryFolder();

  @Rule
  public final WireMockRule wireMockRule =
      new WireMockRule(wireMockConfig().dynamicPort().dynamicPort());

  private final RecordingInterceptor recordingInterceptor = new RecordingInterceptor();
  private final RecordingGatewayService gatewayService = new RecordingGatewayService();
  private ZeebeClient client;

  @Before
  public void setUp() {
    serverRule
        .getServiceRegistry()
        .addService(ServerInterceptors.intercept(gatewayService, recordingInterceptor));
  }

  @After
  public void tearDown() {
    if (client != null) {
      client.close();
      client = null;
    }

    recordingInterceptor.reset();
  }

  @Test
  public void shouldRequestTokenAndAddToCall() throws IOException {
    // given
    mockCredentials(ACCESS_TOKEN);

    final ZeebeClientBuilderImpl builder = new ZeebeClientBuilderImpl();
    builder
        .usePlaintext()
        .credentialsProvider(
            new OAuthCredentialsProviderBuilder()
                .clientId(CLIENT_ID)
                .clientSecret(SECRET)
                .audience(AUDIENCE)
                .authorizationServerUrl("http://localhost:" + wireMockRule.port() + "/oauth/token")
                .credentialsCachePath(tempFolder.newFile().getPath())
                .build());
    client = new ZeebeClientImpl(builder, serverRule.getChannel());

    // when
    client.newTopologyRequest().send().join();

    // then
    assertThat(recordingInterceptor.getCapturedHeaders().get(AUTH_KEY))
        .isEqualTo(TOKEN_TYPE + " " + ACCESS_TOKEN);
  }

  @Test
  public void shouldRetryRequestWithNewCredentials() throws IOException {
    // given
    final String firstToken = "firstToken";
    mockCredentials(firstToken);
    final BiConsumer<ServerCall, Metadata> interceptAction =
        Mockito.spy(
            new BiConsumer<ServerCall, Metadata>() {
              @Override
              public void accept(ServerCall call, Metadata headers) {
                mockCredentials(ACCESS_TOKEN);
                recordingInterceptor.reset();
                call.close(Status.UNAUTHENTICATED, headers);
              }
            });

    recordingInterceptor.setInterceptAction(interceptAction);

    final ZeebeClientBuilderImpl builder = new ZeebeClientBuilderImpl();
    builder
        .usePlaintext()
        .credentialsProvider(
            new OAuthCredentialsProviderBuilder()
                .clientId(CLIENT_ID)
                .clientSecret(SECRET)
                .audience(AUDIENCE)
                .authorizationServerUrl("http://localhost:" + wireMockRule.port() + "/oauth/token")
                .credentialsCachePath(tempFolder.newFile().getPath())
                .build());

    client = new ZeebeClientImpl(builder, serverRule.getChannel());

    // when
    client.newTopologyRequest().send().join();

    // then
    final ArgumentCaptor<Metadata> captor = ArgumentCaptor.forClass(Metadata.class);

    Mockito.verify(interceptAction, times(1)).accept(any(ServerCall.class), captor.capture());
    assertThat(captor.getValue().get(AUTH_KEY)).isEqualTo(TOKEN_TYPE + " " + firstToken);
    assertThat(recordingInterceptor.getCapturedHeaders().get(AUTH_KEY))
        .isEqualTo(TOKEN_TYPE + " " + ACCESS_TOKEN);
  }

  @Test
  public void shouldNotRetryWithSameCredentials() throws IOException {
    // given
    mockCredentials(ACCESS_TOKEN);
    final BiConsumer<ServerCall, Metadata> interceptAction =
        Mockito.spy(
            new BiConsumer<ServerCall, Metadata>() {
              @Override
              public void accept(ServerCall call, Metadata headers) {
                call.close(Status.UNAUTHENTICATED, headers);
              }
            });

    recordingInterceptor.setInterceptAction(interceptAction);

    final ZeebeClientBuilderImpl builder = new ZeebeClientBuilderImpl();
    builder
        .usePlaintext()
        .credentialsProvider(
            new OAuthCredentialsProviderBuilder()
                .clientId(CLIENT_ID)
                .clientSecret(SECRET)
                .audience(AUDIENCE)
                .authorizationServerUrl("http://localhost:" + wireMockRule.port() + "/oauth/token")
                .credentialsCachePath(tempFolder.newFile().getPath())
                .build());

    client = new ZeebeClientImpl(builder, serverRule.getChannel());

    // when
    assertThatThrownBy(() -> client.newTopologyRequest().send().join())
        .isInstanceOf(ClientException.class);
    Mockito.verify(interceptAction, times(1)).accept(any(ServerCall.class), any(Metadata.class));
  }

  @Test
  public void shouldUseCachedCredentials() throws IOException {
    // given
    mockCredentials(ACCESS_TOKEN);
    final String cachePath = tempFolder.getRoot().getPath() + File.separator + ".credsCache";

    try (FileWriter w = new FileWriter(cachePath)) {
      w.write(
          AUDIENCE
              + ":\n"
              + "  auth:\n"
              + "    credentials:\n"
              + "      access_token: "
              + ACCESS_TOKEN
              + "\n"
              + "      expires_in: "
              + EXPIRES_IN
              + "\n"
              + "      token_type: "
              + TOKEN_TYPE);
    }

    final ZeebeClientBuilderImpl builder = new ZeebeClientBuilderImpl();
    builder
        .usePlaintext()
        .credentialsProvider(
            new OAuthCredentialsProviderBuilder()
                .clientId(CLIENT_ID)
                .clientSecret(SECRET)
                .audience(AUDIENCE)
                .authorizationServerUrl("http://localhost:" + wireMockRule.port() + "/oauth/token")
                .credentialsCachePath(cachePath)
                .build());
    client = new ZeebeClientImpl(builder, serverRule.getChannel());

    // when
    client.newTopologyRequest().send().join();

    // then
    assertThat(recordingInterceptor.getCapturedHeaders().get(AUTH_KEY))
        .isEqualTo(TOKEN_TYPE + " " + ACCESS_TOKEN);
    verify(0, postRequestedFor(WireMock.urlPathEqualTo("/oauth/token")));
  }

  @Test
  public void shouldCacheAndReuseCredentials() throws IOException {
    // given
    mockCredentials(ACCESS_TOKEN);
    final String cachePath = tempFolder.getRoot().getPath() + File.separator + ".credsCache";

    final ZeebeClientBuilderImpl builder = new ZeebeClientBuilderImpl();
    final OAuthCredentialsProviderBuilder credsBuilder =
        new OAuthCredentialsProviderBuilder()
            .clientId(CLIENT_ID)
            .clientSecret(SECRET)
            .audience(AUDIENCE)
            .authorizationServerUrl("http://localhost:" + wireMockRule.port() + "/oauth/token")
            .credentialsCachePath(cachePath);
    builder.usePlaintext().credentialsProvider(credsBuilder.build());
    client = new ZeebeClientImpl(builder, serverRule.getChannel());

    // when
    client.newTopologyRequest().send().join();
    verify(1, postRequestedFor(WireMock.urlPathEqualTo("/oauth/token")));

    builder.usePlaintext().credentialsProvider(credsBuilder.build());
    client = new ZeebeClientImpl(builder, serverRule.getChannel());
    client.newTopologyRequest().send().join();

    // then
    assertThat(recordingInterceptor.getCapturedHeaders().get(AUTH_KEY))
        .isEqualTo(TOKEN_TYPE + " " + ACCESS_TOKEN);
    verify(1, postRequestedFor(WireMock.urlPathEqualTo("/oauth/token")));
    assertCacheContents(cachePath);
  }

  @Test
  public void shouldUpdateCacheIfStale() throws IOException {
    // given
    mockCredentials(ACCESS_TOKEN);
    recordingInterceptor.setInterceptAction(
        new BiConsumer<ServerCall, Metadata>() {
          @Override
          public void accept(ServerCall call, Metadata metadata) {
            final String authHeader = metadata.get(AUTH_KEY);
            if (authHeader != null && authHeader.endsWith("staleToken")) {
              call.close(Status.UNAUTHENTICATED, metadata);
            }
          }
        });
    final String cachePath = tempFolder.getRoot().getPath() + File.separator + ".credsCache";

    try (FileWriter w = new FileWriter(cachePath)) {
      w.write(
          AUDIENCE
              + ":\n"
              + "  auth:\n"
              + "    credentials:\n"
              + "      access_token: staleToken\n"
              + "      expires_in: "
              + EXPIRES_IN
              + "\n"
              + "      token_type: "
              + TOKEN_TYPE);
    }

    final ZeebeClientBuilderImpl builder = new ZeebeClientBuilderImpl();
    builder
        .usePlaintext()
        .credentialsProvider(
            new OAuthCredentialsProviderBuilder()
                .clientId(CLIENT_ID)
                .clientSecret(SECRET)
                .audience(AUDIENCE)
                .authorizationServerUrl("http://localhost:" + wireMockRule.port() + "/oauth/token")
                .credentialsCachePath(cachePath)
                .build());
    client = new ZeebeClientImpl(builder, serverRule.getChannel());

    // when
    client.newTopologyRequest().send().join();

    // then
    assertThat(recordingInterceptor.getCapturedHeaders().get(AUTH_KEY))
        .isEqualTo(TOKEN_TYPE + " " + ACCESS_TOKEN);
    verify(1, postRequestedFor(WireMock.urlPathEqualTo("/oauth/token")));
    assertCacheContents(cachePath);
  }

  @Test
  public void shouldFailWithNoAudience() {
    // when/then
    assertThatThrownBy(
            () ->
                new OAuthCredentialsProviderBuilder()
                    .clientId("a")
                    .clientSecret("b")
                    .authorizationServerUrl("http://some.url")
                    .build())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageEndingWith(
            String.format(OAuthCredentialsProviderBuilder.INVALID_ARGUMENT_MSG, "audience"));
  }

  @Test
  public void shouldFailWithNoClientId() {
    // when/then
    assertThatThrownBy(
            () ->
                new OAuthCredentialsProviderBuilder()
                    .audience("a")
                    .clientSecret("b")
                    .authorizationServerUrl("http://some.url")
                    .build())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageEndingWith(
            String.format(OAuthCredentialsProviderBuilder.INVALID_ARGUMENT_MSG, "client id"));
  }

  @Test
  public void shouldFailWithNoClientSecret() {
    // when/then
    assertThatThrownBy(
            () ->
                new OAuthCredentialsProviderBuilder()
                    .audience("a")
                    .clientId("b")
                    .authorizationServerUrl("http://some.url")
                    .build())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageEndingWith(
            String.format(OAuthCredentialsProviderBuilder.INVALID_ARGUMENT_MSG, "client secret"));
  }

  @Test
  public void shouldFailWithNoAuthServerUrl() {
    // when/then
    assertThatThrownBy(
            () ->
                new OAuthCredentialsProviderBuilder()
                    .audience("a")
                    .clientId("b")
                    .clientSecret("c")
                    .build())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageEndingWith(
            String.format(
                OAuthCredentialsProviderBuilder.INVALID_ARGUMENT_MSG, "authorization server URL"));
  }

  @Test
  public void shouldFailWithMalformedServerUrl() {
    // when/then
    assertThatThrownBy(
            () ->
                new OAuthCredentialsProviderBuilder()
                    .audience("a")
                    .clientId("b")
                    .clientSecret("c")
                    .authorizationServerUrl("someServerUrl")
                    .build())
        .isInstanceOf(IllegalArgumentException.class)
        .hasCauseInstanceOf(MalformedURLException.class);
  }

  @Test
  public void shouldFailIfSpecifiedCacheIsDir() {
    // given
    final String cachePath = tempFolder.getRoot().getAbsolutePath() + File.separator + "404_folder";
    new File(cachePath).mkdir();

    // when/then
    assertThatThrownBy(
            () ->
                new OAuthCredentialsProviderBuilder()
                    .audience(AUDIENCE)
                    .clientId(CLIENT_ID)
                    .clientSecret(SECRET)
                    .authorizationServerUrl("http://localhost")
                    .credentialsCachePath(cachePath)
                    .build())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage(
            "Expected specified credentials cache to be a file but found directory instead.");
  }

  /**
   * Mocks an authorization server that returns credentials with the provided access token. Returns
   * the credentials to be return by the server.
   */
  private void mockCredentials(final String accessToken) {
    wireMockRule.stubFor(
        WireMock.post(WireMock.urlPathEqualTo("/oauth/token"))
            .withHeader("Accept", equalTo("application/json"))
            .withRequestBody(
                equalToJson(
                    "{\"client_secret\":\""
                        + SECRET
                        + "\",\"client_id\":\""
                        + CLIENT_ID
                        + "\",\"audience\": \""
                        + AUDIENCE
                        + "\",\"grant_type\": \"client_credentials\"}"))
            .willReturn(
                WireMock.okJson(
                    "{\"access_token\":\""
                        + accessToken
                        + "\",\"token_type\":\""
                        + TOKEN_TYPE
                        + "\",\"expires_in\":"
                        + EXPIRES_IN
                        + ",\"scope\":\""
                        + SCOPE
                        + "\"}")));
  }

  private void assertCacheContents(String cachePath) throws IOException {
    final JsonNode rootNode = YAML_MAPPER.readTree(new File(cachePath));
    assertThat(rootNode.isMissingNode()).isFalse();

    final JsonNode endpointNode = rootNode.get(AUDIENCE);
    assertThat(endpointNode.isMissingNode()).isFalse();

    final JsonNode authNode = endpointNode.get("auth");
    assertThat(authNode.isMissingNode()).isFalse();

    final JsonNode credsNode = authNode.get("credentials");
    assertThat(credsNode.isMissingNode()).isFalse();

    final JsonNode accessToken = credsNode.get("access_token");
    assertThat(accessToken.isValueNode()).isTrue();
    assertThat(accessToken.textValue()).isEqualTo(ACCESS_TOKEN);

    final JsonNode tokenType = credsNode.get("token_type");
    assertThat(tokenType.isValueNode()).isTrue();
    assertThat(tokenType.textValue()).isEqualTo(TOKEN_TYPE);

    final JsonNode expiresIn = credsNode.get("expires_in");
    assertThat(expiresIn.isValueNode()).isTrue();
    assertThat(expiresIn.asLong()).isEqualTo(EXPIRES_IN);

    final JsonNode scope = credsNode.get("scope");
    assertThat(scope.isValueNode()).isTrue();
    assertThat(scope.textValue()).isEqualTo(SCOPE);
  }
}
