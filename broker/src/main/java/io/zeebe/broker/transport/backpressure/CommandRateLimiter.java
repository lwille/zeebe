/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.0. You may not use this file
 * except in compliance with the Zeebe Community License 1.0.
 */
package io.zeebe.broker.transport.backpressure;

import com.netflix.concurrency.limits.limiter.AbstractLimiter;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

public class CommandRateLimiter extends AbstractLimiter<Void> implements RequestLimiter<Void> {

  private final Map<ListenerId, Listener> responseListeners = new ConcurrentHashMap<>();

  protected CommandRateLimiter(CommandRateLimiterBuilder builder) {
    super(builder);
  }

  @Override
  public Optional<Listener> acquire(Void context) {
    if (getInflight() >= getLimit()) {
      return createRejectedListener();
    }
    final Listener listener = createListener();
    return Optional.of(listener);
  }

  private void registerListener(int streamId, long requestId, Listener listener) {
    // assumes the pair <streamId, requestId> is unique.
    responseListeners.put(new ListenerId(streamId, requestId), listener);
  }

  @Override
  public boolean tryAcquire(int streamId, long requestId, Void context) {
    final Optional<Listener> acquired = acquire(null);
    return acquired
        .map(
            listener -> {
              registerListener(streamId, requestId, listener);
              return true;
            })
        .orElse(false);
  }

  @Override
  public void onResponse(int streamId, long requestId) {
    final Listener listener = responseListeners.remove(new ListenerId(streamId, requestId));
    if (listener != null) {
      listener.onSuccess();
    }
  }

  @Override
  public int getInflightCount() {
    return getInflight();
  }

  public static CommandRateLimiterBuilder builder() {
    return new CommandRateLimiterBuilder();
  }

  public static class CommandRateLimiterBuilder
      extends AbstractLimiter.Builder<CommandRateLimiterBuilder> {

    @Override
    protected CommandRateLimiterBuilder self() {
      return this;
    }

    public CommandRateLimiter build() {
      return new CommandRateLimiter(this);
    }
  }

  static class ListenerId {
    private final int streamId;
    private final long requestId;

    ListenerId(int streamId, long requestId) {
      this.streamId = streamId;
      this.requestId = requestId;
    }

    @Override
    public int hashCode() {
      return Objects.hash(streamId, requestId);
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      final ListenerId that = (ListenerId) o;
      return streamId == that.streamId && requestId == that.requestId;
    }
  }
}
