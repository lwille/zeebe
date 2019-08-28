/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.0. You may not use this file
 * except in compliance with the Zeebe Community License 1.0.
 */
package io.zeebe.broker.transport;

import static io.zeebe.broker.clustering.base.ClusterBaseLayerServiceNames.LEADER_PARTITION_GROUP_NAME;
import static io.zeebe.broker.transport.TransportServiceNames.COMMAND_API_MESSAGE_HANDLER;
import static io.zeebe.broker.transport.TransportServiceNames.COMMAND_API_SERVER_NAME;

import com.netflix.concurrency.limits.limit.VegasLimit;
import io.zeebe.broker.system.Component;
import io.zeebe.broker.system.SystemContext;
import io.zeebe.broker.system.configuration.NetworkCfg;
import io.zeebe.broker.transport.backpressure.PartitionAwareRequestLimiter;
import io.zeebe.broker.transport.commandapi.CommandApiRequestResponseService;
import io.zeebe.servicecontainer.ServiceContainer;
import io.zeebe.transport.ServerMessageHandler;
import io.zeebe.transport.ServerRequestHandler;
import io.zeebe.transport.ServerTransport;
import io.zeebe.transport.SocketAddress;
import io.zeebe.util.ByteValue;
import io.zeebe.util.sched.future.ActorFuture;
import java.net.InetSocketAddress;

public class TransportComponent implements Component {
  @Override
  public void init(final SystemContext context) {
    createSocketBindings(context);
  }

  private PartitionAwareRequestLimiter createRequestLimiter() {
    final PartitionAwareRequestLimiter limiter =
        new PartitionAwareRequestLimiter(() -> VegasLimit.newDefault());
    return limiter;
  }

  private void createSocketBindings(final SystemContext context) {
    final NetworkCfg networkCfg = context.getBrokerConfiguration().getNetwork();
    final ServiceContainer serviceContainer = context.getServiceContainer();

    final PartitionAwareRequestLimiter limiter = createRequestLimiter();

    final CommandApiRequestResponseService commandHandler =
        new CommandApiRequestResponseService(limiter);
    serviceContainer
        .createService(COMMAND_API_MESSAGE_HANDLER, commandHandler)
        .dependency(
            TransportServiceNames.serverTransport(TransportServiceNames.COMMAND_API_SERVER_NAME),
            commandHandler.getServerTransportInjector())
        .groupReference(
            LEADER_PARTITION_GROUP_NAME, commandHandler.getLeaderParitionsGroupReference())
        .install();

    final ActorFuture<ServerTransport> commandApiFuture =
        bindNonBufferingProtocolEndpoint(
            context,
            serviceContainer,
            COMMAND_API_SERVER_NAME,
            networkCfg,
            commandHandler.getServerRequestHandler(),
            commandHandler.getServerMessageHandler());

    context.addRequiredStartAction(commandApiFuture);
  }

  protected ActorFuture<ServerTransport> bindNonBufferingProtocolEndpoint(
      final SystemContext systemContext,
      final ServiceContainer serviceContainer,
      final String name,
      final NetworkCfg networkCfg,
      final ServerRequestHandler requestHandler,
      final ServerMessageHandler messageHandler) {

    final SocketAddress bindAddr = networkCfg.getCommandApi().toSocketAddress();

    return createServerTransport(
        systemContext,
        serviceContainer,
        name,
        bindAddr.toInetSocketAddress(),
        networkCfg.getMaxMessageSize(),
        requestHandler,
        messageHandler);
  }

  protected ActorFuture<ServerTransport> createServerTransport(
      final SystemContext systemContext,
      final ServiceContainer serviceContainer,
      final String name,
      final InetSocketAddress bindAddress,
      final ByteValue maxMessageSize,
      final ServerRequestHandler requestHandler,
      final ServerMessageHandler messageHandler) {
    final ServerTransportService service =
        new ServerTransportService(
            name, bindAddress, maxMessageSize, requestHandler, messageHandler);

    systemContext.addResourceReleasingDelegate(service.getReleasingResourcesDelegate());

    return serviceContainer
        .createService(TransportServiceNames.serverTransport(name), service)
        .install();
  }
}
