/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.0. You may not use this file
 * except in compliance with the Zeebe Community License 1.0.
 */
package io.zeebe.broker.transport;

import io.zeebe.broker.transport.backpressure.RequestLimiter;
import io.zeebe.broker.transport.commandapi.CommandApiRequestResponseService;
import io.zeebe.servicecontainer.ServiceName;
import io.zeebe.transport.ServerTransport;

public class TransportServiceNames {
  public static final ServiceName<CommandApiRequestResponseService> COMMAND_API_MESSAGE_HANDLER =
      ServiceName.newServiceName(
          "transport.commandApi.messageHandler", CommandApiRequestResponseService.class);

  public static final String COMMAND_API_SERVER_NAME = "commandApi.server";
  public static final ServiceName<RequestLimiter> REQUEST_LIMITER_SERVICE =
      ServiceName.newServiceName("transport.commandApu.requestLimiter", RequestLimiter.class);

  public static ServiceName<ServerTransport> serverTransport(String identifier) {
    return ServiceName.newServiceName(
        String.format("transport.%s.server", identifier), ServerTransport.class);
  }
}
