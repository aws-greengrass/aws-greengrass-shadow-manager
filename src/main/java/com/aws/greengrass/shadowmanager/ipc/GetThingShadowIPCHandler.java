/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.shadowmanager.ipc;

import com.aws.greengrass.logging.api.Logger;
import com.aws.greengrass.logging.impl.LogManager;
import com.aws.greengrass.shadowmanager.exception.ThrottledRequestException;
import com.aws.greengrass.shadowmanager.model.LogEvents;
import software.amazon.awssdk.aws.greengrass.GeneratedAbstractGetThingShadowOperationHandler;
import software.amazon.awssdk.aws.greengrass.model.GetThingShadowRequest;
import software.amazon.awssdk.aws.greengrass.model.GetThingShadowResponse;
import software.amazon.awssdk.aws.greengrass.model.InvalidArgumentsError;
import software.amazon.awssdk.aws.greengrass.model.ResourceNotFoundError;
import software.amazon.awssdk.aws.greengrass.model.ServiceError;
import software.amazon.awssdk.aws.greengrass.model.UnauthorizedError;
import software.amazon.awssdk.eventstreamrpc.OperationContinuationHandlerContext;
import software.amazon.awssdk.eventstreamrpc.model.EventStreamJsonMessage;

import static com.aws.greengrass.shadowmanager.model.Constants.LOG_SHADOW_NAME_KEY;
import static com.aws.greengrass.shadowmanager.model.Constants.LOG_THING_NAME_KEY;

/**
 * Handler class with business logic for all GetThingShadow requests over IPC.
 */
public class GetThingShadowIPCHandler extends GeneratedAbstractGetThingShadowOperationHandler {
    private static final Logger logger = LogManager.getLogger(GetThingShadowIPCHandler.class);

    private final String serviceName;
    private final InboundRateLimiter inboundRateLimiter;
    private final GetThingShadowRequestHandler handler;

    /**
     * IPC Handler class for responding to GetThingShadow requests.
     *
     * @param context            topics passed by the Nucleus
     * @param inboundRateLimiter the rate limiter for local shadow requests
     * @param handler            handler class to handle the Get Shadow request.
     */
    public GetThingShadowIPCHandler(OperationContinuationHandlerContext context,
                                    InboundRateLimiter inboundRateLimiter,
                                    GetThingShadowRequestHandler handler) {
        super(context);
        this.inboundRateLimiter = inboundRateLimiter;
        this.handler = handler;
        this.serviceName = context.getAuthenticationData().getIdentityLabel();
    }

    @Override
    protected void onStreamClosed() {
        //NA
    }

    /**
     * Handles GetThingShadow Requests from IPC.
     *
     * @param request GetThingShadow request from IPC API
     * @return GetThingShadow response
     * @throws ResourceNotFoundError if requested document is not found locally
     * @throws UnauthorizedError     if GetThingShadow call not authorized
     * @throws InvalidArgumentsError if validation error occurred with supplied request fields
     * @throws ServiceError          if database error occurs
     */
    @Override
    @SuppressWarnings("PMD.PreserveStackTrace")
    public GetThingShadowResponse handleRequest(GetThingShadowRequest request) {
        try {
            // TODO: refactor request handler class so that SyncHandler can retry throttled requests
            inboundRateLimiter.acquireLockForThing(request.getThingName());
        } catch (ThrottledRequestException e) {
            logger.atWarn()
                    .setEventType(LogEvents.GET_THING_SHADOW.code())
                    .setCause(e)
                    .kv(LOG_THING_NAME_KEY, request.getThingName())
                    .kv(LOG_SHADOW_NAME_KEY, request.getShadowName())
                    .log();
            throw new ServiceError("Too Many Requests");
        }

        return this.handler.handleRequest(request, serviceName);
    }

    @Override
    public void handleStreamEvent(EventStreamJsonMessage streamRequestEvent) {
        //NA
    }
}
