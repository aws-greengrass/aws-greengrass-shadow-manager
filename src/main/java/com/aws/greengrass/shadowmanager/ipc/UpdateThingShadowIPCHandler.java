/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.shadowmanager.ipc;

import com.aws.greengrass.logging.api.Logger;
import com.aws.greengrass.logging.impl.LogManager;
import com.aws.greengrass.shadowmanager.exception.ThrottledRequestException;
import com.aws.greengrass.shadowmanager.model.LogEvents;
import com.aws.greengrass.shadowmanager.model.UpdateThingShadowHandlerResponse;
import software.amazon.awssdk.aws.greengrass.GeneratedAbstractUpdateThingShadowOperationHandler;
import software.amazon.awssdk.aws.greengrass.model.ConflictError;
import software.amazon.awssdk.aws.greengrass.model.InvalidArgumentsError;
import software.amazon.awssdk.aws.greengrass.model.ServiceError;
import software.amazon.awssdk.aws.greengrass.model.UnauthorizedError;
import software.amazon.awssdk.aws.greengrass.model.UpdateThingShadowRequest;
import software.amazon.awssdk.aws.greengrass.model.UpdateThingShadowResponse;
import software.amazon.awssdk.eventstreamrpc.OperationContinuationHandlerContext;
import software.amazon.awssdk.eventstreamrpc.model.EventStreamJsonMessage;

import static com.aws.greengrass.shadowmanager.model.Constants.LOG_SHADOW_NAME_KEY;
import static com.aws.greengrass.shadowmanager.model.Constants.LOG_THING_NAME_KEY;

/**
 * Handler class with business logic for all UpdateThingShadow requests over IPC.
 */
public class UpdateThingShadowIPCHandler extends GeneratedAbstractUpdateThingShadowOperationHandler {
    private static final Logger logger = LogManager.getLogger(UpdateThingShadowIPCHandler.class);

    private final String serviceName;
    private final InboundRateLimiter inboundRateLimiter;
    private final UpdateThingShadowRequestHandler handler;

    /**
     * IPC Handler class for responding to UpdateThingShadow requests.
     *
     * @param context            topics passed by the Nucleus
     * @param inboundRateLimiter the rate limiter for local shadow requests
     * @param handler            handler class to handle the Update Shadow request.
     */
    public UpdateThingShadowIPCHandler(OperationContinuationHandlerContext context,
                                       InboundRateLimiter inboundRateLimiter,
                                       UpdateThingShadowRequestHandler handler) {
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
     * Handles UpdateThingShadow Requests from IPC.
     *
     * @param request UpdateThingShadow request from IPC API
     * @return UpdateThingShadow response
     * @throws ConflictError         if version conflict found when updating shadow document
     * @throws UnauthorizedError     if UpdateThingShadow call not authorized
     * @throws InvalidArgumentsError if validation error occurred with supplied request fields
     * @throws ServiceError          if database error occurs
     */
    @Override
    @SuppressWarnings("PMD.PreserveStackTrace")
    public UpdateThingShadowResponse handleRequest(UpdateThingShadowRequest request) {
        try {
            // TODO: refactor request handler class so that SyncHandler can retry throttled requests
            inboundRateLimiter.acquireLockForThing(request.getThingName());
        } catch (ThrottledRequestException e) {
            logger.atWarn()
                    .setEventType(LogEvents.UPDATE_THING_SHADOW.code())
                    .setCause(e)
                    .kv(LOG_THING_NAME_KEY, request.getThingName())
                    .kv(LOG_SHADOW_NAME_KEY, request.getShadowName())
                    .log();
            throw new ServiceError("Local UpdateThingShadow request throttled");
        }

        UpdateThingShadowHandlerResponse response = this.handler.handleRequest(request, serviceName);
        return response.getUpdateThingShadowResponse();
    }

    @Override
    public void handleStreamEvent(EventStreamJsonMessage streamRequestEvent) {
        //NA
    }
}
