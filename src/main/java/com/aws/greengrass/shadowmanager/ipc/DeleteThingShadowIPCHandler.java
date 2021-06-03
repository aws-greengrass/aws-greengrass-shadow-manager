/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.shadowmanager.ipc;


import com.aws.greengrass.logging.api.Logger;
import com.aws.greengrass.logging.impl.LogManager;
import com.aws.greengrass.shadowmanager.exception.ThrottledRequestException;
import com.aws.greengrass.shadowmanager.model.LogEvents;
import software.amazon.awssdk.aws.greengrass.GeneratedAbstractDeleteThingShadowOperationHandler;
import software.amazon.awssdk.aws.greengrass.model.DeleteThingShadowRequest;
import software.amazon.awssdk.aws.greengrass.model.DeleteThingShadowResponse;
import software.amazon.awssdk.aws.greengrass.model.InvalidArgumentsError;
import software.amazon.awssdk.aws.greengrass.model.ResourceNotFoundError;
import software.amazon.awssdk.aws.greengrass.model.ServiceError;
import software.amazon.awssdk.aws.greengrass.model.UnauthorizedError;
import software.amazon.awssdk.eventstreamrpc.OperationContinuationHandlerContext;
import software.amazon.awssdk.eventstreamrpc.model.EventStreamJsonMessage;

import static com.aws.greengrass.shadowmanager.model.Constants.LOG_SHADOW_NAME_KEY;
import static com.aws.greengrass.shadowmanager.model.Constants.LOG_THING_NAME_KEY;

/**
 * Handler class with business logic for all DeleteThingShadow requests over IPC.
 */
public class DeleteThingShadowIPCHandler extends GeneratedAbstractDeleteThingShadowOperationHandler {
    private static final Logger logger = LogManager.getLogger(DeleteThingShadowIPCHandler.class);

    private final String serviceName;
    private final InboundRateLimiter inboundRateLimiter;
    private final DeleteThingShadowRequestHandler handler;

    /**
     * IPC Handler class for responding to DeleteThingShadow requests.
     *
     * @param context            topics passed by the Nucleus
     * @param inboundRateLimiter the rate limiter for local shadow requests
     * @param handler            handler class to handle the Delete Shadow request.
     */
    public DeleteThingShadowIPCHandler(OperationContinuationHandlerContext context,
                                       InboundRateLimiter inboundRateLimiter,
                                       DeleteThingShadowRequestHandler handler) {
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
     * Handles DeleteThingShadow Requests from IPC.
     *
     * @param request DeleteThingShadow request from IPC API
     * @return DeleteThingShadow response
     * @throws ResourceNotFoundError if requested document is not found locally
     * @throws UnauthorizedError     if DeleteThingShadow call not authorized
     * @throws InvalidArgumentsError if validation error occurred with supplied request fields
     * @throws ServiceError          if database error occurs
     */
    @Override
    @SuppressWarnings("PMD.PreserveStackTrace")
    public DeleteThingShadowResponse handleRequest(DeleteThingShadowRequest request) {
        try {
            // GG_NEEDS_REVIEW: TODO: refactor request handler class so that SyncHandler can retry throttled requests
            inboundRateLimiter.acquireLockForThing(request.getThingName());
        } catch (ThrottledRequestException e) {
            logger.atWarn()
                    .setEventType(LogEvents.DELETE_THING_SHADOW.code())
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
