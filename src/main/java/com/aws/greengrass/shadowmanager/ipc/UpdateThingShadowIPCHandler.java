/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.shadowmanager.ipc;

import software.amazon.awssdk.aws.greengrass.GeneratedAbstractUpdateThingShadowOperationHandler;
import software.amazon.awssdk.aws.greengrass.model.ConflictError;
import software.amazon.awssdk.aws.greengrass.model.InvalidArgumentsError;
import software.amazon.awssdk.aws.greengrass.model.ServiceError;
import software.amazon.awssdk.aws.greengrass.model.UnauthorizedError;
import software.amazon.awssdk.aws.greengrass.model.UpdateThingShadowRequest;
import software.amazon.awssdk.aws.greengrass.model.UpdateThingShadowResponse;
import software.amazon.awssdk.eventstreamrpc.OperationContinuationHandlerContext;
import software.amazon.awssdk.eventstreamrpc.model.EventStreamJsonMessage;

/**
 * Handler class with business logic for all UpdateThingShadow requests over IPC.
 */
public class UpdateThingShadowIPCHandler extends GeneratedAbstractUpdateThingShadowOperationHandler {
    private final String serviceName;

    private final UpdateThingShadowRequestHandler handler;

    /**
     * IPC Handler class for responding to UpdateThingShadow requests.
     *
     * @param context topics passed by the Nucleus.
     * @param handler Update handler class to handle the Update Shadow request.
     */
    public UpdateThingShadowIPCHandler(OperationContinuationHandlerContext context,
                                       UpdateThingShadowRequestHandler handler) {
        super(context);
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
    public UpdateThingShadowResponse handleRequest(UpdateThingShadowRequest request) {
        return this.handler.handleRequest(request, serviceName);
    }

    @Override
    public void handleStreamEvent(EventStreamJsonMessage streamRequestEvent) {
        //NA
    }
}
