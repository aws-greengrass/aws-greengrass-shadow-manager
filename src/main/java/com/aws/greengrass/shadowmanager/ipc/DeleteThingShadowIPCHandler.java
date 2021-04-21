/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.shadowmanager.ipc;

import software.amazon.awssdk.aws.greengrass.GeneratedAbstractDeleteThingShadowOperationHandler;
import software.amazon.awssdk.aws.greengrass.model.DeleteThingShadowRequest;
import software.amazon.awssdk.aws.greengrass.model.DeleteThingShadowResponse;
import software.amazon.awssdk.aws.greengrass.model.InvalidArgumentsError;
import software.amazon.awssdk.aws.greengrass.model.ResourceNotFoundError;
import software.amazon.awssdk.aws.greengrass.model.ServiceError;
import software.amazon.awssdk.aws.greengrass.model.UnauthorizedError;
import software.amazon.awssdk.eventstreamrpc.OperationContinuationHandlerContext;
import software.amazon.awssdk.eventstreamrpc.model.EventStreamJsonMessage;

/**
 * Handler class with business logic for all DeleteThingShadow requests over IPC.
 */
public class DeleteThingShadowIPCHandler extends GeneratedAbstractDeleteThingShadowOperationHandler {
    private final String serviceName;
    private final DeleteThingShadowRequestHandler handler;

    /**
     * IPC Handler class for responding to DeleteThingShadow requests.
     *
     * @param context                     topics passed by the Nucleus
     * @param handler Update handler class to handle the Update Shadow request.
     */
    public DeleteThingShadowIPCHandler(OperationContinuationHandlerContext context,
                                       DeleteThingShadowRequestHandler handler) {
        super(context);
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
    public DeleteThingShadowResponse handleRequest(DeleteThingShadowRequest request) {
        return this.handler.handleRequest(request, serviceName);
    }

    @Override
    public void handleStreamEvent(EventStreamJsonMessage streamRequestEvent) {
        //NA
    }
}
