/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.shadowmanager.ipc;

import software.amazon.awssdk.aws.greengrass.GeneratedAbstractGetThingShadowOperationHandler;
import software.amazon.awssdk.aws.greengrass.model.GetThingShadowRequest;
import software.amazon.awssdk.aws.greengrass.model.GetThingShadowResponse;
import software.amazon.awssdk.aws.greengrass.model.InvalidArgumentsError;
import software.amazon.awssdk.aws.greengrass.model.ResourceNotFoundError;
import software.amazon.awssdk.aws.greengrass.model.ServiceError;
import software.amazon.awssdk.aws.greengrass.model.UnauthorizedError;
import software.amazon.awssdk.eventstreamrpc.OperationContinuationHandlerContext;
import software.amazon.awssdk.eventstreamrpc.model.EventStreamJsonMessage;

/**
 * Handler class with business logic for all GetThingShadow requests over IPC.
 */
public class GetThingShadowIPCHandler extends GeneratedAbstractGetThingShadowOperationHandler {
    private final String serviceName;

    private final GetThingShadowRequestHandler handler;

    /**
     * IPC Handler class for responding to GetThingShadow requests.
     *
     * @param context topics passed by the Nucleus
     * @param handler handler class to handle the Get Shadow request.
     */
    public GetThingShadowIPCHandler(OperationContinuationHandlerContext context,
                                    GetThingShadowRequestHandler handler) {
        super(context);
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
        return this.handler.handleRequest(request, serviceName);
    }

    @Override
    public void handleStreamEvent(EventStreamJsonMessage streamRequestEvent) {
        //NA
    }
}
