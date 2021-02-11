/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.shadowmanager;

import com.aws.greengrass.logging.api.Logger;
import com.aws.greengrass.logging.impl.LogManager;
import lombok.AccessLevel;
import lombok.Setter;
import software.amazon.awssdk.aws.greengrass.GeneratedAbstractDeleteThingShadowOperationHandler;
import software.amazon.awssdk.aws.greengrass.GeneratedAbstractGetThingShadowOperationHandler;
import software.amazon.awssdk.aws.greengrass.GeneratedAbstractUpdateThingShadowOperationHandler;
import software.amazon.awssdk.aws.greengrass.model.DeleteThingShadowRequest;
import software.amazon.awssdk.aws.greengrass.model.DeleteThingShadowResponse;
import software.amazon.awssdk.aws.greengrass.model.GetThingShadowRequest;
import software.amazon.awssdk.aws.greengrass.model.GetThingShadowResponse;
import software.amazon.awssdk.aws.greengrass.model.UpdateThingShadowRequest;
import software.amazon.awssdk.aws.greengrass.model.UpdateThingShadowResponse;
import software.amazon.awssdk.eventstreamrpc.OperationContinuationHandlerContext;
import software.amazon.awssdk.eventstreamrpc.model.EventStreamJsonMessage;

import javax.inject.Inject;

/**
 * Class to handle business logic for all ShadowManager requests over IPC.
 */
public class ShadowManagerIPCAgent {
    private static final Logger logger = LogManager.getLogger(ShadowManagerIPCAgent.class);

    @Inject
    @Setter(AccessLevel.PACKAGE)
    ShadowManager shadowManager;

    public GetThingShadowOperationHandler getGetThingShadowOperationHandler(
            OperationContinuationHandlerContext context) {
        return new GetThingShadowOperationHandler(context);
    }

    public UpdateThingShadowOperationHandler getUpdateThingShadowOperationHandler(
            OperationContinuationHandlerContext context) {
        return new UpdateThingShadowOperationHandler(context);
    }

    public DeleteThingShadowOperationHandler getDeleteThingShadowOperationHandler(
            OperationContinuationHandlerContext context) {
        return new DeleteThingShadowOperationHandler(context);
    }

    class GetThingShadowOperationHandler extends GeneratedAbstractGetThingShadowOperationHandler {
        private final String serviceName;

        protected GetThingShadowOperationHandler(OperationContinuationHandlerContext context) {
            super(context);
            serviceName = context.getAuthenticationData().getIdentityLabel();
        }

        @Override
        protected void onStreamClosed() {

        }

        @Override
        public GetThingShadowResponse handleRequest(GetThingShadowRequest request) {
            logger.atDebug("ipc-get-secret-request").log();
            return shadowManager.handleGetThingShadowIPCRequest(request, serviceName);
        }

        @Override
        public void handleStreamEvent(EventStreamJsonMessage streamRequestEvent) {

        }

    }

    class UpdateThingShadowOperationHandler extends GeneratedAbstractUpdateThingShadowOperationHandler {
        private final String serviceName;

        protected UpdateThingShadowOperationHandler(OperationContinuationHandlerContext context) {
            super(context);
            serviceName = context.getAuthenticationData().getIdentityLabel();
        }

        @Override
        protected void onStreamClosed() {

        }

        @Override
        public UpdateThingShadowResponse handleRequest(UpdateThingShadowRequest request) {
            logger.atDebug("ipc-get-secret-request").log();
            return shadowManager.handleUpdateThingShadowIPCRequest(request, serviceName);
        }

        @Override
        public void handleStreamEvent(EventStreamJsonMessage streamRequestEvent) {

        }

    }

    class DeleteThingShadowOperationHandler extends GeneratedAbstractDeleteThingShadowOperationHandler {
        private final String serviceName;

        protected DeleteThingShadowOperationHandler(OperationContinuationHandlerContext context) {
            super(context);
            serviceName = context.getAuthenticationData().getIdentityLabel();
        }

        @Override
        protected void onStreamClosed() {

        }

        @Override
        public DeleteThingShadowResponse handleRequest(DeleteThingShadowRequest request) {
            logger.atDebug("ipc-get-secret-request").log();
            return shadowManager.handleDeleteThingShadowIPCRequest(request, serviceName);
        }

        @Override
        public void handleStreamEvent(EventStreamJsonMessage streamRequestEvent) {

        }

    }

}