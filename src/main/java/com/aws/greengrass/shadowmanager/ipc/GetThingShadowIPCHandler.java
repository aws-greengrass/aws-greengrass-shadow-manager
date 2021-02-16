/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.shadowmanager.ipc;

import com.aws.greengrass.authorization.AuthorizationHandler;
import com.aws.greengrass.authorization.exceptions.AuthorizationException;
import com.aws.greengrass.logging.api.Logger;
import com.aws.greengrass.logging.impl.LogManager;
import com.aws.greengrass.shadowmanager.ShadowManagerDAO;
import com.aws.greengrass.shadowmanager.exception.ShadowManagerDataException;
import software.amazon.awssdk.aws.greengrass.GeneratedAbstractGetThingShadowOperationHandler;
import software.amazon.awssdk.aws.greengrass.model.GetThingShadowRequest;
import software.amazon.awssdk.aws.greengrass.model.GetThingShadowResponse;
import software.amazon.awssdk.aws.greengrass.model.InvalidArgumentsError;
import software.amazon.awssdk.aws.greengrass.model.ResourceNotFoundError;
import software.amazon.awssdk.aws.greengrass.model.ServiceError;
import software.amazon.awssdk.aws.greengrass.model.UnauthorizedError;
import software.amazon.awssdk.eventstreamrpc.OperationContinuationHandlerContext;
import software.amazon.awssdk.eventstreamrpc.model.EventStreamJsonMessage;

import static software.amazon.awssdk.aws.greengrass.GreengrassCoreIPCService.GET_THING_SHADOW;

/**
 * Handler class with business logic for all GetThingShadow requests over IPC.
 */
public class GetThingShadowIPCHandler extends GeneratedAbstractGetThingShadowOperationHandler {
    private static final Logger logger = LogManager.getLogger(GetThingShadowIPCHandler.class);
    private final String serviceName;

    private final ShadowManagerDAO dao;
    private final AuthorizationHandler authorizationHandler;

    /**
     * Handles IPC Requests for GetThingShadow.
     *
     * @param context               topics passed by the Nucleus
     * @param dao             Local shadow database management
     * @param authorizationHandler The authorization handler
     */
    public GetThingShadowIPCHandler(
            OperationContinuationHandlerContext context,
            ShadowManagerDAO dao,
            AuthorizationHandler authorizationHandler) {
        super(context);
        this.authorizationHandler = authorizationHandler;
        this.dao = dao;
        this.serviceName = context.getAuthenticationData().getIdentityLabel();
    }

    @Override
    protected void onStreamClosed() {

    }

    @Override
    public GetThingShadowResponse handleRequest(GetThingShadowRequest request) {
        logger.atDebug("ipc-get-thing-shadow-request").log();
        return handleGetThingShadowIPCRequest(request);
    }

    /**
     * Handles GetThingShadow Requests from IPC.
     *
     * @param request     GetThingShadow request from IPC API
     * @return GetThingShadow response
     * @throws ResourceNotFoundError if requested document is not found locally
     * @throws UnauthorizedError if GetThingShadow call not authorized
     * @throws InvalidArgumentsError if validation error occurred with supplied request fields
     * @throws ServiceError if database error occurs
     */
    public GetThingShadowResponse handleGetThingShadowIPCRequest(GetThingShadowRequest request) {
        try {
            GetThingShadowResponse response = new GetThingShadowResponse();
            String thingName = request.getThingName();
            String shadowName = request.getShadowName();
            IPCUtil.validateThingNameAndDoAuthorization(authorizationHandler, GET_THING_SHADOW,
                    serviceName, thingName, shadowName);

            byte[] result = dao.getShadowThing(thingName, shadowName)
                    .orElseThrow(() -> {
                        ResourceNotFoundError rnf = new ResourceNotFoundError(
                                String.format("No shadow found for thingName: %s, shadowName: %s",
                                        thingName, shadowName));
                        rnf.setResourceType(IPCUtil.SHADOW_RESOURCE_TYPE);
                        logger.atWarn()
                                .setEventType(IPCUtil.LogEvents.DOCUMENT_NOT_FOUND.code())
                                .setCause(rnf)
                                .log(String.format("Could not process GetThingShadow Request: %s", rnf.getMessage()));
                        return rnf;
                    });

            response.setPayload(result);
            return response;

        } catch (AuthorizationException e) {
            logger.atError()
                    .setEventType(IPCUtil.LogEvents.AUTHORIZATION_ERROR.code())
                    .log("Could not process GetThingShadow Request: %s", e.getMessage());
            throw new UnauthorizedError(e.getMessage());
        } catch (InvalidArgumentsError e) {
            logger.atWarn()
                    .setEventType(IPCUtil.LogEvents.INVALID_THING_NAME.code()).setCause(e)
                    .log("Could not process GetThingShadow Request: %s", e.getMessage());
            throw e;
        } catch (ShadowManagerDataException e) {
            logger.atError()
                    .setEventType(IPCUtil.LogEvents.DATABASE_OPERATION_ERROR.code()).setCause(e)
                    .log("Could not process GetThingShadow Request: %s", e.getMessage());
            throw new ServiceError(e.getMessage());
        }
    }

    @Override
    public void handleStreamEvent(EventStreamJsonMessage streamRequestEvent) {

    }
}
