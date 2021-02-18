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
import software.amazon.awssdk.aws.greengrass.GeneratedAbstractDeleteThingShadowOperationHandler;
import software.amazon.awssdk.aws.greengrass.model.DeleteThingShadowRequest;
import software.amazon.awssdk.aws.greengrass.model.DeleteThingShadowResponse;
import software.amazon.awssdk.aws.greengrass.model.InvalidArgumentsError;
import software.amazon.awssdk.aws.greengrass.model.ResourceNotFoundError;
import software.amazon.awssdk.aws.greengrass.model.ServiceError;
import software.amazon.awssdk.aws.greengrass.model.UnauthorizedError;
import software.amazon.awssdk.eventstreamrpc.OperationContinuationHandlerContext;
import software.amazon.awssdk.eventstreamrpc.model.EventStreamJsonMessage;

import static software.amazon.awssdk.aws.greengrass.GreengrassCoreIPCService.DELETE_THING_SHADOW;

/**
 * Handler class with business logic for all DeleteThingShadow requests over IPC.
 */
public class DeleteThingShadowIPCHandler extends GeneratedAbstractDeleteThingShadowOperationHandler {
    private static final Logger logger = LogManager.getLogger(DeleteThingShadowIPCHandler.class);
    private final String serviceName;

    private final ShadowManagerDAO dao;
    private final AuthorizationHandler authorizationHandler;

    /**
     * IPC Handler class for responding to DeleteThingShadow requests.
     *
     * @param context              topics passed by the Nucleus
     * @param dao                  Local shadow database management
     * @param authorizationHandler The authorization handler
     */
    public DeleteThingShadowIPCHandler(
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
        try {
            logger.atTrace("ipc-update-thing-shadow-request").log();

            DeleteThingShadowResponse response = new DeleteThingShadowResponse();
            String thingName = request.getThingName();
            String shadowName = request.getShadowName();
            IPCUtil.validateThingNameAndDoAuthorization(authorizationHandler, DELETE_THING_SHADOW,
                    serviceName, thingName, shadowName);

            byte[] result = dao.deleteShadowThing(thingName, shadowName)
                    .orElseThrow(() -> {
                        ResourceNotFoundError rnf = new ResourceNotFoundError(
                                String.format("No shadow found for thingName: %s, shadowName: %s",
                                        thingName, shadowName));
                        rnf.setResourceType(IPCUtil.SHADOW_RESOURCE_TYPE);
                        logger.atInfo()
                                .setEventType(IPCUtil.LogEvents.DELETE_THING_SHADOW.code())
                                .setCause(rnf)
                                .log("Could not process DeleteThingShadow Request");
                        return rnf;
                    });

            response.setPayload(result);
            return response;

        } catch (AuthorizationException e) {
            logger.atWarn()
                    .setEventType(IPCUtil.LogEvents.DELETE_THING_SHADOW.code())
                    .setCause(e)
                    .log("Could not process DeleteThingShadow Request");
            throw new UnauthorizedError(e.getMessage());
        } catch (InvalidArgumentsError e) {
            logger.atInfo()
                    .setEventType(IPCUtil.LogEvents.DELETE_THING_SHADOW.code())
                    .setCause(e)
                    .log("Could not process DeleteThingShadow Request");
            throw e;
        } catch (ShadowManagerDataException e) {
            logger.atError()
                    .setEventType(IPCUtil.LogEvents.DELETE_THING_SHADOW.code())
                    .setCause(e)
                    .log("Could not process DeleteThingShadow Request");
            throw new ServiceError(e.getMessage());
        }
    }

    @Override
    public void handleStreamEvent(EventStreamJsonMessage streamRequestEvent) {

    }
}
