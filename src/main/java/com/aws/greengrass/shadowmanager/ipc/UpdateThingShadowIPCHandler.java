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
import software.amazon.awssdk.aws.greengrass.GeneratedAbstractUpdateThingShadowOperationHandler;
import software.amazon.awssdk.aws.greengrass.model.ConflictError;
import software.amazon.awssdk.aws.greengrass.model.InvalidArgumentsError;
import software.amazon.awssdk.aws.greengrass.model.ServiceError;
import software.amazon.awssdk.aws.greengrass.model.UnauthorizedError;
import software.amazon.awssdk.aws.greengrass.model.UpdateThingShadowRequest;
import software.amazon.awssdk.aws.greengrass.model.UpdateThingShadowResponse;
import software.amazon.awssdk.eventstreamrpc.OperationContinuationHandlerContext;
import software.amazon.awssdk.eventstreamrpc.model.EventStreamJsonMessage;

import static com.aws.greengrass.ipc.common.ExceptionUtil.translateExceptions;
import static software.amazon.awssdk.aws.greengrass.GreengrassCoreIPCService.UPDATE_THING_SHADOW;

/**
 * Handler class with business logic for all UpdateThingShadow requests over IPC.
 */
public class UpdateThingShadowIPCHandler extends GeneratedAbstractUpdateThingShadowOperationHandler {
    private static final Logger logger = LogManager.getLogger(UpdateThingShadowIPCHandler.class);
    private final String serviceName;

    private final ShadowManagerDAO dao;
    private final AuthorizationHandler authorizationHandler;

    /**
     * IPC Handler class for responding to UpdateThingShadow requests.
     *
     * @param context              topics passed by the Nucleus
     * @param dao                  Local shadow database management
     * @param authorizationHandler The authorization handler
     */
    public UpdateThingShadowIPCHandler(
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
     * Handles UpdateThingShadow Requests from IPC.
     * TODO: Need to implement conflict resolution after pending discussions
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
        return translateExceptions(() -> {
            String thingName = request.getThingName();
            String shadowName = request.getShadowName();
            byte[] payload = request.getPayload();

            try {
                logger.atTrace("ipc-update-thing-shadow-request").log();

                IPCUtil.validateThingNameAndDoAuthorization(authorizationHandler, UPDATE_THING_SHADOW,
                        serviceName, thingName, shadowName);
                if (payload == null || payload.length == 0) {
                    throw new InvalidArgumentsError("Missing update payload");
                }
                validatePayloadVersion(thingName, shadowName, payload);

                byte[] result = dao.updateShadowThing(thingName, shadowName, payload)
                        .orElseThrow(() -> {
                            ServiceError error = new ServiceError("Unexpected error occurred in trying to "
                                    + "update shadow thing.");
                            logger.atError()
                                    .setEventType(IPCUtil.LogEvents.GET_THING_SHADOW.code())
                                    .setCause(error)
                                    .log("Could not process UpdateThingShadow Request for "
                                            + "thingName: {}, shadowName: {}", thingName, shadowName);
                            return error;
                        });

                UpdateThingShadowResponse response = new UpdateThingShadowResponse();
                response.setPayload(result);
                return response;

            } catch (AuthorizationException e) {
                logger.atWarn()
                        .setEventType(IPCUtil.LogEvents.UPDATE_THING_SHADOW.code())
                        .setCause(e)
                        .log("Could not process UpdateThingShadow Request for thingName: {}, shadowName: {}",
                                thingName, shadowName);
                throw new UnauthorizedError(e.getMessage());
            } catch (ConflictError | InvalidArgumentsError e) {
                logger.atInfo()
                        .setEventType(IPCUtil.LogEvents.UPDATE_THING_SHADOW.code())
                        .setCause(e)
                        .log("Could not process UpdateThingShadow Request for thingName: {}, shadowName: {}",
                                thingName, shadowName);
                throw e;
            } catch (ShadowManagerDataException e) {
                logger.atError()
                        .setEventType(IPCUtil.LogEvents.UPDATE_THING_SHADOW.code())
                        .setCause(e)
                        .log("Could not process UpdateThingShadow Request for thingName: {}, shadowName: {}",
                                thingName, shadowName);
                throw new ServiceError(e.getMessage());
            }
        });
    }

    // TODO: Implement version conflict validation
    private void validatePayloadVersion(String thingName, String shadowName, byte[] updatePayload) throws ConflictError{

    }

    @Override
    public void handleStreamEvent(EventStreamJsonMessage streamRequestEvent) {

    }
}
