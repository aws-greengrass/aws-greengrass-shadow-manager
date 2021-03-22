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
import com.aws.greengrass.shadowmanager.ipc.model.AcceptRequest;
import com.aws.greengrass.shadowmanager.ipc.model.Operation;
import com.aws.greengrass.shadowmanager.ipc.model.RejectRequest;
import com.aws.greengrass.shadowmanager.model.ErrorMessage;
import software.amazon.awssdk.aws.greengrass.GeneratedAbstractDeleteThingShadowOperationHandler;
import software.amazon.awssdk.aws.greengrass.model.DeleteThingShadowRequest;
import software.amazon.awssdk.aws.greengrass.model.DeleteThingShadowResponse;
import software.amazon.awssdk.aws.greengrass.model.InvalidArgumentsError;
import software.amazon.awssdk.aws.greengrass.model.ResourceNotFoundError;
import software.amazon.awssdk.aws.greengrass.model.ServiceError;
import software.amazon.awssdk.aws.greengrass.model.UnauthorizedError;
import software.amazon.awssdk.eventstreamrpc.OperationContinuationHandlerContext;
import software.amazon.awssdk.eventstreamrpc.model.EventStreamJsonMessage;

import static com.aws.greengrass.ipc.common.ExceptionUtil.translateExceptions;
import static software.amazon.awssdk.aws.greengrass.GreengrassCoreIPCService.DELETE_THING_SHADOW;

/**
 * Handler class with business logic for all DeleteThingShadow requests over IPC.
 */
public class DeleteThingShadowIPCHandler extends GeneratedAbstractDeleteThingShadowOperationHandler {
    private static final Logger logger = LogManager.getLogger(DeleteThingShadowIPCHandler.class);
    private final String serviceName;

    private final ShadowManagerDAO dao;
    private final AuthorizationHandler authorizationHandler;
    private final PubSubClientWrapper pubSubClientWrapper;

    /**
     * IPC Handler class for responding to DeleteThingShadow requests.
     *
     * @param context                   topics passed by the Nucleus
     * @param dao                       Local shadow database management
     * @param authorizationHandler      The authorization handler
     * @param pubSubClientWrapper       The PubSub client wrapper
     */
    public DeleteThingShadowIPCHandler(
            OperationContinuationHandlerContext context,
            ShadowManagerDAO dao,
            AuthorizationHandler authorizationHandler,
            PubSubClientWrapper pubSubClientWrapper) {
        super(context);
        this.authorizationHandler = authorizationHandler;
        this.dao = dao;
        this.pubSubClientWrapper = pubSubClientWrapper;
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
        return translateExceptions(() -> {
            String thingName = request.getThingName();
            String shadowName = request.getShadowName();

            try {
                logger.atTrace("ipc-update-thing-shadow-request").log();

                DeleteThingShadowResponse response = new DeleteThingShadowResponse();
                IPCUtil.validateThingNameAndDoAuthorization(authorizationHandler, DELETE_THING_SHADOW,
                        serviceName, thingName, shadowName);

                byte[] result = dao.deleteShadowThing(thingName, shadowName)
                        .orElseThrow(() -> {
                            ResourceNotFoundError rnf = new ResourceNotFoundError("No shadow found");
                            rnf.setResourceType(IPCUtil.SHADOW_RESOURCE_TYPE);
                            logger.atWarn()
                                    .setEventType(IPCUtil.LogEvents.DELETE_THING_SHADOW.code())
                                    .setCause(rnf)
                                    .kv(IPCUtil.LOG_THING_NAME_KEY, thingName)
                                    .kv(IPCUtil.LOG_SHADOW_NAME_KEY, shadowName)
                                    .log("Unable to process delete shadow since shadow does not exist");
                            pubSubClientWrapper.reject(RejectRequest.builder().thingName(thingName)
                                    .shadowName(shadowName)
                                    .errorMessage(ErrorMessage.createShadowNotFoundMessage(shadowName))
                                    .publishOperation(Operation.DELETE_SHADOW)
                                    .build());
                            return rnf;
                        });

                logger.atDebug()
                        .kv(IPCUtil.LOG_THING_NAME_KEY, thingName)
                        .kv(IPCUtil.LOG_SHADOW_NAME_KEY, shadowName)
                        .log("Successfully delete shadow");
                response.setPayload(result);

                pubSubClientWrapper.accept(AcceptRequest.builder().thingName(thingName).shadowName(shadowName)
                        .payload(new byte[0]).publishOperation(Operation.DELETE_SHADOW)
                        .publishOperation(Operation.DELETE_SHADOW)
                        .build());
                return response;

            } catch (AuthorizationException e) {
                logger.atWarn()
                        .setEventType(IPCUtil.LogEvents.DELETE_THING_SHADOW.code())
                        .setCause(e)
                        .kv(IPCUtil.LOG_THING_NAME_KEY, thingName)
                        .kv(IPCUtil.LOG_SHADOW_NAME_KEY, shadowName)
                        .log("Not authorized to update shadow");
                pubSubClientWrapper.reject(RejectRequest.builder().thingName(thingName).shadowName(shadowName)
                        .errorMessage(ErrorMessage.UNAUTHORIZED_MESSAGE)
                        .publishOperation(Operation.DELETE_SHADOW)
                        .build());
                throw new UnauthorizedError(e.getMessage());
            } catch (IllegalArgumentException e) {
                logger.atWarn()
                        .setEventType(IPCUtil.LogEvents.DELETE_THING_SHADOW.code())
                        .setCause(e)
                        .kv(IPCUtil.LOG_THING_NAME_KEY, thingName)
                        .kv(IPCUtil.LOG_SHADOW_NAME_KEY, shadowName)
                        .log();
                // TODO: Get the Error Message based on the exception message we get from the validate.
                pubSubClientWrapper.reject(RejectRequest.builder().thingName(thingName).shadowName(shadowName)
                        .errorMessage(ErrorMessage.INVALID_CLIENT_TOKEN_MESSAGE)
                        .publishOperation(Operation.DELETE_SHADOW)
                        .build());
                throw new InvalidArgumentsError(e.getMessage());
            } catch (ShadowManagerDataException e) {
                logger.atError()
                        .setEventType(IPCUtil.LogEvents.DELETE_THING_SHADOW.code())
                        .setCause(e)
                        .kv(IPCUtil.LOG_THING_NAME_KEY, thingName)
                        .kv(IPCUtil.LOG_SHADOW_NAME_KEY, shadowName)
                        .log("Could not process UpdateThingShadow Request due to internal service error");
                pubSubClientWrapper.reject(RejectRequest.builder().thingName(thingName).shadowName(shadowName)
                        .errorMessage(ErrorMessage.createInternalServiceErrorMessage())
                        .publishOperation(Operation.DELETE_SHADOW)
                        .build());
                throw new ServiceError(e.getMessage());
            }
        });
    }

    @Override
    public void handleStreamEvent(EventStreamJsonMessage streamRequestEvent) {

    }
}
