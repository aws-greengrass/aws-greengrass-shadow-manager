/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.shadowmanager.ipc;

import com.aws.greengrass.authorization.exceptions.AuthorizationException;
import com.aws.greengrass.logging.api.Logger;
import com.aws.greengrass.logging.impl.LogManager;
import com.aws.greengrass.shadowmanager.AuthorizationHandlerWrapper;
import com.aws.greengrass.shadowmanager.ShadowManagerDAO;
import com.aws.greengrass.shadowmanager.exception.InvalidRequestParametersException;
import com.aws.greengrass.shadowmanager.exception.ShadowManagerDataException;
import com.aws.greengrass.shadowmanager.ipc.model.Operation;
import com.aws.greengrass.shadowmanager.ipc.model.PubSubRequest;
import com.aws.greengrass.shadowmanager.model.ErrorMessage;
import com.aws.greengrass.shadowmanager.model.LogEvents;
import com.aws.greengrass.shadowmanager.model.ResponseMessageBuilder;
import com.aws.greengrass.shadowmanager.model.ShadowDocument;
import com.aws.greengrass.shadowmanager.model.ShadowRequest;
import com.aws.greengrass.shadowmanager.sync.SyncHandler;
import com.aws.greengrass.shadowmanager.util.JsonUtil;
import com.aws.greengrass.shadowmanager.util.ShadowWriteSynchronizeHelper;
import com.aws.greengrass.shadowmanager.util.Validator;
import com.fasterxml.jackson.databind.JsonNode;
import software.amazon.awssdk.aws.greengrass.model.DeleteThingShadowRequest;
import software.amazon.awssdk.aws.greengrass.model.DeleteThingShadowResponse;
import software.amazon.awssdk.aws.greengrass.model.InvalidArgumentsError;
import software.amazon.awssdk.aws.greengrass.model.ResourceNotFoundError;
import software.amazon.awssdk.aws.greengrass.model.ServiceError;
import software.amazon.awssdk.aws.greengrass.model.UnauthorizedError;

import java.io.IOException;
import java.time.Instant;
import java.util.Optional;

import static com.aws.greengrass.ipc.common.ExceptionUtil.translateExceptions;
import static com.aws.greengrass.shadowmanager.model.Constants.LOG_SHADOW_NAME_KEY;
import static com.aws.greengrass.shadowmanager.model.Constants.LOG_THING_NAME_KEY;
import static com.aws.greengrass.shadowmanager.model.Constants.SHADOW_RESOURCE_TYPE;
import static software.amazon.awssdk.aws.greengrass.GreengrassCoreIPCService.DELETE_THING_SHADOW;

public class DeleteThingShadowRequestHandler extends BaseRequestHandler {
    private static final Logger logger = LogManager.getLogger(DeleteThingShadowRequestHandler.class);

    private final ShadowManagerDAO dao;
    private final AuthorizationHandlerWrapper authorizationHandlerWrapper;
    private final ShadowWriteSynchronizeHelper synchronizeHelper;
    private final SyncHandler syncHandler;

    /**
     * IPC Handler class for responding to DeleteThingShadow requests.
     *
     * @param dao                         Local shadow database management
     * @param authorizationHandlerWrapper The authorization handler wrapper
     * @param pubSubClientWrapper         The PubSub client wrapper
     * @param synchronizeHelper           The shadow write operation synchronizer helper.
     * @param syncHandler                 The handler class to perform shadow sync operations.
     */
    public DeleteThingShadowRequestHandler(
            ShadowManagerDAO dao,
            AuthorizationHandlerWrapper authorizationHandlerWrapper,
            PubSubClientWrapper pubSubClientWrapper,
            ShadowWriteSynchronizeHelper synchronizeHelper,
            SyncHandler syncHandler) {
        super(pubSubClientWrapper);
        this.authorizationHandlerWrapper = authorizationHandlerWrapper;
        this.dao = dao;
        this.synchronizeHelper = synchronizeHelper;
        this.syncHandler = syncHandler;
    }

    /**
     * Handles DeleteThingShadow Requests from IPC.
     *
     * @param request     DeleteThingShadow request from IPC API
     * @param serviceName the service name making the request.
     * @return DeleteThingShadow response
     * @throws ResourceNotFoundError if requested document is not found locally
     * @throws UnauthorizedError     if DeleteThingShadow call not authorized
     * @throws InvalidArgumentsError if validation error occurred with supplied request fields
     * @throws ServiceError          if database error occurs
     */
    @SuppressWarnings("PMD.PreserveStackTrace")
    public DeleteThingShadowResponse handleRequest(DeleteThingShadowRequest request, String serviceName) {
        return translateExceptions(() -> {
            String thingName = request.getThingName();
            String shadowName = request.getShadowName();
            logger.atTrace("ipc-delete-thing-shadow-request").log();

            ShadowRequest shadowRequest = new ShadowRequest(thingName, shadowName);
            shadowName = shadowRequest.getShadowName();
            try {
                Validator.validateShadowRequest(shadowRequest);
            } catch (InvalidRequestParametersException e) {
                throwInvalidArgumentsError(thingName, shadowName, Optional.empty(), e, Operation.DELETE_SHADOW);
            }

            synchronized (synchronizeHelper.getThingShadowLock(shadowRequest)) {
                try {

                    authorizationHandlerWrapper.doAuthorization(DELETE_THING_SHADOW, serviceName, shadowRequest);

                    Optional<ShadowDocument> deletedShadowDocument = dao.deleteShadowThing(thingName, shadowName);
                    if (!deletedShadowDocument.isPresent()) {
                        ResourceNotFoundError rnf = new ResourceNotFoundError("No shadow found");
                        rnf.setResourceType(SHADOW_RESOURCE_TYPE);
                        logger.atWarn()
                                .setEventType(LogEvents.DELETE_THING_SHADOW.code())
                                .setCause(rnf)
                                .kv(LOG_THING_NAME_KEY, thingName)
                                .kv(LOG_SHADOW_NAME_KEY, shadowName)
                                .log("Unable to process delete shadow since shadow does not exist");
                        publishErrorMessage(thingName, shadowName, Optional.empty(),
                                ErrorMessage.createShadowNotFoundMessage(shadowName), Operation.DELETE_SHADOW);
                        throw rnf;
                    }
                    logger.atDebug()
                            .kv(LOG_THING_NAME_KEY, thingName)
                            .kv(LOG_SHADOW_NAME_KEY, shadowName)
                            .log("Successfully deleted the local shadow");

                    JsonNode responseNode = ResponseMessageBuilder.builder()
                            .withVersion(deletedShadowDocument.get().getVersion())
                            .withTimestamp(Instant.now())
                            .build();
                    getPubSubClientWrapper().accept(PubSubRequest.builder()
                            .thingName(thingName)
                            .shadowName(shadowName)
                            .payload(JsonUtil.getPayloadBytes(responseNode))
                            .publishOperation(Operation.DELETE_SHADOW)
                            .build());
                    DeleteThingShadowResponse response = new DeleteThingShadowResponse();
                    /*
                     After a successful delete, the payload expected over the synchronous operation is an empty response
                     Reference:
                     https://docs.aws.amazon.com/iot/latest/developerguide/device-shadow-rest-api.html
                     #API_DeleteThingShadow
                    */
                    response.setPayload(new byte[0]);
                    this.syncHandler.pushCloudDeleteSyncRequest(thingName, shadowName);
                    return response;

                } catch (AuthorizationException e) {
                    logger.atWarn()
                            .setEventType(LogEvents.DELETE_THING_SHADOW.code())
                            .setCause(e)
                            .kv(LOG_THING_NAME_KEY, thingName)
                            .kv(LOG_SHADOW_NAME_KEY, shadowName)
                            .log("Not authorized to delete shadow");
                    publishErrorMessage(thingName, shadowName, Optional.empty(), ErrorMessage.UNAUTHORIZED_MESSAGE,
                            Operation.DELETE_SHADOW);
                    throw new UnauthorizedError(e.getMessage());
                } catch (ShadowManagerDataException | IOException e) {
                    logger.atError()
                            .setEventType(LogEvents.DELETE_THING_SHADOW.code())
                            .setCause(e)
                            .kv(LOG_THING_NAME_KEY, thingName)
                            .kv(LOG_SHADOW_NAME_KEY, shadowName)
                            .log("Could not process DeleteThingShadow Request due to internal service error");
                    publishErrorMessage(thingName, shadowName, Optional.empty(),
                            ErrorMessage.INTERNAL_SERVICE_FAILURE_MESSAGE, Operation.DELETE_SHADOW);
                    throw new ServiceError(e.getMessage());
                }
            }
        });
    }
}
