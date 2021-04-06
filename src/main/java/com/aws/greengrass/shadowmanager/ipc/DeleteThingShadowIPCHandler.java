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
import com.aws.greengrass.shadowmanager.ipc.model.AcceptRequest;
import com.aws.greengrass.shadowmanager.ipc.model.Operation;
import com.aws.greengrass.shadowmanager.ipc.model.RejectRequest;
import com.aws.greengrass.shadowmanager.model.ErrorMessage;
import com.aws.greengrass.shadowmanager.model.LogEvents;
import com.aws.greengrass.shadowmanager.model.ResponseMessageBuilder;
import com.aws.greengrass.shadowmanager.model.ShadowDocument;
import com.aws.greengrass.shadowmanager.model.ShadowRequest;
import com.aws.greengrass.shadowmanager.util.JsonUtil;
import com.fasterxml.jackson.databind.JsonNode;
import software.amazon.awssdk.aws.greengrass.GeneratedAbstractDeleteThingShadowOperationHandler;
import software.amazon.awssdk.aws.greengrass.model.DeleteThingShadowRequest;
import software.amazon.awssdk.aws.greengrass.model.DeleteThingShadowResponse;
import software.amazon.awssdk.aws.greengrass.model.InvalidArgumentsError;
import software.amazon.awssdk.aws.greengrass.model.ResourceNotFoundError;
import software.amazon.awssdk.aws.greengrass.model.ServiceError;
import software.amazon.awssdk.aws.greengrass.model.UnauthorizedError;
import software.amazon.awssdk.eventstreamrpc.OperationContinuationHandlerContext;
import software.amazon.awssdk.eventstreamrpc.model.EventStreamJsonMessage;

import java.io.IOException;
import java.time.Instant;
import java.util.Optional;

import static com.aws.greengrass.ipc.common.ExceptionUtil.translateExceptions;
import static com.aws.greengrass.shadowmanager.model.Constants.LOG_SHADOW_NAME_KEY;
import static com.aws.greengrass.shadowmanager.model.Constants.LOG_THING_NAME_KEY;
import static com.aws.greengrass.shadowmanager.model.Constants.SHADOW_RESOURCE_TYPE;
import static software.amazon.awssdk.aws.greengrass.GreengrassCoreIPCService.DELETE_THING_SHADOW;

/**
 * Handler class with business logic for all DeleteThingShadow requests over IPC.
 */
public class DeleteThingShadowIPCHandler extends GeneratedAbstractDeleteThingShadowOperationHandler {
    private static final Logger logger = LogManager.getLogger(DeleteThingShadowIPCHandler.class);
    private final String serviceName;

    private final ShadowManagerDAO dao;
    private final AuthorizationHandlerWrapper authorizationHandlerWrapper;
    private final PubSubClientWrapper pubSubClientWrapper;

    /**
     * IPC Handler class for responding to DeleteThingShadow requests.
     *
     * @param context                     topics passed by the Nucleus
     * @param dao                         Local shadow database management
     * @param authorizationHandlerWrapper The authorization handler wrapper
     * @param pubSubClientWrapper         The PubSub client wrapper
     */
    public DeleteThingShadowIPCHandler(
            OperationContinuationHandlerContext context,
            ShadowManagerDAO dao,
            AuthorizationHandlerWrapper authorizationHandlerWrapper,
            PubSubClientWrapper pubSubClientWrapper) {
        super(context);
        this.authorizationHandlerWrapper = authorizationHandlerWrapper;
        this.dao = dao;
        this.pubSubClientWrapper = pubSubClientWrapper;
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
        return translateExceptions(() -> {
            String thingName = request.getThingName();
            String shadowName = request.getShadowName();
            //TODO: Add payload to DeleteThingShadowRequest and then validate the version of the document the customer
            //    wants to delete and pass the client token in the response
            byte[] payload = null;

            try {
                logger.atTrace("ipc-update-thing-shadow-request").log();

                ShadowRequest shadowRequest = new ShadowRequest(thingName, shadowName);
                Validator.validateShadowRequest(shadowRequest);
                authorizationHandlerWrapper.doAuthorization(DELETE_THING_SHADOW, serviceName, shadowRequest);

                byte[] result = dao.deleteShadowThing(thingName, shadowName)
                        .orElseThrow(() -> {
                            ResourceNotFoundError rnf = new ResourceNotFoundError("No shadow found");
                            rnf.setResourceType(SHADOW_RESOURCE_TYPE);
                            logger.atWarn()
                                    .setEventType(LogEvents.DELETE_THING_SHADOW.code())
                                    .setCause(rnf)
                                    .kv(LOG_THING_NAME_KEY, thingName)
                                    .kv(LOG_SHADOW_NAME_KEY, shadowName)
                                    .log("Unable to process delete shadow since shadow does not exist");
                            pubSubClientWrapper.reject(RejectRequest.builder().thingName(thingName)
                                    .shadowName(shadowName)
                                    .errorMessage(ErrorMessage.createShadowNotFoundMessage(shadowName))
                                    .publishOperation(Operation.DELETE_SHADOW)
                                    .build());
                            return rnf;
                        });

                logger.atDebug()
                        .kv(LOG_THING_NAME_KEY, thingName)
                        .kv(LOG_SHADOW_NAME_KEY, shadowName)
                        .log("Successfully delete shadow");

                ShadowDocument deletedShadowDocument = new ShadowDocument(result);

                // Get the Client Token if present in the payload.
                Optional<JsonNode> payloadJson = JsonUtil.getPayloadJson(payload);
                Optional<String> clientToken = payloadJson.flatMap(JsonUtil::getClientToken);

                JsonNode responseNode = ResponseMessageBuilder.builder()
                        .withVersion(deletedShadowDocument.getVersion())
                        .withClientToken(clientToken)
                        .withTimestamp(Instant.now())
                        .build();
                pubSubClientWrapper.accept(AcceptRequest.builder()
                        .thingName(thingName)
                        .shadowName(shadowName)
                        .payload(JsonUtil.getPayloadBytes(responseNode))
                        .publishOperation(Operation.DELETE_SHADOW)
                        .build());
                DeleteThingShadowResponse response = new DeleteThingShadowResponse();
                /*
                 After a successful delete, the payload expected over the synchronous operation is an empty response
                 Reference:
                 https://docs.aws.amazon.com/iot/latest/developerguide/device-shadow-rest-api.html#API_DeleteThingShadow
                */
                response.setPayload(new byte[0]);
                return response;

            } catch (AuthorizationException e) {
                logger.atWarn()
                        .setEventType(LogEvents.DELETE_THING_SHADOW.code())
                        .setCause(e)
                        .kv(LOG_THING_NAME_KEY, thingName)
                        .kv(LOG_SHADOW_NAME_KEY, shadowName)
                        .log("Not authorized to update shadow");
                pubSubClientWrapper.reject(RejectRequest.builder().thingName(thingName).shadowName(shadowName)
                        .errorMessage(ErrorMessage.UNAUTHORIZED_MESSAGE)
                        .publishOperation(Operation.DELETE_SHADOW)
                        .build());
                throw new UnauthorizedError(e.getMessage());
            } catch (InvalidRequestParametersException e) {
                logger.atWarn()
                        .setEventType(LogEvents.DELETE_THING_SHADOW.code())
                        .setCause(e)
                        .kv(LOG_THING_NAME_KEY, thingName)
                        .kv(LOG_SHADOW_NAME_KEY, shadowName)
                        .log();
                pubSubClientWrapper.reject(RejectRequest.builder().thingName(thingName).shadowName(shadowName)
                        .errorMessage(e.getErrorMessage())
                        .publishOperation(Operation.DELETE_SHADOW)
                        .build());
                throw new InvalidArgumentsError(e.getMessage());
            } catch (ShadowManagerDataException | IOException e) {
                logger.atError()
                        .setEventType(LogEvents.DELETE_THING_SHADOW.code())
                        .setCause(e)
                        .kv(LOG_THING_NAME_KEY, thingName)
                        .kv(LOG_SHADOW_NAME_KEY, shadowName)
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
        //NA
    }
}