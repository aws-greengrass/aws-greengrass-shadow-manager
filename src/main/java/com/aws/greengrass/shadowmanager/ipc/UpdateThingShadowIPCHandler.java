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
import com.aws.greengrass.shadowmanager.exception.InvalidRequestParametersException;
import com.aws.greengrass.shadowmanager.exception.ShadowManagerDataException;
import com.aws.greengrass.shadowmanager.ipc.model.AcceptRequest;
import com.aws.greengrass.shadowmanager.ipc.model.Operation;
import com.aws.greengrass.shadowmanager.ipc.model.RejectRequest;
import com.aws.greengrass.shadowmanager.model.ErrorMessage;
import com.aws.greengrass.shadowmanager.model.ResponseMessageBuilder;
import com.aws.greengrass.shadowmanager.model.ShadowDocument;
import com.aws.greengrass.shadowmanager.util.JsonUtil;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import software.amazon.awssdk.aws.greengrass.GeneratedAbstractUpdateThingShadowOperationHandler;
import software.amazon.awssdk.aws.greengrass.model.ConflictError;
import software.amazon.awssdk.aws.greengrass.model.InvalidArgumentsError;
import software.amazon.awssdk.aws.greengrass.model.ServiceError;
import software.amazon.awssdk.aws.greengrass.model.UnauthorizedError;
import software.amazon.awssdk.aws.greengrass.model.UpdateThingShadowRequest;
import software.amazon.awssdk.aws.greengrass.model.UpdateThingShadowResponse;
import software.amazon.awssdk.eventstreamrpc.OperationContinuationHandlerContext;
import software.amazon.awssdk.eventstreamrpc.model.EventStreamJsonMessage;

import java.io.IOException;
import java.time.Instant;
import java.util.Optional;

import static com.aws.greengrass.ipc.common.ExceptionUtil.translateExceptions;
import static com.aws.greengrass.shadowmanager.model.Constants.LOG_SHADOW_NAME_KEY;
import static com.aws.greengrass.shadowmanager.model.Constants.LOG_THING_NAME_KEY;
import static com.aws.greengrass.shadowmanager.model.Constants.SHADOW_DOCUMENT_STATE;
import static software.amazon.awssdk.aws.greengrass.GreengrassCoreIPCService.UPDATE_THING_SHADOW;

/**
 * Handler class with business logic for all UpdateThingShadow requests over IPC.
 */
public class UpdateThingShadowIPCHandler extends GeneratedAbstractUpdateThingShadowOperationHandler {
    private static final Logger logger = LogManager.getLogger(UpdateThingShadowIPCHandler.class);
    private final String serviceName;

    private final ShadowManagerDAO dao;
    private final AuthorizationHandler authorizationHandler;
    private final PubSubClientWrapper pubSubClientWrapper;

    /**
     * IPC Handler class for responding to UpdateThingShadow requests.
     *
     * @param context              topics passed by the Nucleus
     * @param dao                  Local shadow database management
     * @param authorizationHandler The authorization handler
     * @param pubSubClientWrapper  The PubSub client wrapper
     */
    public UpdateThingShadowIPCHandler(
            OperationContinuationHandlerContext context,
            ShadowManagerDAO dao,
            AuthorizationHandler authorizationHandler,
            PubSubClientWrapper pubSubClientWrapper) {
        super(context);
        this.authorizationHandler = authorizationHandler;
        this.dao = dao;
        this.serviceName = context.getAuthenticationData().getIdentityLabel();
        this.pubSubClientWrapper = pubSubClientWrapper;
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
        return translateExceptions(() -> {
            // TODO: Sync this entire function possibly with delete handler as well.
            String thingName = request.getThingName();
            String shadowName = request.getShadowName();
            byte[] updatedDocumentRequestBytes = request.getPayload();
            ShadowDocument currentDocument = null;
            try {
                logger.atTrace("ipc-update-thing-shadow-request")
                        .kv(LOG_THING_NAME_KEY, thingName)
                        .kv(LOG_SHADOW_NAME_KEY, shadowName)
                        .log();

                // Validate that the thing name is valid and the component has proper authorization to perform
                // the shadow update.
                IPCUtil.validateThingNameAndDoAuthorization(authorizationHandler, UPDATE_THING_SHADOW,
                        serviceName, thingName, shadowName);
                if (updatedDocumentRequestBytes == null || updatedDocumentRequestBytes.length == 0) {
                    throw new InvalidRequestParametersException("Missing update payload",
                            ErrorMessage.FORBIDDEN_MESSAGE);
                }

                // Get the current document from the DAO if present and convert it into a ShadowDocument object.
                byte[] currentDocumentBytes = dao.getShadowThing(thingName, shadowName).orElse(new byte[0]);
                currentDocument = new ShadowDocument(currentDocumentBytes);

                // Validate the payload sent in the update shadow request. Validates the following:
                // 1.The payload schema to ensure that the JSON has the correct schema.
                // 2. The state node schema to ensure it's correctness.
                // 3. The depth of the state node to ensure it is within the boundaries.
                // 4. The version of the payload to ensure that its current version + 1.
                JsonUtil.validatePayload(currentDocument, updatedDocumentRequestBytes);
            } catch (AuthorizationException e) {
                logger.atWarn()
                        .setEventType(IPCUtil.LogEvents.UPDATE_THING_SHADOW.code())
                        .setCause(e)
                        .kv(LOG_THING_NAME_KEY, thingName)
                        .kv(LOG_SHADOW_NAME_KEY, shadowName)
                        .log("Not authorized to update shadow");
                pubSubClientWrapper.reject(RejectRequest.builder().thingName(thingName).shadowName(shadowName)
                        .errorMessage(ErrorMessage.UNAUTHORIZED_MESSAGE)
                        .publishOperation(Operation.UPDATE_SHADOW)
                        .build());
                throw new UnauthorizedError(e.getMessage());
            } catch (ConflictError e) {
                logger.atWarn()
                        .setEventType(IPCUtil.LogEvents.UPDATE_THING_SHADOW.code())
                        .setCause(e)
                        .kv(LOG_THING_NAME_KEY, thingName)
                        .kv(LOG_SHADOW_NAME_KEY, shadowName)
                        .log("Conflicting version in shadow update message");
                pubSubClientWrapper.reject(RejectRequest.builder().thingName(thingName).shadowName(shadowName)
                        .errorMessage(ErrorMessage.createVersionConflictMessage())
                        .publishOperation(Operation.UPDATE_SHADOW)
                        .build());
                throw e;
            } catch (InvalidRequestParametersException e) {
                logger.atWarn()
                        .setEventType(IPCUtil.LogEvents.UPDATE_THING_SHADOW.code())
                        .setCause(e)
                        .kv(LOG_THING_NAME_KEY, thingName)
                        .kv(LOG_SHADOW_NAME_KEY, shadowName)
                        .log();
                pubSubClientWrapper.reject(RejectRequest.builder().thingName(thingName).shadowName(shadowName)
                        .errorMessage(e.getErrorMessage())
                        .publishOperation(Operation.UPDATE_SHADOW)
                        .build());
                throw new InvalidArgumentsError(e.getMessage());
            } catch (ShadowManagerDataException | IOException e) {
                throwServiceError(thingName, shadowName, e);
            }

            try {
                JsonNode updateDocumentRequest = JsonUtil.getPayloadJson(updatedDocumentRequestBytes)
                        .orElseThrow(() -> new InvalidRequestParametersException(ErrorMessage
                        .createInvalidPayloadJsonMessage("Update shadow request payload must be an Object")));
                // Generate the new merged document based on the update shadow patch payload.
                ShadowDocument updatedDocument = currentDocument.createNewMergedDocument(updateDocumentRequest);

                // Get the client token if present in the update shadow request.
                Optional<String> clientToken = JsonUtil.getClientToken(updateDocumentRequest);

                // 1. Updates the new document in the DAO.
                // 2. Publishes the message on the delta topic over PubSub if applicable.
                // 3. Publishes the documents message over the documents topic.
                handleUpdate(thingName, shadowName, clientToken, currentDocument, updatedDocument);

                // Build the response object to send over the accepted topic and as the payload in the response object.
                // State node is the same shadow document update payload we received in the update request.
                ObjectNode responseNode = ResponseMessageBuilder.builder()
                        .withVersion(updatedDocument.getVersion())
                        .withClientToken(clientToken)
                        .withTimestamp(Instant.now())
                        .withState(updateDocumentRequest.get(SHADOW_DOCUMENT_STATE))
                        //TODO: Handle metadata
                        //.withMetadata(updatedDocument.getMetadata())
                        .build();
                byte[] responseNodeBytes = JsonUtil.getPayloadBytes(responseNode);

                pubSubClientWrapper.accept(AcceptRequest.builder().thingName(thingName).shadowName(shadowName)
                        .payload(responseNodeBytes)
                        .publishOperation(Operation.UPDATE_SHADOW)
                        .build());

                UpdateThingShadowResponse response = new UpdateThingShadowResponse();
                response.setPayload(responseNodeBytes);
                logger.atDebug()
                        .kv(LOG_THING_NAME_KEY, thingName)
                        .kv(LOG_SHADOW_NAME_KEY, shadowName)
                        .log("Successfully updated shadow");
                return response;
            } catch (ShadowManagerDataException | IOException e) {
                throwServiceError(thingName, shadowName, e);
            }
            return null;
        });
    }

    /**
     * Raises a Service error based on the exception.
     *
     * @param thingName  The thing name.
     * @param shadowName The shadow name.
     * @param e          The Exception thrown
     * @throws ServiceError always
     */
    private void throwServiceError(String thingName, String shadowName, Exception e)
            throws ServiceError {
        logger.atError()
                .setEventType(IPCUtil.LogEvents.UPDATE_THING_SHADOW.code())
                .setCause(e)
                .kv(LOG_THING_NAME_KEY, thingName)
                .kv(LOG_SHADOW_NAME_KEY, shadowName)
                .log("Could not process UpdateThingShadow Request due to internal service error");
        pubSubClientWrapper.reject(RejectRequest.builder().thingName(thingName).shadowName(shadowName)
                .errorMessage(ErrorMessage.createInternalServiceErrorMessage())
                .publishOperation(Operation.UPDATE_SHADOW)
                .build());
        throw new ServiceError(e.getMessage());
    }

    /**
     * Handles the Shadow update by sending messages over PubSub for accepted, delta, documents and rejected topics
     * as well as handles the update of the shadow document in the DAO.
     *
     * @param thingName       The thing name.
     * @param shadowName      The name of the shadow.
     * @param clientToken     The client token if present in the update shadow request.
     * @param sourceDocument  The shadow document currently in the DAO.
     * @param updatedDocument The updated shadow document.
     * @throws IOException  if there is any issue while serializing/deserializing the shadow document.
     * @throws ServiceError if there was an issue while updating the shadow in the DAO.
     */
    private void handleUpdate(String thingName, String shadowName, Optional<String> clientToken,
                              ShadowDocument sourceDocument, ShadowDocument updatedDocument)
            throws IOException, ServiceError {
        dao.updateShadowThing(thingName, shadowName, JsonUtil.getPayloadBytes(updatedDocument.toJson()))
                .orElseThrow(() -> {
                    ServiceError error = new ServiceError("Unexpected error occurred in trying to "
                            + "update shadow thing.");
                    logger.atError()
                            .setEventType(IPCUtil.LogEvents.UPDATE_THING_SHADOW.code())
                            .kv(LOG_THING_NAME_KEY, thingName)
                            .kv(LOG_SHADOW_NAME_KEY, shadowName)
                            .setCause(error)
                            .log();
                    pubSubClientWrapper.reject(RejectRequest.builder().thingName(thingName)
                            .shadowName(shadowName)
                            .errorMessage(ErrorMessage.createInternalServiceErrorMessage())
                            .publishOperation(Operation.UPDATE_SHADOW)
                            .build());
                    return error;
                });
        publishDeltaMessage(thingName, shadowName, clientToken, updatedDocument);
        publishDocumentsMessage(thingName, shadowName, clientToken, sourceDocument, updatedDocument);
    }

    private void publishDeltaMessage(String thingName, String shadowName, Optional<String> clientToken,
                                     ShadowDocument updatedDocument)
            throws IOException {
        Optional<JsonNode> delta = updatedDocument.getDelta();
        // Only send the delta if there is any difference in the desired and reported states.
        if (delta.isPresent()) {
            JsonNode responseMessage = ResponseMessageBuilder.builder()
                    .withClientToken(clientToken)
                    .withTimestamp(Instant.now())
                    .withState(delta.get())
                    .withVersion(updatedDocument.getVersion())
                    .build();

            pubSubClientWrapper.delta(AcceptRequest.builder().thingName(thingName)
                    .shadowName(shadowName)
                    .payload(JsonUtil.getPayloadBytes(responseMessage))
                    .publishOperation(Operation.UPDATE_SHADOW)
                    .build());
        }
    }

    private void publishDocumentsMessage(String thingName, String shadowName, Optional<String> clientToken,
                                         ShadowDocument sourceDocument, ShadowDocument updatedDocument)
            throws IOException {
        JsonNode responseMessage = ResponseMessageBuilder.builder()
                .withClientToken(clientToken)
                .withTimestamp(Instant.now())
                .withPrevious(sourceDocument.isNewDocument() ? null : sourceDocument.toJson())
                .withCurrent(updatedDocument.toJson())
                .build();
        // Send the current document on the documents topic after successfully updating the shadow document.
        pubSubClientWrapper.documents(AcceptRequest.builder().thingName(thingName).shadowName(shadowName)
                .payload(JsonUtil.getPayloadBytes(responseMessage))
                .publishOperation(Operation.UPDATE_SHADOW)
                .build());

    }


    @Override
    public void handleStreamEvent(EventStreamJsonMessage streamRequestEvent) {
        //NA
    }
}
