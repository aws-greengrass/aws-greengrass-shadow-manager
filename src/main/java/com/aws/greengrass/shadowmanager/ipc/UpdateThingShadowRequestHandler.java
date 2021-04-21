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
import com.aws.greengrass.shadowmanager.model.Constants;
import com.aws.greengrass.shadowmanager.model.ErrorMessage;
import com.aws.greengrass.shadowmanager.model.LogEvents;
import com.aws.greengrass.shadowmanager.model.ResponseMessageBuilder;
import com.aws.greengrass.shadowmanager.model.ShadowDocument;
import com.aws.greengrass.shadowmanager.model.ShadowRequest;
import com.aws.greengrass.shadowmanager.sync.SyncHandler;
import com.aws.greengrass.shadowmanager.util.JsonUtil;
import com.aws.greengrass.shadowmanager.util.ShadowWriteSynchronizeHelper;
import com.aws.greengrass.shadowmanager.util.Validator;
import com.aws.greengrass.util.Pair;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import software.amazon.awssdk.aws.greengrass.model.ConflictError;
import software.amazon.awssdk.aws.greengrass.model.InvalidArgumentsError;
import software.amazon.awssdk.aws.greengrass.model.ServiceError;
import software.amazon.awssdk.aws.greengrass.model.UnauthorizedError;
import software.amazon.awssdk.aws.greengrass.model.UpdateThingShadowRequest;
import software.amazon.awssdk.aws.greengrass.model.UpdateThingShadowResponse;

import java.io.IOException;
import java.time.Instant;
import java.util.Optional;

import static com.aws.greengrass.ipc.common.ExceptionUtil.translateExceptions;
import static com.aws.greengrass.shadowmanager.model.Constants.LOG_SHADOW_NAME_KEY;
import static com.aws.greengrass.shadowmanager.model.Constants.LOG_THING_NAME_KEY;
import static com.aws.greengrass.shadowmanager.util.JsonUtil.isNullOrMissing;
import static software.amazon.awssdk.aws.greengrass.GreengrassCoreIPCService.UPDATE_THING_SHADOW;

public class UpdateThingShadowRequestHandler {
    private static final Logger logger = LogManager.getLogger(UpdateThingShadowRequestHandler.class);

    private final ShadowManagerDAO dao;
    private final AuthorizationHandlerWrapper authorizationHandlerWrapper;
    private final PubSubClientWrapper pubSubClientWrapper;
    private final ShadowWriteSynchronizeHelper synchronizeHelper;
    private final SyncHandler syncHandler;

    /**
     * IPC Handler class for responding to UpdateThingShadow requests.
     *
     * @param dao                         Local shadow database management
     * @param authorizationHandlerWrapper The authorization handler wrapper
     * @param pubSubClientWrapper         The PubSub client wrapper
     * @param synchronizeHelper           The shadow write operation synchronizer helper.
     * @param syncHandler                 The handler class to perform shadow sync operations.
     */
    public UpdateThingShadowRequestHandler(
            ShadowManagerDAO dao,
            AuthorizationHandlerWrapper authorizationHandlerWrapper,
            PubSubClientWrapper pubSubClientWrapper,
            ShadowWriteSynchronizeHelper synchronizeHelper, SyncHandler syncHandler) {
        this.authorizationHandlerWrapper = authorizationHandlerWrapper;
        this.dao = dao;
        this.pubSubClientWrapper = pubSubClientWrapper;
        this.synchronizeHelper = synchronizeHelper;
        this.syncHandler = syncHandler;
    }


    /**
     * Handles UpdateThingShadow Requests from IPC.
     *
     * @param request     UpdateThingShadow request from IPC API
     * @param serviceName the service name making the request.
     * @return UpdateThingShadow response
     * @throws ConflictError         if version conflict found when updating shadow document
     * @throws UnauthorizedError     if UpdateThingShadow call not authorized
     * @throws InvalidArgumentsError if validation error occurred with supplied request fields
     * @throws ServiceError          if database error occurs
     */
    @SuppressWarnings({"PMD.PreserveStackTrace", "PMD.PrematureDeclaration", "checkstyle:JavadocMethod"})
    public UpdateThingShadowResponse handleRequest(UpdateThingShadowRequest request, String serviceName) {
        return translateExceptions(() -> {
            String thingName = request.getThingName();
            String shadowName = request.getShadowName();
            byte[] updatedDocumentRequestBytes = request.getPayload();
            ShadowDocument currentDocument = null;
            Optional<String> clientToken = Optional.empty();
            JsonNode updateDocumentRequest = null;
            logger.atTrace("ipc-update-thing-shadow-request")
                    .kv(LOG_THING_NAME_KEY, thingName)
                    .kv(LOG_SHADOW_NAME_KEY, shadowName)
                    .log();

            ShadowRequest shadowRequest = new ShadowRequest(thingName, shadowName);
            try {
                Validator.validateShadowRequest(shadowRequest);
            } catch (InvalidRequestParametersException e) {
                throwInvalidArgumentsError(thingName, shadowName, clientToken, e);
            }

            synchronized (synchronizeHelper.getThingShadowLock(shadowRequest)) {
                try {

                    if (updatedDocumentRequestBytes == null || updatedDocumentRequestBytes.length == 0) {
                        throw new InvalidRequestParametersException(ErrorMessage.PAYLOAD_MISSING_MESSAGE);
                    }
                    authorizationHandlerWrapper.doAuthorization(UPDATE_THING_SHADOW, serviceName, shadowRequest);

                    // Get the current document from the DAO if present and convert it into a ShadowDocument object.
                    currentDocument = dao.getShadowThing(thingName, shadowName).orElse(new ShadowDocument());

                    // Validate the payload sent in the update shadow request. Validates the following:
                    // 1.The payload schema to ensure that the JSON has the correct schema.
                    // 2. The state node schema to ensure it's correctness.
                    // 3. The depth of the state node to ensure it is within the boundaries.
                    // 4. The version of the payload to ensure that its current version + 1.

                    // If the payload size is greater than the maximum default shadow document size, then raise an
                    // invalid parameters error for payload too large.
                    //TODO: get the max doc size from config.
                    if (updatedDocumentRequestBytes.length > Constants.DEFAULT_DOCUMENT_SIZE) {
                        throw new InvalidRequestParametersException(ErrorMessage.PAYLOAD_TOO_LARGE_MESSAGE);
                    }
                    updateDocumentRequest = JsonUtil.getPayloadJson(updatedDocumentRequestBytes)
                            .filter(d -> !isNullOrMissing(d))
                            .orElseThrow(() ->
                                    new InvalidRequestParametersException(ErrorMessage
                                            .createInvalidPayloadJsonMessage("")));
                    // Validate the payload schema
                    JsonUtil.validatePayloadSchema(updateDocumentRequest);

                    // Get the client token if present in the update shadow request.
                    clientToken = JsonUtil.getClientToken(updateDocumentRequest);

                    JsonUtil.validatePayload(currentDocument, updateDocumentRequest);
                } catch (AuthorizationException e) {
                    logger.atWarn()
                            .setEventType(LogEvents.UPDATE_THING_SHADOW.code())
                            .setCause(e)
                            .kv(LOG_THING_NAME_KEY, thingName)
                            .kv(LOG_SHADOW_NAME_KEY, shadowName)
                            .log("Not authorized to update shadow");
                    publishErrorMessage(thingName, shadowName, clientToken, ErrorMessage.UNAUTHORIZED_MESSAGE);
                    throw new UnauthorizedError(e.getMessage());
                } catch (ConflictError e) {
                    logger.atWarn()
                            .setEventType(LogEvents.UPDATE_THING_SHADOW.code())
                            .setCause(e)
                            .kv(LOG_THING_NAME_KEY, thingName)
                            .kv(LOG_SHADOW_NAME_KEY, shadowName)
                            .log("Conflicting version in shadow update message");
                    publishErrorMessage(thingName, shadowName, clientToken, ErrorMessage.VERSION_CONFLICT_MESSAGE);
                    throw e;
                } catch (InvalidRequestParametersException e) {
                    throwInvalidArgumentsError(thingName, shadowName, clientToken, e);
                } catch (ShadowManagerDataException | IOException e) {
                    throwServiceError(thingName, shadowName, clientToken, e);
                }

                try {
                    // Generate the new merged document based on the update shadow patch payload.
                    ShadowDocument updatedDocument = new ShadowDocument(currentDocument);
                    final Pair<JsonNode, JsonNode> patchStateMetadataPair = updatedDocument
                            .update(updateDocumentRequest);

                    // Update the new document in the DAO.
                    Optional<byte[]> result = dao.updateShadowThing(thingName, shadowName,
                            JsonUtil.getPayloadBytes(updatedDocument.toJson(false)),
                            updatedDocument.getVersion());
                    if (!result.isPresent()) {
                        ServiceError error = new ServiceError("Unexpected error occurred in trying to "
                                + "update shadow thing.");
                        logger.atError()
                                .setEventType(LogEvents.UPDATE_THING_SHADOW.code())
                                .kv(LOG_THING_NAME_KEY, thingName)
                                .kv(LOG_SHADOW_NAME_KEY, shadowName)
                                .setCause(error)
                                .log();
                        publishErrorMessage(thingName, shadowName, clientToken,
                                ErrorMessage.INTERNAL_SERVICE_FAILURE_MESSAGE);
                        throw error;
                    }

                    // Publish the message on the delta topic over PubSub if applicable.
                    publishDeltaMessage(thingName, shadowName, clientToken, updatedDocument);

                    // Publish the documents message over the documents topic.
                    publishDocumentsMessage(thingName, shadowName, clientToken, currentDocument, updatedDocument);

                    // Build the response object to send over the accepted topic and as the payload in the response
                    // object. State node is the same shadow document update payload we received in the update request.
                    ObjectNode responseNode = ResponseMessageBuilder.builder()
                            .withVersion(updatedDocument.getVersion())
                            .withClientToken(clientToken)
                            .withTimestamp(Instant.now())
                            .withState(patchStateMetadataPair.getLeft())
                            .withMetadata(patchStateMetadataPair.getRight())
                            .build();
                    byte[] responseNodeBytes = JsonUtil.getPayloadBytes(responseNode);

                    pubSubClientWrapper.accept(PubSubRequest.builder().thingName(thingName).shadowName(shadowName)
                            .payload(responseNodeBytes)
                            .publishOperation(Operation.UPDATE_SHADOW)
                            .build());

                    UpdateThingShadowResponse response = new UpdateThingShadowResponse();
                    response.setPayload(responseNodeBytes);
                    logger.atDebug()
                            .kv(LOG_THING_NAME_KEY, thingName)
                            .kv(LOG_SHADOW_NAME_KEY, shadowName)
                            .log("Successfully updated shadow");
                    this.syncHandler.pushCloudUpdateSyncRequest(thingName, shadowName, updatedDocumentRequestBytes);
                    return response;
                } catch (ShadowManagerDataException | IOException e) {
                    throwServiceError(thingName, shadowName, clientToken, e);
                }
                return null;
            }
        });
    }

    /**
     * Raises a Service error based on the exception.
     *
     * @param thingName   The thing name.
     * @param shadowName  The shadow name.
     * @param clientToken The client token.
     * @param e           The Exception thrown
     * @throws ServiceError always
     */
    @SuppressWarnings("PMD.AvoidUncheckedExceptionsInSignatures")
    private void throwServiceError(String thingName, String shadowName, Optional<String> clientToken, Exception e)
            throws ServiceError {
        logger.atError()
                .setEventType(LogEvents.UPDATE_THING_SHADOW.code())
                .setCause(e)
                .kv(LOG_THING_NAME_KEY, thingName)
                .kv(LOG_SHADOW_NAME_KEY, shadowName)
                .log("Could not process UpdateThingShadow Request due to internal service error");
        publishErrorMessage(thingName, shadowName, clientToken, ErrorMessage.INTERNAL_SERVICE_FAILURE_MESSAGE);
        throw new ServiceError(e.getMessage());
    }

    /**
     * Raises a Invalid Arguments error based for a Invalid Request Parameters Exception.
     *
     * @param thingName   The thing name.
     * @param shadowName  The shadow name.
     * @param clientToken The client token.
     * @param e           The Exception thrown
     * @throws InvalidRequestParametersException always
     */
    @SuppressWarnings("PMD.AvoidUncheckedExceptionsInSignatures")
    private void throwInvalidArgumentsError(String thingName, String shadowName, Optional<String> clientToken,
                                            InvalidRequestParametersException e)
            throws InvalidArgumentsError {
        logger.atWarn()
                .setEventType(LogEvents.DELETE_THING_SHADOW.code())
                .setCause(e)
                .kv(LOG_THING_NAME_KEY, thingName)
                .kv(LOG_SHADOW_NAME_KEY, shadowName)
                .log();
        publishErrorMessage(thingName, shadowName, clientToken, e.getErrorMessage());
        throw new InvalidArgumentsError(e.getMessage());
    }

    /**
     * Build the error response message and publish the error message over PubSub.
     *
     * @param thingName    The thing name.
     * @param shadowName   The shadow name.
     * @param clientToken  The client token if present in the update shadow request.
     * @param errorMessage The error message containing error information.
     */
    //TODO: Maybe move this class into a util class?
    private void publishErrorMessage(String thingName, String shadowName, Optional<String> clientToken,
                                     ErrorMessage errorMessage) {
        JsonNode errorResponse = ResponseMessageBuilder.builder()
                .withTimestamp(Instant.now())
                .withClientToken(clientToken)
                .withError(errorMessage).build();

        try {
            pubSubClientWrapper.reject(PubSubRequest.builder()
                    .thingName(thingName)
                    .shadowName(shadowName)
                    .payload(JsonUtil.getPayloadBytes(errorResponse))
                    .publishOperation(Operation.UPDATE_SHADOW)
                    .build());
        } catch (JsonProcessingException e) {
            logger.atError()
                    .setEventType(Operation.UPDATE_SHADOW.getLogEventType())
                    .kv(LOG_THING_NAME_KEY, thingName)
                    .kv(LOG_SHADOW_NAME_KEY, shadowName)
                    .cause(e)
                    .log("Unable to publish reject message");
        }
    }

    private void publishDeltaMessage(String thingName, String shadowName, Optional<String> clientToken,
                                     ShadowDocument updatedDocument)
            throws IOException {
        Optional<Pair<JsonNode, JsonNode>> deltaMetaDataPair = updatedDocument.getDelta();
        // Only send the delta if there is any difference in the desired and reported states.
        if (deltaMetaDataPair.isPresent()) {
            JsonNode responseMessage = ResponseMessageBuilder.builder()
                    .withVersion(updatedDocument.getVersion())
                    .withTimestamp(Instant.now())
                    .withState(deltaMetaDataPair.get().getLeft())
                    .withMetadata(deltaMetaDataPair.get().getRight())
                    .withClientToken(clientToken)
                    .build();

            pubSubClientWrapper.delta(PubSubRequest.builder().thingName(thingName)
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
                .withPrevious(sourceDocument.isNewDocument() ? null : sourceDocument.toJson(true))
                .withCurrent(updatedDocument.toJson(true))
                .withClientToken(clientToken)
                .withTimestamp(Instant.now())
                .build();
        // Send the current document on the documents topic after successfully updating the shadow document.
        pubSubClientWrapper.documents(PubSubRequest.builder().thingName(thingName).shadowName(shadowName)
                .payload(JsonUtil.getPayloadBytes(responseMessage))
                .publishOperation(Operation.UPDATE_SHADOW)
                .build());

    }
}
