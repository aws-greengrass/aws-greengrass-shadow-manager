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
import com.aws.greengrass.shadowmanager.util.JsonUtil;
import com.aws.greengrass.shadowmanager.util.Validator;
import com.aws.greengrass.util.Pair;
import com.fasterxml.jackson.core.JsonProcessingException;
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
import static com.aws.greengrass.shadowmanager.util.JsonUtil.isNullOrMissing;
import static software.amazon.awssdk.aws.greengrass.GreengrassCoreIPCService.UPDATE_THING_SHADOW;

/**
 * Handler class with business logic for all UpdateThingShadow requests over IPC.
 */
public class UpdateThingShadowIPCHandler extends GeneratedAbstractUpdateThingShadowOperationHandler {
    private static final Logger logger = LogManager.getLogger(UpdateThingShadowIPCHandler.class);
    private final String serviceName;

    private final ShadowManagerDAO dao;
    private final AuthorizationHandlerWrapper authorizationHandlerWrapper;
    private final PubSubClientWrapper pubSubClientWrapper;

    /**
     * IPC Handler class for responding to UpdateThingShadow requests.
     *
     * @param context                     topics passed by the Nucleus
     * @param dao                         Local shadow database management
     * @param authorizationHandlerWrapper The authorization handler wrapper
     * @param pubSubClientWrapper         The PubSub client wrapper
     */
    public UpdateThingShadowIPCHandler(
            OperationContinuationHandlerContext context,
            ShadowManagerDAO dao,
            AuthorizationHandlerWrapper authorizationHandlerWrapper,
            PubSubClientWrapper pubSubClientWrapper) {
        super(context);
        this.authorizationHandlerWrapper = authorizationHandlerWrapper;
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
    @SuppressWarnings({"PMD.PreserveStackTrace", "PMD.PrematureDeclaration"})
    public UpdateThingShadowResponse handleRequest(UpdateThingShadowRequest request) {
        return translateExceptions(() -> {
            // TODO: Sync this entire function possibly with delete handler as well.
            String thingName = request.getThingName();
            String shadowName = request.getShadowName();
            byte[] updatedDocumentRequestBytes = request.getPayload();
            ShadowDocument currentDocument = null;
            Optional<String> clientToken = Optional.empty();
            JsonNode updateDocumentRequest = null;
            try {
                logger.atTrace("ipc-update-thing-shadow-request")
                        .kv(LOG_THING_NAME_KEY, thingName)
                        .kv(LOG_SHADOW_NAME_KEY, shadowName)
                        .log();

                ShadowRequest shadowRequest = new ShadowRequest(thingName, shadowName);
                Validator.validateShadowRequest(shadowRequest);
                if (updatedDocumentRequestBytes == null || updatedDocumentRequestBytes.length == 0) {
                    throw new InvalidRequestParametersException(ErrorMessage.createPayloadMissingMessage());
                }
                authorizationHandlerWrapper.doAuthorization(UPDATE_THING_SHADOW, serviceName, shadowRequest);

                // Get the current document from the DAO if present and convert it into a ShadowDocument object.
                byte[] currentDocumentBytes = dao.getShadowThing(thingName, shadowName).orElse(new byte[0]);
                currentDocument = new ShadowDocument(currentDocumentBytes);

                // Validate the payload sent in the update shadow request. Validates the following:
                // 1.The payload schema to ensure that the JSON has the correct schema.
                // 2. The state node schema to ensure it's correctness.
                // 3. The depth of the state node to ensure it is within the boundaries.
                // 4. The version of the payload to ensure that its current version + 1.

                // If the payload size is greater than the maximum default shadow document size, then raise an invalid
                // parameters error for payload too large.
                //TODO: get the max doc size from config.
                if (updatedDocumentRequestBytes.length > Constants.DEFAULT_DOCUMENT_SIZE) {
                    ErrorMessage errorMessage = ErrorMessage.createPayloadTooLargeMessage();
                    throw new InvalidRequestParametersException(errorMessage);
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
                publishErrorMessage(thingName, shadowName, clientToken, ErrorMessage.createVersionConflictMessage());
                throw e;
            } catch (InvalidRequestParametersException e) {
                logger.atWarn()
                        .setEventType(LogEvents.UPDATE_THING_SHADOW.code())
                        .setCause(e)
                        .kv(LOG_THING_NAME_KEY, thingName)
                        .kv(LOG_SHADOW_NAME_KEY, shadowName)
                        .log();
                publishErrorMessage(thingName, shadowName, clientToken, e.getErrorMessage());
                throw new InvalidArgumentsError(e.getMessage());
            } catch (ShadowManagerDataException | IOException e) {
                throwServiceError(thingName, shadowName, clientToken, e);
            }

            try {
                // Generate the new merged document based on the update shadow patch payload.
                ShadowDocument updatedDocument = new ShadowDocument(currentDocument);
                final Pair<JsonNode, JsonNode> patchStateMetadataPair = updatedDocument.update(updateDocumentRequest);

                // Update the new document in the DAO.
                Optional<byte[]> result = dao.updateShadowThing(thingName, shadowName,
                        JsonUtil.getPayloadBytes(updatedDocument.toJson()));
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
                            ErrorMessage.createInternalServiceErrorMessage());
                    throw error;
                }

                // Publish the message on the delta topic over PubSub if applicable.
                publishDeltaMessage(thingName, shadowName, clientToken, updatedDocument);

                // Publish the documents message over the documents topic.
                publishDocumentsMessage(thingName, shadowName, clientToken, currentDocument, updatedDocument);

                // Build the response object to send over the accepted topic and as the payload in the response object.
                // State node is the same shadow document update payload we received in the update request.
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
                return response;
            } catch (ShadowManagerDataException | IOException e) {
                throwServiceError(thingName, shadowName, clientToken, e);
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
    @SuppressWarnings("PMD.AvoidUncheckedExceptionsInSignatures")
    private void throwServiceError(String thingName, String shadowName, Optional<String> clientToken, Exception e)
            throws ServiceError {
        logger.atError()
                .setEventType(LogEvents.UPDATE_THING_SHADOW.code())
                .setCause(e)
                .kv(LOG_THING_NAME_KEY, thingName)
                .kv(LOG_SHADOW_NAME_KEY, shadowName)
                .log("Could not process UpdateThingShadow Request due to internal service error");
        publishErrorMessage(thingName, shadowName, clientToken, ErrorMessage.createInternalServiceErrorMessage());
        throw new ServiceError(e.getMessage());
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
                .withPrevious(sourceDocument.isNewDocument() ? null : sourceDocument.toJson())
                .withCurrent(updatedDocument.toJson())
                .withClientToken(clientToken)
                .withTimestamp(Instant.now())
                .build();
        // Send the current document on the documents topic after successfully updating the shadow document.
        pubSubClientWrapper.documents(PubSubRequest.builder().thingName(thingName).shadowName(shadowName)
                .payload(JsonUtil.getPayloadBytes(responseMessage))
                .publishOperation(Operation.UPDATE_SHADOW)
                .build());

    }

    @Override
    public void handleStreamEvent(EventStreamJsonMessage streamRequestEvent) {
        //NA
    }
}
