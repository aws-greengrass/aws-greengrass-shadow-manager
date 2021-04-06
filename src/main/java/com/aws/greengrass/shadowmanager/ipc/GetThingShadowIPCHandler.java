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
import com.fasterxml.jackson.databind.node.ObjectNode;
import software.amazon.awssdk.aws.greengrass.GeneratedAbstractGetThingShadowOperationHandler;
import software.amazon.awssdk.aws.greengrass.model.GetThingShadowRequest;
import software.amazon.awssdk.aws.greengrass.model.GetThingShadowResponse;
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
import static software.amazon.awssdk.aws.greengrass.GreengrassCoreIPCService.GET_THING_SHADOW;

/**
 * Handler class with business logic for all GetThingShadow requests over IPC.
 */
public class GetThingShadowIPCHandler extends GeneratedAbstractGetThingShadowOperationHandler {
    private static final Logger logger = LogManager.getLogger(GetThingShadowIPCHandler.class);
    private final String serviceName;

    private final ShadowManagerDAO dao;
    private final AuthorizationHandlerWrapper authorizationHandlerWrapper;
    private final PubSubClientWrapper pubSubClientWrapper;

    /**
     * IPC Handler class for responding to GetThingShadow requests.
     *
     * @param context                     topics passed by the Nucleus
     * @param dao                         Local shadow database management
     * @param authorizationHandlerWrapper The authorization handler wrapper
     * @param pubSubClientWrapper         The PubSub client wrapper
     */
    public GetThingShadowIPCHandler(OperationContinuationHandlerContext context,
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
     * Handles GetThingShadow Requests from IPC.
     *
     * @param request GetThingShadow request from IPC API
     * @return GetThingShadow response
     * @throws ResourceNotFoundError if requested document is not found locally
     * @throws UnauthorizedError     if GetThingShadow call not authorized
     * @throws InvalidArgumentsError if validation error occurred with supplied request fields
     * @throws ServiceError          if database error occurs
     */
    @Override
    public GetThingShadowResponse handleRequest(GetThingShadowRequest request) {
        return translateExceptions(() -> {
            String thingName = request.getThingName();
            String shadowName = request.getShadowName();
            //TODO: Add payload to GetThingShadowRequest
            byte[] payload = null;

            try {
                logger.atTrace("ipc-get-thing-shadow-request")
                        .kv(LOG_THING_NAME_KEY, thingName)
                        .kv(LOG_SHADOW_NAME_KEY, shadowName)
                        .log();

                ShadowRequest shadowRequest = new ShadowRequest(thingName, shadowName);
                Validator.validateShadowRequest(shadowRequest);
                authorizationHandlerWrapper.doAuthorization(GET_THING_SHADOW, serviceName, shadowRequest);

                byte[] currentDocumentBytes = dao.getShadowThing(thingName, shadowName)
                        .orElseThrow(() -> {
                            ResourceNotFoundError rnf = new ResourceNotFoundError("No shadow found");
                            rnf.setResourceType(SHADOW_RESOURCE_TYPE);
                            logger.atWarn()
                                    .setEventType(LogEvents.GET_THING_SHADOW.code())
                                    .setCause(rnf)
                                    .kv(LOG_THING_NAME_KEY, thingName)
                                    .kv(LOG_SHADOW_NAME_KEY, shadowName)
                                    .log("Shadow does not exist");
                            pubSubClientWrapper.reject(RejectRequest.builder().thingName(thingName)
                                    .shadowName(shadowName)
                                    .errorMessage(ErrorMessage.createShadowNotFoundMessage(shadowName))
                                    .publishOperation(Operation.GET_SHADOW)
                                    .build());
                            return rnf;
                        });
                ShadowDocument currentShadowDocument = new ShadowDocument(currentDocumentBytes);

                // Get the Client Token if present in the payload.
                Optional<JsonNode> payloadJson = JsonUtil.getPayloadJson(payload);
                Optional<String> clientToken = payloadJson.flatMap(JsonUtil::getClientToken);

                ObjectNode responseNode = ResponseMessageBuilder.builder()
                        .withState(currentShadowDocument.getState().toJsonWithDelta())
                        //TODO: Update the metadata when implemented.
                        //.withMetadata()
                        .withVersion(currentShadowDocument.getVersion())
                        .withClientToken(clientToken)
                        .withTimestamp(Instant.now()).build();

                byte[] responseNodeBytes = JsonUtil.getPayloadBytes(responseNode);

                pubSubClientWrapper.accept(AcceptRequest.builder().thingName(thingName).shadowName(shadowName)
                        .payload(responseNodeBytes)
                        .publishOperation(Operation.GET_SHADOW)
                        .build());

                GetThingShadowResponse response = new GetThingShadowResponse();
                response.setPayload(responseNodeBytes);
                return response;

            } catch (AuthorizationException e) {
                logger.atWarn()
                        .setEventType(LogEvents.GET_THING_SHADOW.code())
                        .setCause(e)
                        .kv(LOG_THING_NAME_KEY, thingName)
                        .kv(LOG_SHADOW_NAME_KEY, shadowName)
                        .log("Not authorized to update shadow");
                pubSubClientWrapper.reject(RejectRequest.builder().thingName(thingName).shadowName(shadowName)
                        .errorMessage(ErrorMessage.UNAUTHORIZED_MESSAGE)
                        .publishOperation(Operation.GET_SHADOW)
                        .build());
                throw new UnauthorizedError(e.getMessage());
            } catch (InvalidRequestParametersException e) {
                logger.atWarn()
                        .setEventType(LogEvents.GET_THING_SHADOW.code())
                        .setCause(e)
                        .kv(LOG_THING_NAME_KEY, thingName)
                        .kv(LOG_SHADOW_NAME_KEY, shadowName)
                        .log();
                pubSubClientWrapper.reject(RejectRequest.builder().thingName(thingName).shadowName(shadowName)
                        .errorMessage(e.getErrorMessage())
                        .publishOperation(Operation.GET_SHADOW)
                        .build());
                throw new InvalidArgumentsError(e.getMessage());
            } catch (ShadowManagerDataException | IOException e) {
                logger.atError()
                        .setEventType(LogEvents.GET_THING_SHADOW.code())
                        .setCause(e)
                        .kv(LOG_THING_NAME_KEY, thingName)
                        .kv(LOG_SHADOW_NAME_KEY, shadowName)
                        .log("Could not process UpdateThingShadow Request due to internal service error");
                pubSubClientWrapper.reject(RejectRequest.builder().thingName(thingName).shadowName(shadowName)
                        .errorMessage(ErrorMessage.createInternalServiceErrorMessage())
                        .publishOperation(Operation.GET_SHADOW)
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
