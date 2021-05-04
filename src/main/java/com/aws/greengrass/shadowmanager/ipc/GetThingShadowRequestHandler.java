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
import com.aws.greengrass.shadowmanager.exception.ThrottledRequestException;
import com.aws.greengrass.shadowmanager.ipc.model.Operation;
import com.aws.greengrass.shadowmanager.ipc.model.PubSubRequest;
import com.aws.greengrass.shadowmanager.model.ErrorMessage;
import com.aws.greengrass.shadowmanager.model.LogEvents;
import com.aws.greengrass.shadowmanager.model.ResponseMessageBuilder;
import com.aws.greengrass.shadowmanager.model.ShadowDocument;
import com.aws.greengrass.shadowmanager.model.ShadowRequest;
import com.aws.greengrass.shadowmanager.util.JsonUtil;
import com.aws.greengrass.shadowmanager.util.Validator;
import com.fasterxml.jackson.databind.node.ObjectNode;
import software.amazon.awssdk.aws.greengrass.model.GetThingShadowRequest;
import software.amazon.awssdk.aws.greengrass.model.GetThingShadowResponse;
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
import static software.amazon.awssdk.aws.greengrass.GreengrassCoreIPCService.GET_THING_SHADOW;

public class GetThingShadowRequestHandler extends BaseRequestHandler {
    private static final Logger logger = LogManager.getLogger(GetThingShadowRequestHandler.class);
    private final ShadowManagerDAO dao;
    private final AuthorizationHandlerWrapper authorizationHandlerWrapper;

    /**
     * IPC Handler class for responding to GetThingShadow requests.
     *
     * @param dao                         Local shadow database management
     * @param authorizationHandlerWrapper The authorization handler wrapper
     * @param pubSubClientWrapper         The PubSub client wrapper
     * @param inboundRateLimiter          The inbound rate limiter class for throttling local requests
     */
    public GetThingShadowRequestHandler(ShadowManagerDAO dao,
                                        AuthorizationHandlerWrapper authorizationHandlerWrapper,
                                        PubSubClientWrapper pubSubClientWrapper,
                                        InboundRateLimiter inboundRateLimiter) {
        super(pubSubClientWrapper, inboundRateLimiter);
        this.authorizationHandlerWrapper = authorizationHandlerWrapper;
        this.dao = dao;
    }

    /**
     * Handles GetThingShadow Requests.
     *
     * @param request     GetThingShadow request from IPC API
     * @param serviceName the service name making the request.
     * @return GetThingShadow response
     * @throws ResourceNotFoundError if requested document is not found locally
     * @throws UnauthorizedError     if GetThingShadow call not authorized
     * @throws InvalidArgumentsError if validation error occurred with supplied request fields
     * @throws ServiceError          if database error occurs
     */
    @SuppressWarnings("PMD.PreserveStackTrace")
    public GetThingShadowResponse handleRequest(GetThingShadowRequest request, String serviceName) {
        return translateExceptions(() -> {
            String thingName = request.getThingName();
            String shadowName = request.getShadowName();

            try {
                logger.atTrace("ipc-get-thing-shadow-request")
                        .kv(LOG_THING_NAME_KEY, thingName)
                        .kv(LOG_SHADOW_NAME_KEY, shadowName)
                        .log();

                ShadowRequest shadowRequest = new ShadowRequest(thingName, shadowName);
                try {
                    inboundRateLimiter.acquireLockForThing(thingName);
                    Validator.validateShadowRequest(shadowRequest);
                } catch (InvalidRequestParametersException e) {
                    throwInvalidArgumentsError(thingName, shadowName, Optional.empty(), e, Operation.DELETE_SHADOW);
                } catch (ThrottledRequestException e) {
                    logger.atError()
                            .setEventType(LogEvents.DELETE_THING_SHADOW.code())
                            .setCause(e)
                            .kv(LOG_THING_NAME_KEY, thingName)
                            .kv(LOG_SHADOW_NAME_KEY, shadowName)
                            .log("Local GetThingShadow request throttled");
                    throw new ServiceError(e.getMessage());
                }

                authorizationHandlerWrapper.doAuthorization(GET_THING_SHADOW, serviceName, shadowRequest);

                Optional<ShadowDocument> currentShadowDocument = dao.getShadowThing(thingName, shadowName);
                if (!currentShadowDocument.isPresent()) {
                    ResourceNotFoundError rnf = new ResourceNotFoundError("No shadow found");
                    rnf.setResourceType(SHADOW_RESOURCE_TYPE);
                    logger.atWarn()
                            .setEventType(LogEvents.GET_THING_SHADOW.code())
                            .setCause(rnf)
                            .kv(LOG_THING_NAME_KEY, thingName)
                            .kv(LOG_SHADOW_NAME_KEY, shadowName)
                            .log("Shadow does not exist");
                    publishErrorMessage(thingName, shadowName, Optional.empty(),
                            ErrorMessage.createShadowNotFoundMessage(shadowName), Operation.GET_SHADOW);
                    throw rnf;
                }

                ObjectNode responseNode = ResponseMessageBuilder.builder()
                        .withState(currentShadowDocument.get().getState().toJsonWithDelta())
                        .withMetadata(currentShadowDocument.get().getMetadata().toJson())
                        .withVersion(currentShadowDocument.get().getVersion())
                        .withTimestamp(Instant.now()).build();

                byte[] responseNodeBytes = JsonUtil.getPayloadBytes(responseNode);

                pubSubClientWrapper.accept(PubSubRequest.builder().thingName(thingName).shadowName(shadowName)
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
                publishErrorMessage(thingName, shadowName, Optional.empty(), ErrorMessage.UNAUTHORIZED_MESSAGE,
                        Operation.GET_SHADOW);
                throw new UnauthorizedError(e.getMessage());
            } catch (InvalidRequestParametersException e) {
                logger.atWarn()
                        .setEventType(LogEvents.GET_THING_SHADOW.code())
                        .setCause(e)
                        .kv(LOG_THING_NAME_KEY, thingName)
                        .kv(LOG_SHADOW_NAME_KEY, shadowName)
                        .log();
                publishErrorMessage(thingName, shadowName, Optional.empty(), e.getErrorMessage(), Operation.GET_SHADOW);
                throw new InvalidArgumentsError(e.getMessage());
            } catch (ShadowManagerDataException | IOException e) {
                logger.atError()
                        .setEventType(LogEvents.GET_THING_SHADOW.code())
                        .setCause(e)
                        .kv(LOG_THING_NAME_KEY, thingName)
                        .kv(LOG_SHADOW_NAME_KEY, shadowName)
                        .log("Could not process GetThingShadow Request due to internal service error");
                publishErrorMessage(thingName, shadowName, Optional.empty(),
                        ErrorMessage.INTERNAL_SERVICE_FAILURE_MESSAGE, Operation.GET_SHADOW);
                throw new ServiceError(e.getMessage());
            }
        });
    }
}
