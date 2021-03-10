/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.shadowmanager.ipc;

import com.aws.greengrass.authorization.AuthorizationHandler;
import com.aws.greengrass.authorization.exceptions.AuthorizationException;
import com.aws.greengrass.builtin.services.pubsub.PubSubIPCEventStreamAgent;
import com.aws.greengrass.logging.api.Logger;
import com.aws.greengrass.logging.impl.LogManager;
import com.aws.greengrass.shadowmanager.ShadowManagerDAO;
import com.aws.greengrass.shadowmanager.exception.ShadowManagerDataException;
import com.aws.greengrass.shadowmanager.model.ErrorMessage;
import software.amazon.awssdk.aws.greengrass.GeneratedAbstractGetThingShadowOperationHandler;
import software.amazon.awssdk.aws.greengrass.model.GetThingShadowRequest;
import software.amazon.awssdk.aws.greengrass.model.GetThingShadowResponse;
import software.amazon.awssdk.aws.greengrass.model.InvalidArgumentsError;
import software.amazon.awssdk.aws.greengrass.model.ResourceNotFoundError;
import software.amazon.awssdk.aws.greengrass.model.ServiceError;
import software.amazon.awssdk.aws.greengrass.model.UnauthorizedError;
import software.amazon.awssdk.eventstreamrpc.OperationContinuationHandlerContext;
import software.amazon.awssdk.eventstreamrpc.model.EventStreamJsonMessage;

import java.util.concurrent.atomic.AtomicReference;

import static com.aws.greengrass.ipc.common.ExceptionUtil.translateExceptions;
import static com.aws.greengrass.shadowmanager.ShadowManager.SERVICE_NAME;
import static software.amazon.awssdk.aws.greengrass.GreengrassCoreIPCService.GET_THING_SHADOW;

/**
 * Handler class with business logic for all GetThingShadow requests over IPC.
 */
public class GetThingShadowIPCHandler extends GeneratedAbstractGetThingShadowOperationHandler {
    private static final Logger logger = LogManager.getLogger(GetThingShadowIPCHandler.class);
    private static final String PUBLISH_TOPIC_OP = "/get";
    private final String serviceName;

    private final ShadowManagerDAO dao;
    private final AuthorizationHandler authorizationHandler;
    private final PubSubIPCEventStreamAgent pubSubIPCEventStreamAgent;

    /**
     * IPC Handler class for responding to GetThingShadow requests.
     *
     * @param context                   topics passed by the Nucleus
     * @param dao                       Local shadow database management
     * @param authorizationHandler      The authorization handler
     * @param pubSubIPCEventStreamAgent The pubsub agent for new IPC
     */
    public GetThingShadowIPCHandler(OperationContinuationHandlerContext context,
                                    ShadowManagerDAO dao,
                                    AuthorizationHandler authorizationHandler,
                                    PubSubIPCEventStreamAgent pubSubIPCEventStreamAgent) {
        super(context);
        this.authorizationHandler = authorizationHandler;
        this.dao = dao;
        this.pubSubIPCEventStreamAgent = pubSubIPCEventStreamAgent;
        this.serviceName = context.getAuthenticationData().getIdentityLabel();
    }

    @Override
    protected void onStreamClosed() {

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
            AtomicReference<String> shadowNamePrefix = IPCUtil.getShadowNamePrefix(shadowName, PUBLISH_TOPIC_OP);

            try {
                logger.atTrace("ipc-get-thing-shadow-request").log();

                GetThingShadowResponse response = new GetThingShadowResponse();
                IPCUtil.validateThingNameAndDoAuthorization(authorizationHandler, GET_THING_SHADOW,
                        serviceName, thingName, shadowName);

                byte[] result = dao.getShadowThing(thingName, shadowName)
                        .orElseThrow(() -> {
                            ResourceNotFoundError rnf = new ResourceNotFoundError("No shadow found");
                            rnf.setResourceType(IPCUtil.SHADOW_RESOURCE_TYPE);
                            logger.atWarn()
                                    .setEventType(IPCUtil.LogEvents.GET_THING_SHADOW.code())
                                    .setCause(rnf)
                                    .kv(IPCUtil.LOG_THING_NAME_KEY, thingName)
                                    .kv(IPCUtil.LOG_SHADOW_NAME_KEY, shadowName)
                                    .log("Shadow does not exist");
                            IPCUtil.handleRejectedPublish(pubSubIPCEventStreamAgent, shadowName, thingName,
                                    shadowNamePrefix.get(), IPCUtil.LogEvents.GET_THING_SHADOW.code(),
                                    ErrorMessage.createShadowNotFoundMessage(shadowName));
                            return rnf;
                        });

                response.setPayload(result);

                pubSubIPCEventStreamAgent.publish(
                        String.format(IPCUtil.SHADOW_PUBLISH_TOPIC_ACCEPTED_FORMAT, thingName, shadowNamePrefix.get()),
                        result, SERVICE_NAME);
                return response;

            } catch (AuthorizationException e) {
                logger.atWarn()
                        .setEventType(IPCUtil.LogEvents.GET_THING_SHADOW.code())
                        .setCause(e)
                        .kv(IPCUtil.LOG_THING_NAME_KEY, thingName)
                        .kv(IPCUtil.LOG_SHADOW_NAME_KEY, shadowName)
                        .log("Not authorized to update shadow");
                IPCUtil.handleRejectedPublish(pubSubIPCEventStreamAgent, shadowName, thingName, shadowNamePrefix.get(),
                        IPCUtil.LogEvents.GET_THING_SHADOW.code(), ErrorMessage.UNAUTHORIZED_MESSAGE);
                throw new UnauthorizedError(e.getMessage());
            } catch (InvalidArgumentsError e) {
                logger.atWarn()
                        .setEventType(IPCUtil.LogEvents.GET_THING_SHADOW.code())
                        .setCause(e)
                        .kv(IPCUtil.LOG_THING_NAME_KEY, thingName)
                        .kv(IPCUtil.LOG_SHADOW_NAME_KEY, shadowName)
                        .log();
                // TODO: Get the Error Message based on the exception message we get from the validate.
                IPCUtil.handleRejectedPublish(pubSubIPCEventStreamAgent, shadowName, thingName, shadowNamePrefix.get(),
                        IPCUtil.LogEvents.GET_THING_SHADOW.code(), ErrorMessage.INVALID_CLIENT_TOKEN_MESSAGE);
                throw e;
            } catch (ShadowManagerDataException e) {
                logger.atError()
                        .setEventType(IPCUtil.LogEvents.GET_THING_SHADOW.code())
                        .setCause(e)
                        .kv(IPCUtil.LOG_THING_NAME_KEY, thingName)
                        .kv(IPCUtil.LOG_SHADOW_NAME_KEY, shadowName)
                        .log("Could not process UpdateThingShadow Request due to internal service error");
                IPCUtil.handleRejectedPublish(pubSubIPCEventStreamAgent, shadowName, thingName, shadowNamePrefix.get(),
                        IPCUtil.LogEvents.GET_THING_SHADOW.code(), ErrorMessage.createInternalServiceErrorMessage());
                throw new ServiceError(e.getMessage());
            }
        });
    }

    @Override
    public void handleStreamEvent(EventStreamJsonMessage streamRequestEvent) {

    }
}
