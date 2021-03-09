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
import software.amazon.awssdk.aws.greengrass.GeneratedAbstractUpdateThingShadowOperationHandler;
import software.amazon.awssdk.aws.greengrass.model.ConflictError;
import software.amazon.awssdk.aws.greengrass.model.InvalidArgumentsError;
import software.amazon.awssdk.aws.greengrass.model.ServiceError;
import software.amazon.awssdk.aws.greengrass.model.UnauthorizedError;
import software.amazon.awssdk.aws.greengrass.model.UpdateThingShadowRequest;
import software.amazon.awssdk.aws.greengrass.model.UpdateThingShadowResponse;
import software.amazon.awssdk.eventstreamrpc.OperationContinuationHandlerContext;
import software.amazon.awssdk.eventstreamrpc.model.EventStreamJsonMessage;

import java.util.Optional;

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
            // TODO: Sync this entire function possibly with delete handler as well.
            // TODO: Define Shadow Document models and validate payload.
            // TODO: Calculate delta and publish update.
            String thingName = request.getThingName();
            String shadowName = request.getShadowName();
            byte[] payload = request.getPayload();

            try {
                logger.atTrace("ipc-update-thing-shadow-request")
                        .kv(IPCUtil.LOG_THING_NAME_KEY, thingName)
                        .kv(IPCUtil.LOG_SHADOW_NAME_KEY, shadowName)
                        .log();

                IPCUtil.validateThingNameAndDoAuthorization(authorizationHandler, UPDATE_THING_SHADOW,
                        serviceName, thingName, shadowName);
                if (payload == null || payload.length == 0) {
                    throw new InvalidArgumentsError("Missing update payload");
                }
                byte[] source = dao.getShadowThing(thingName, shadowName)
                        .orElse(new byte[0]);

                validatePayloadVersion(thingName, shadowName, payload, source);

                // Payload on the accept topic is the same shadow document update we received in the update request.
                // TODO: Get the correct update document from the request.
                pubSubClientWrapper.accept(AcceptRequest.builder().thingName(thingName).shadowName(shadowName)
                        .payload(payload)
                        .publishOperation(Operation.UPDATE_SHADOW)
                        .build());

                //TODO: Calculate delta based on the desired and reported states.
                Optional<byte[]> delta = calculateDelta(source, payload);
                // Only send the delta if there is any difference in the desired and reported states.
                delta.ifPresent(d -> pubSubClientWrapper.delta(AcceptRequest.builder().thingName(thingName)
                        .shadowName(shadowName)
                        .payload(d)
                        .publishOperation(Operation.UPDATE_SHADOW)
                        .build()));

                byte[] newDocument = calculateNewDocument(source, payload);
                // Send the current document on the documents topic after successfully updating the shadow document.
                // TODO: Documents payload consists of the reported state, desired state (if any) and the delta state
                //    any.
                pubSubClientWrapper.documents(AcceptRequest.builder().thingName(thingName).shadowName(shadowName)
                        .payload(newDocument)
                        .publishOperation(Operation.UPDATE_SHADOW)
                        .build());

                byte[] result = dao.updateShadowThing(thingName, shadowName, newDocument)
                        .orElseThrow(() -> {
                            ServiceError error = new ServiceError("Unexpected error occurred in trying to "
                                    + "update shadow thing.");
                            logger.atError()
                                    .setEventType(IPCUtil.LogEvents.UPDATE_THING_SHADOW.code())
                                    .kv(IPCUtil.LOG_THING_NAME_KEY, thingName)
                                    .kv(IPCUtil.LOG_SHADOW_NAME_KEY, shadowName)
                                    .setCause(error)
                                    .log();
                            pubSubClientWrapper.reject(RejectRequest.builder().thingName(thingName)
                                    .shadowName(shadowName)
                                    .errorMessage(ErrorMessage.createInternalServiceErrorMessage())
                                    .publishOperation(Operation.UPDATE_SHADOW)
                                    .build());
                            return error;
                        });

                UpdateThingShadowResponse response = new UpdateThingShadowResponse();
                response.setPayload(result);
                logger.atDebug()
                        .kv(IPCUtil.LOG_THING_NAME_KEY, thingName)
                        .kv(IPCUtil.LOG_SHADOW_NAME_KEY, shadowName)
                        .log("Successfully updated shadow");
                return response;

            } catch (AuthorizationException e) {
                logger.atWarn()
                        .setEventType(IPCUtil.LogEvents.UPDATE_THING_SHADOW.code())
                        .setCause(e)
                        .kv(IPCUtil.LOG_THING_NAME_KEY, thingName)
                        .kv(IPCUtil.LOG_SHADOW_NAME_KEY, shadowName)
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
                        .kv(IPCUtil.LOG_THING_NAME_KEY, thingName)
                        .kv(IPCUtil.LOG_SHADOW_NAME_KEY, shadowName)
                        .log("Conflicting version in shadow update message");
                pubSubClientWrapper.reject(RejectRequest.builder().thingName(thingName).shadowName(shadowName)
                        .errorMessage(ErrorMessage.createVersionConflictMessage())
                        .publishOperation(Operation.UPDATE_SHADOW)
                        .build());
                throw e;
            } catch (InvalidArgumentsError e) {
                logger.atWarn()
                        .setEventType(IPCUtil.LogEvents.UPDATE_THING_SHADOW.code())
                        .setCause(e)
                        .kv(IPCUtil.LOG_THING_NAME_KEY, thingName)
                        .kv(IPCUtil.LOG_SHADOW_NAME_KEY, shadowName)
                        .log();
                // TODO: Get the Error Message based on the exception message we get from the validate.
                pubSubClientWrapper.reject(RejectRequest.builder().thingName(thingName).shadowName(shadowName)
                        .errorMessage(ErrorMessage.INVALID_CLIENT_TOKEN_MESSAGE)
                        .publishOperation(Operation.UPDATE_SHADOW)
                        .build());
                throw e;
            } catch (ShadowManagerDataException e) {
                logger.atError()
                        .setEventType(IPCUtil.LogEvents.UPDATE_THING_SHADOW.code())
                        .setCause(e)
                        .kv(IPCUtil.LOG_THING_NAME_KEY, thingName)
                        .kv(IPCUtil.LOG_SHADOW_NAME_KEY, shadowName)
                        .log("Could not process UpdateThingShadow Request due to internal service error");
                pubSubClientWrapper.reject(RejectRequest.builder().thingName(thingName).shadowName(shadowName)
                        .errorMessage(ErrorMessage.createInternalServiceErrorMessage())
                        .publishOperation(Operation.UPDATE_SHADOW)
                        .build());
                throw new ServiceError(e.getMessage());
            }
        });
    }

    // TODO: Implement version conflict validation
    private void validatePayloadVersion(String thingName, String shadowName, byte[] payload, byte[] sourceDocument)
            throws ConflictError {
        if (sourceDocument.length == 0) {
            logger.atTrace()
                    .kv(IPCUtil.LOG_THING_NAME_KEY, thingName)
                    .kv(IPCUtil.LOG_SHADOW_NAME_KEY, shadowName)
                    .log("No need to check check version for new shadow");
            return;
        }
    }

    private Optional<byte[]> calculateDelta(byte[] source, byte[] payload) {
        return Optional.empty();
    }

    private byte[] calculateNewDocument(byte[] source, byte[] payload) {
        return new byte[0];
    }

    @Override
    public void handleStreamEvent(EventStreamJsonMessage streamRequestEvent) {

    }
}
