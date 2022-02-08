/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.shadowmanager.ipc;

import com.aws.greengrass.logging.api.Logger;
import com.aws.greengrass.logging.impl.LogManager;
import com.aws.greengrass.shadowmanager.exception.InvalidRequestParametersException;
import com.aws.greengrass.shadowmanager.ipc.model.Operation;
import com.aws.greengrass.shadowmanager.ipc.model.PubSubRequest;
import com.aws.greengrass.shadowmanager.model.ErrorMessage;
import com.aws.greengrass.shadowmanager.model.ResponseMessageBuilder;
import com.aws.greengrass.shadowmanager.util.JsonUtil;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import lombok.Getter;
import software.amazon.awssdk.aws.greengrass.model.InvalidArgumentsError;

import java.time.Instant;
import java.util.Optional;

import static com.aws.greengrass.shadowmanager.model.Constants.LOG_SHADOW_NAME_KEY;
import static com.aws.greengrass.shadowmanager.model.Constants.LOG_THING_NAME_KEY;

public class BaseRequestHandler {
    private static final Logger logger = LogManager.getLogger(BaseRequestHandler.class);
    @Getter
    private final PubSubClientWrapper pubSubClientWrapper;

    BaseRequestHandler(PubSubClientWrapper pubSubClientWrapper) {
        this.pubSubClientWrapper = pubSubClientWrapper;
    }

    /**
     * Build the error response message and publish the error message over PubSub.
     *
     * @param thingName    The thing name.
     * @param shadowName   The shadow name.
     * @param clientToken  The client token if present in the update shadow request.
     * @param errorMessage The error message containing error information.
     * @param op           The publish operation.
     */
    void publishErrorMessage(String thingName, String shadowName, Optional<String> clientToken,
                             ErrorMessage errorMessage, Operation op) {
        JsonNode errorResponse = ResponseMessageBuilder.builder()
                .withTimestamp(Instant.now())
                .withClientToken(clientToken)
                .withError(errorMessage).build();

        try {
            pubSubClientWrapper.reject(PubSubRequest.builder()
                    .thingName(thingName)
                    .shadowName(shadowName)
                    .payload(JsonUtil.getPayloadBytes(errorResponse))
                    .publishOperation(op)
                    .build());
        } catch (JsonProcessingException e) {
            logger.atError()
                    .setEventType(op.getLogEventType())
                    .kv(LOG_THING_NAME_KEY, thingName)
                    .kv(LOG_SHADOW_NAME_KEY, shadowName)
                    .cause(e)
                    .log("Unable to publish reject message");
        }
    }

    /**
     * Raises a Invalid Arguments error based for a Invalid Request Parameters Exception.
     *
     * @param thingName   The thing name.
     * @param shadowName  The shadow name.
     * @param clientToken The client token.
     * @param e           The Exception thrown
     * @throws InvalidArgumentsError always
     */
    @SuppressWarnings("PMD.AvoidUncheckedExceptionsInSignatures")
    void throwInvalidArgumentsError(String thingName, String shadowName, Optional<String> clientToken,
                                    InvalidRequestParametersException e, Operation op)
            throws InvalidArgumentsError {
        logger.atWarn()
                .setEventType(op.getLogEventType())
                .setCause(e)
                .kv(LOG_THING_NAME_KEY, thingName)
                .kv(LOG_SHADOW_NAME_KEY, shadowName)
                .log();
        publishErrorMessage(thingName, shadowName, clientToken, e.getErrorMessage(), op);
        throw new InvalidArgumentsError(e.getMessage());
    }
}
