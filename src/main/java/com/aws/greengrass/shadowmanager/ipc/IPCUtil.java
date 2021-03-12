/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.shadowmanager.ipc;

import com.aws.greengrass.authorization.AuthorizationHandler;
import com.aws.greengrass.authorization.Permission;
import com.aws.greengrass.authorization.exceptions.AuthorizationException;
import com.aws.greengrass.shadowmanager.exception.InvalidRequestParametersException;
import com.aws.greengrass.shadowmanager.model.ErrorMessage;
import com.aws.greengrass.util.Utils;
import software.amazon.awssdk.aws.greengrass.model.InvalidArgumentsError;

import java.util.StringJoiner;
import java.util.regex.Pattern;

import static com.aws.greengrass.shadowmanager.model.Constants.CLASSIC_SHADOW_IDENTIFIER;
import static com.aws.greengrass.shadowmanager.model.Constants.SHADOW_MANAGER_NAME;
import static com.aws.greengrass.shadowmanager.model.Constants.SHADOW_RESOURCE_JOINER;

public final class IPCUtil {
    static final int MAX_THING_NAME_LENGTH = 128;
    static final int MAX_SHADOW_NAME_LENGTH = 64;
    static final Pattern SHADOW_PATTERN = Pattern.compile("[a-zA-Z0-9:_-]+");

    public enum LogEvents {
        GET_THING_SHADOW("handle-get-thing-shadow"),
        UPDATE_THING_SHADOW("handle-update-thing-shadow"),
        DELETE_THING_SHADOW("handle-delete-thing-shadow"),
        LIST_NAMED_SHADOWS("handle-list-named-shadows-for-thing");

        String code;

        LogEvents(String code) {
            this.code = code;
        }

        public String code() {
            return code;
        }
    }

    private IPCUtil() {
    }

    /**
     * Validate the thingName and checks whether the service is authorized to run the operation on the shadow.
     * TODO: Remove in oncoming PR to split this up
     *
     * @param authorizationHandler The pubsub agent for new IPC
     * @param opCode               The IPC defined operation code
     * @param serviceName          The service which will be running the operation
     * @param thingName            The thingName of the local shadow
     */
    static void validateThingNameAndDoAuthorization(AuthorizationHandler authorizationHandler, String opCode,
                                                    String serviceName, String thingName)
            throws AuthorizationException, InvalidArgumentsError {
        validateThingNameAndDoAuthorization(authorizationHandler, opCode, serviceName,
                thingName, CLASSIC_SHADOW_IDENTIFIER);
    }

    /**
     * Validate the thingName and checks whether the service is authorized to run the operation on the shadow.
     *
     * @param authorizationHandler The pubsub agent for new IPC
     * @param opCode               The IPC defined operation code
     * @param serviceName          The service which will be running the operation
     * @param thingName            The thingName of the local shadow
     * @param shadowName           the shadowName of the local shadow
     */
    static void validateThingNameAndDoAuthorization(AuthorizationHandler authorizationHandler, String opCode,
                                                    String serviceName, String thingName, String shadowName)
            throws AuthorizationException, InvalidRequestParametersException {

        if (Utils.isEmpty(thingName)) {
            throw new InvalidRequestParametersException(ErrorMessage.createThingNotFoundMessage());
        }

    static void doAuthorization(AuthorizationHandler authorizationHandler, String opCode,
                                String serviceName, String thingName, String shadowName)
            throws AuthorizationException, InvalidArgumentsError {
        StringJoiner shadowResource = new StringJoiner("/");
        shadowResource.add(thingName);
        shadowResource.add(SHADOW_RESOURCE_JOINER);

        if (Utils.isNotEmpty(shadowName)) {
            shadowResource.add(shadowName);
        }

        authorizationHandler.isAuthorized(
                SHADOW_MANAGER_NAME,
                Permission.builder()
                        .principal(serviceName)
                        .operation(opCode)
                        .resource(shadowResource.toString())
                        .build());
    }

    static void validateThingName(String thingName) {
        if (Utils.isEmpty(thingName)) {
            throw new InvalidArgumentsError("ThingName absent in request");
        }

        if (thingName.length() > MAX_THING_NAME_LENGTH) {
            throw new InvalidArgumentsError("ThingName has a maximum length of " + MAX_THING_NAME_LENGTH);
        }

        Matcher matcher = SHADOW_PATTERN.matcher(thingName);
        if (!matcher.matches()) {
            throw new InvalidArgumentsError("ThingName must match pattern " + SHADOW_PATTERN.pattern());
        }
    }

    static void validateShadowName(String shadowName) {
        if (Utils.isEmpty(shadowName)) {
            return;
        }

        if (shadowName.length() > MAX_SHADOW_NAME_LENGTH) {
            throw new InvalidArgumentsError("ShadowName has a maximum length of " + MAX_SHADOW_NAME_LENGTH);
        }

        Matcher matcher = SHADOW_PATTERN.matcher(shadowName);
        if (!matcher.matches()) {
            throw new InvalidArgumentsError("ShadowName must match pattern " + SHADOW_PATTERN.pattern());
        }
    }

    /**
     * Publish the message using PubSub agent when a desired operation for a shadow has been rejected.
     *
     * @param pubSubIPCEventStreamAgent The pubsub agent for new IPC
     * @param shadowName                The name of the shadow for which the publish event is for
     * @param thingName                 The name of the thing for which the publish event is for
     * @param publishOp                 The operation causing the publish
     * @param eventType                 The type of event causing the publish
     * @param errorMessage              The error message object containing reject information
     */
    static void handleRejectedPublish(PubSubIPCEventStreamAgent pubSubIPCEventStreamAgent,
                                      String shadowName, String thingName, String publishOp, String eventType,
                                      ErrorMessage errorMessage) {
        if (Utils.isEmpty(thingName)) {
            logger.atWarn()
                    .setEventType(eventType)
                    .log("Unable to publish to rejected pubsub topic since the thing name {} is empty",
                            shadowName, thingName);
            return;
        }
        byte[] payload;
        try {
            payload = STRICT_MAPPER_JSON.writeValueAsBytes(errorMessage);
        } catch (JsonProcessingException e) {
            logger.atError()
                    .kv(IPCUtil.LOG_THING_NAME_KEY, thingName)
                    .kv(IPCUtil.LOG_SHADOW_NAME_KEY, shadowName)
                    .cause(e)
                    .log("Unable to publish reject message over IPC");
            return;
        }
        pubSubIPCEventStreamAgent.publish(
                String.format(IPCUtil.SHADOW_PUBLISH_TOPIC_REJECTED_FORMAT, thingName, publishOp),
                payload, SERVICE_NAME);
    }

    /**
     * Gets the Shadow name topic prefix.
     *
     * @param shadowName     The name of the shadow
     * @param publishTopicOp The operation causing the publish
     * @return the full topic prefix for the shadow name for the publish topic.
     */
    static AtomicReference<String> getShadowNamePrefix(String shadowName, String publishTopicOp) {
        AtomicReference<String> shadowNamePrefix = new AtomicReference<>(publishTopicOp);
        if (!Utils.isEmpty(shadowName)) {
            shadowNamePrefix.set(String.format(IPCUtil.NAMED_SHADOW_TOPIC_PREFIX, shadowName)
                    + shadowNamePrefix.get());
        }
        return shadowNamePrefix;
    }
}
