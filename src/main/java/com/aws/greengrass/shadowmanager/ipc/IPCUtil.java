/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.shadowmanager.ipc;

import com.aws.greengrass.authorization.AuthorizationHandler;
import com.aws.greengrass.authorization.Permission;
import com.aws.greengrass.authorization.exceptions.AuthorizationException;
import com.aws.greengrass.builtin.services.pubsub.PubSubIPCEventStreamAgent;
import com.aws.greengrass.logging.api.Logger;
import com.aws.greengrass.logging.impl.LogManager;
import com.aws.greengrass.shadowmanager.model.ErrorMessage;
import com.aws.greengrass.util.Utils;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import software.amazon.awssdk.aws.greengrass.model.InvalidArgumentsError;

import java.util.StringJoiner;
import java.util.concurrent.atomic.AtomicReference;

import static com.aws.greengrass.shadowmanager.ShadowManager.SERVICE_NAME;

public final class IPCUtil {

    private static final Logger logger = LogManager.getLogger(IPCUtil.class);
    private static final ObjectMapper STRICT_MAPPER_JSON = new ObjectMapper(new JsonFactory());
    static final String SHADOW_RESOURCE_TYPE = "shadow";
    static final String SHADOW_RESOURCE_JOINER = "shadow";
    static final String SHADOW_MANAGER_NAME = "aws.greengrass.ShadowManager";
    static final String SHADOW_PUBLISH_TOPIC_ACCEPTED_FORMAT = "$aws/things/%s/shadow%s/accepted";
    static final String SHADOW_PUBLISH_TOPIC_REJECTED_FORMAT = "$aws/things/%s/shadow%s/rejected";
    static final String SHADOW_PUBLISH_TOPIC_DELTA_FORMAT = "$aws/things/%s/shadow%s/delta";
    static final String NAMED_SHADOW_TOPIC_PREFIX = "/name/%s";
    static final String LOG_THING_NAME_KEY = "thing name";
    static final String LOG_SHADOW_NAME_KEY = "shadow name";
    static final String LOG_NEXT_TOKEN_KEY = "nextToken";
    static final String CLASSIC_SHADOW_IDENTIFIER = "";

    enum LogEvents {
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
        STRICT_MAPPER_JSON.findAndRegisterModules();
        STRICT_MAPPER_JSON.setDefaultPropertyInclusion(JsonInclude.Include.NON_NULL);
    }

    /**
     * Validate the thingName and checks whether the service is authorized to run the operation on the shadow.
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
            throws AuthorizationException, InvalidArgumentsError {

        if (Utils.isEmpty(thingName)) {
            throw new InvalidArgumentsError("ThingName absent in request");
        }

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
