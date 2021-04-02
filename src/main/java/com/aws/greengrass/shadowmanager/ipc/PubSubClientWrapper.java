/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.shadowmanager.ipc;

import com.aws.greengrass.builtin.services.pubsub.PubSubIPCEventStreamAgent;
import com.aws.greengrass.logging.api.Logger;
import com.aws.greengrass.logging.impl.LogManager;
import com.aws.greengrass.shadowmanager.ShadowUtil;
import com.aws.greengrass.shadowmanager.ipc.model.AcceptRequest;
import com.aws.greengrass.shadowmanager.ipc.model.IPCRequest;
import com.aws.greengrass.shadowmanager.ipc.model.RejectRequest;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import software.amazon.awssdk.aws.greengrass.model.InvalidArgumentsError;

import javax.inject.Inject;

import static com.aws.greengrass.shadowmanager.ShadowManager.SERVICE_NAME;
import static com.aws.greengrass.shadowmanager.model.Constants.LOG_SHADOW_NAME_KEY;
import static com.aws.greengrass.shadowmanager.model.Constants.LOG_THING_NAME_KEY;
import static com.aws.greengrass.shadowmanager.model.Constants.SHADOW_PUBLISH_ACCEPTED_TOPIC;
import static com.aws.greengrass.shadowmanager.model.Constants.SHADOW_PUBLISH_DELTA_TOPIC;
import static com.aws.greengrass.shadowmanager.model.Constants.SHADOW_PUBLISH_DOCUMENTS_TOPIC;
import static com.aws.greengrass.shadowmanager.model.Constants.SHADOW_PUBLISH_REJECTED_TOPIC;

/**
 * Class to handle PubSub interaction with the PubSub Event Stream Agent.
 */
public class PubSubClientWrapper {
    private static final Logger logger = LogManager.getLogger(PubSubClientWrapper.class);
    private static final ObjectMapper STRICT_MAPPER_JSON = new ObjectMapper(new JsonFactory());
    private final PubSubIPCEventStreamAgent pubSubIPCEventStreamAgent;

    /**
     * Constructor.
     *
     * @param pubSubIPCEventStreamAgent PubSub event stream agent
     */
    @Inject
    public PubSubClientWrapper(PubSubIPCEventStreamAgent pubSubIPCEventStreamAgent) {
        STRICT_MAPPER_JSON.findAndRegisterModules();
        STRICT_MAPPER_JSON.setDefaultPropertyInclusion(JsonInclude.Include.NON_NULL);
        this.pubSubIPCEventStreamAgent = pubSubIPCEventStreamAgent;
    }

    /**
     * Publish the message using PubSub agent when a desired operation for a shadow has been rejected.
     *
     * @param rejectRequest The request object containing the reject information.
     */
    public void reject(RejectRequest rejectRequest) {
        byte[] payload;
        try {
            payload = STRICT_MAPPER_JSON.writeValueAsBytes(rejectRequest.getErrorMessage());
        } catch (JsonProcessingException e) {
            logger.atError()
                    .setEventType(rejectRequest.getPublishOperation().getLogEventType())
                    .kv(LOG_THING_NAME_KEY, rejectRequest.getThingName())
                    .kv(LOG_SHADOW_NAME_KEY, rejectRequest.getShadowName())
                    .cause(e)
                    .log("Unable to publish reject message");
            return;
        }
        try {
            this.pubSubIPCEventStreamAgent.publish(getShadowTopic(rejectRequest, SHADOW_PUBLISH_REJECTED_TOPIC),
                    payload, SERVICE_NAME);
            logger.atTrace()
                    .setEventType(rejectRequest.getPublishOperation().getLogEventType())
                    .kv(LOG_THING_NAME_KEY, rejectRequest.getThingName())
                    .kv(LOG_SHADOW_NAME_KEY, rejectRequest.getShadowName())
                    .log("Successfully published reject message");
        } catch (InvalidArgumentsError e) {
            logger.atError().cause(e)
                    .kv(LOG_THING_NAME_KEY, rejectRequest.getThingName())
                    .kv(LOG_SHADOW_NAME_KEY, rejectRequest.getShadowName())
                    .setEventType(rejectRequest.getPublishOperation().getLogEventType())
                    .log("Unable to publish rejected message");
        }
    }

    /**
     * Publish the message using PubSub agent when a desired operation for a shadow has been accepted.
     *
     * @param acceptRequest The request object containing the accepted information.
     */
    public void accept(AcceptRequest acceptRequest) {
        handleAcceptedMessage(acceptRequest, SHADOW_PUBLISH_ACCEPTED_TOPIC);
    }


    /**
     * Publish the message using PubSub agent when a desired operation for a shadow has been accepted and the delta
     * information needs to be published.
     *
     * @param acceptRequest The request object containing the delta information.
     */
    public void delta(AcceptRequest acceptRequest) {
        handleAcceptedMessage(acceptRequest, SHADOW_PUBLISH_DELTA_TOPIC);
    }

    /**
     * Publish the message using PubSub agent when a desired operation for a shadow has been accepted and the documents
     * information needs to be published.
     *
     * @param acceptRequest The request object containing the documents information.
     */
    public void documents(AcceptRequest acceptRequest) {
        handleAcceptedMessage(acceptRequest, SHADOW_PUBLISH_DOCUMENTS_TOPIC);
    }

    /**
     * Publish the message using PubSub agent when a desired operation for a shadow has been accepted.
     *
     * @param acceptRequest     The request object containing the accepted information.
     * @param shadowTopicFormat The format for the shadow topic on which to publish the message
     */
    private void handleAcceptedMessage(AcceptRequest acceptRequest, String shadowTopicFormat) {
        try {
            this.pubSubIPCEventStreamAgent.publish(getShadowTopic(acceptRequest, shadowTopicFormat),
                    acceptRequest.getPayload(), SERVICE_NAME);
            logger.atTrace()
                    .setEventType(acceptRequest.getPublishOperation().getLogEventType())
                    .kv(LOG_THING_NAME_KEY, acceptRequest.getThingName())
                    .kv(LOG_SHADOW_NAME_KEY, acceptRequest.getShadowName())
                    .log("Successfully published accept message");
        } catch (InvalidArgumentsError e) {
            logger.atError().cause(e)
                    .kv(LOG_THING_NAME_KEY, acceptRequest.getThingName())
                    .kv(LOG_SHADOW_NAME_KEY, acceptRequest.getShadowName())
                    .setEventType(acceptRequest.getPublishOperation().getLogEventType())
                    .log("Unable to publish accepted message");
        }
    }

    /**
     * Gets the Shadow name topic prefix.
     *
     * @param ipcRequest Object that includes thingName, shadowName, and operation to form the shadow topic prefix
     * @return the full topic prefix for the shadow name for the publish topic.
     */
    private String getShadowTopic(IPCRequest ipcRequest, String topic) {
        String thingName = ipcRequest.getThingName();
        String shadowName = ipcRequest.getShadowName();
        String publishTopicOp = ipcRequest.getPublishOperation().getOp();
        return ShadowUtil.getShadowTopicPrefix(thingName, shadowName) + publishTopicOp + topic;
    }
}
