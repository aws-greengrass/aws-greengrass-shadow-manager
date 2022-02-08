/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.shadowmanager.ipc;

import com.aws.greengrass.builtin.services.pubsub.PubSubIPCEventStreamAgent;
import com.aws.greengrass.builtin.services.pubsub.PublishEvent;
import com.aws.greengrass.logging.api.Logger;
import com.aws.greengrass.logging.impl.LogManager;
import com.aws.greengrass.shadowmanager.ipc.model.PubSubRequest;
import software.amazon.awssdk.aws.greengrass.model.InvalidArgumentsError;

import java.util.function.Consumer;
import javax.inject.Inject;

import static com.aws.greengrass.shadowmanager.ShadowManager.SERVICE_NAME;
import static com.aws.greengrass.shadowmanager.model.Constants.LOG_SHADOW_NAME_KEY;
import static com.aws.greengrass.shadowmanager.model.Constants.LOG_THING_NAME_KEY;
import static com.aws.greengrass.shadowmanager.model.Constants.PUBSUB_SUBSCRIBE_TOPIC;
import static com.aws.greengrass.shadowmanager.model.Constants.SHADOW_MANAGER_NAME;
import static com.aws.greengrass.shadowmanager.model.Constants.SHADOW_PUBLISH_ACCEPTED_TOPIC;
import static com.aws.greengrass.shadowmanager.model.Constants.SHADOW_PUBLISH_DELTA_TOPIC;
import static com.aws.greengrass.shadowmanager.model.Constants.SHADOW_PUBLISH_DOCUMENTS_TOPIC;
import static com.aws.greengrass.shadowmanager.model.Constants.SHADOW_PUBLISH_REJECTED_TOPIC;

/**
 * Class to handle PubSub interaction with the PubSub Event Stream Agent.
 */
public class PubSubClientWrapper {
    private static final Logger logger = LogManager.getLogger(PubSubClientWrapper.class);
    private final PubSubIPCEventStreamAgent pubSubIPCEventStreamAgent;

    /**
     * Constructor.
     *
     * @param pubSubIPCEventStreamAgent PubSub event stream agent
     */
    @Inject
    public PubSubClientWrapper(PubSubIPCEventStreamAgent pubSubIPCEventStreamAgent) {
        this.pubSubIPCEventStreamAgent = pubSubIPCEventStreamAgent;
    }

    /**
     * Publish the message using PubSub agent when a desired operation for a shadow has been rejected.
     *
     * @param rejectRequest The request object containing the reject information.
     */
    public void reject(PubSubRequest rejectRequest) {
        handlePubSubMessagePblish(rejectRequest, SHADOW_PUBLISH_REJECTED_TOPIC);
    }

    /**
     * Publish the message using PubSub agent when a desired operation for a shadow has been accepted.
     *
     * @param acceptRequest The request object containing the accepted information.
     */
    public void accept(PubSubRequest acceptRequest) {
        handlePubSubMessagePblish(acceptRequest, SHADOW_PUBLISH_ACCEPTED_TOPIC);
    }


    /**
     * Publish the message using PubSub agent when a desired operation for a shadow has been accepted and the delta
     * information needs to be published.
     *
     * @param acceptRequest The request object containing the delta information.
     */
    public void delta(PubSubRequest acceptRequest) {
        handlePubSubMessagePblish(acceptRequest, SHADOW_PUBLISH_DELTA_TOPIC);
    }

    /**
     * Publish the message using PubSub agent when a desired operation for a shadow has been accepted and the documents
     * information needs to be published.
     *
     * @param acceptRequest The request object containing the documents information.
     */
    public void documents(PubSubRequest acceptRequest) {
        handlePubSubMessagePblish(acceptRequest, SHADOW_PUBLISH_DOCUMENTS_TOPIC);
    }

    /**
     * Publish the message using PubSub agent when a desired operation for a shadow has been accepted.
     *
     * @param pubSubRequest     The request object containing the accepted information.
     * @param shadowTopicFormat The format for the shadow topic on which to publish the message
     */
    private void handlePubSubMessagePblish(PubSubRequest pubSubRequest, String shadowTopicFormat) {
        try {
            this.pubSubIPCEventStreamAgent.publish(getShadowPublishTopic(pubSubRequest, shadowTopicFormat),
                    pubSubRequest.getPayload(), SERVICE_NAME);
            logger.atTrace()
                    .setEventType(pubSubRequest.getPublishOperation().getLogEventType())
                    .kv(LOG_THING_NAME_KEY, pubSubRequest.getThingName())
                    .kv(LOG_SHADOW_NAME_KEY, pubSubRequest.getShadowName())
                    .log("Successfully published PubSub message");
        } catch (InvalidArgumentsError e) {
            logger.atError().cause(e)
                    .kv(LOG_THING_NAME_KEY, pubSubRequest.getThingName())
                    .kv(LOG_SHADOW_NAME_KEY, pubSubRequest.getShadowName())
                    .setEventType(pubSubRequest.getPublishOperation().getLogEventType())
                    .log("Unable to publish PubSub message");
        }
    }

    /**
     * Gets the Shadow name topic prefix.
     *
     * @param ipcRequest Object that includes thingName, shadowName, and operation to form the shadow topic prefix
     * @param topic      The shadow publish topic to be added onto the topic prefix and operation
     * @return the full topic prefix for the shadow name for the publish topic.
     */
    private String getShadowPublishTopic(PubSubRequest ipcRequest, String topic) {
        String shadowTopicPrefix = ipcRequest.getShadowTopicPrefix();
        String publishTopicOp = ipcRequest.getPublishOperation().getOp();
        return shadowTopicPrefix + publishTopicOp + topic;
    }

    /**
     * Subscribes to the shadow topic over local PubSub.
     *
     * @param cb Consumer to invoke upon receiving a new message over local PubSub.
     */
    public void subscribe(Consumer<PublishEvent> cb) {
        this.pubSubIPCEventStreamAgent.subscribe(PUBSUB_SUBSCRIBE_TOPIC, cb, SHADOW_MANAGER_NAME);
    }

    /**
     * Unsubscribes to the shadow topic over local PubSub.
     *
     * @param cb Consumer to invoke upon receiving a new message over local PubSub.
     */
    public void unsubscribe(Consumer<PublishEvent> cb) {
        this.pubSubIPCEventStreamAgent.unsubscribe(PUBSUB_SUBSCRIBE_TOPIC, cb, SHADOW_MANAGER_NAME);
    }
}
