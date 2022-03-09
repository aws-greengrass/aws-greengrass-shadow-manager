/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.shadowmanager;

import com.aws.greengrass.builtin.services.pubsub.PublishEvent;
import com.aws.greengrass.logging.api.Logger;
import com.aws.greengrass.logging.impl.LogManager;
import com.aws.greengrass.shadowmanager.exception.InvalidRequestParametersException;
import com.aws.greengrass.shadowmanager.ipc.DeleteThingShadowRequestHandler;
import com.aws.greengrass.shadowmanager.ipc.GetThingShadowRequestHandler;
import com.aws.greengrass.shadowmanager.ipc.PubSubClientWrapper;
import com.aws.greengrass.shadowmanager.ipc.UpdateThingShadowRequestHandler;
import com.aws.greengrass.shadowmanager.model.ShadowRequest;
import software.amazon.awssdk.aws.greengrass.model.DeleteThingShadowRequest;
import software.amazon.awssdk.aws.greengrass.model.GetThingShadowRequest;
import software.amazon.awssdk.aws.greengrass.model.GreengrassCoreIPCError;
import software.amazon.awssdk.aws.greengrass.model.UpdateThingShadowRequest;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.aws.greengrass.shadowmanager.model.Constants.LOG_OPERATION;
import static com.aws.greengrass.shadowmanager.model.Constants.LOG_SHADOW_NAME_KEY;
import static com.aws.greengrass.shadowmanager.model.Constants.LOG_THING_NAME_KEY;
import static com.aws.greengrass.shadowmanager.model.Constants.LOG_TOPIC;
import static com.aws.greengrass.shadowmanager.model.Constants.SHADOW_MANAGER_NAME;


/**
 * Class that handles PubSub shadow topic subscription and managing any publish events on the shadow topic.
 */
public class PubSubIntegrator {
    private static final Logger logger = LogManager.getLogger(PubSubIntegrator.class);

    private final DeleteThingShadowRequestHandler deleteThingShadowRequestHandler;
    private final UpdateThingShadowRequestHandler updateThingShadowRequestHandler;
    private final GetThingShadowRequestHandler getThingShadowRequestHandler;
    private final PubSubClientWrapper pubSubClientWrapper;
    private final Pattern shadowPattern = Pattern.compile("\\$aws\\/things\\/(.*)\\/shadow(\\/name\\/(.*))?"
            + "\\/(update|delete|get)$");
    private final Pattern shadowResponsePattern = Pattern.compile("\\$aws\\/things\\/(.*)\\/shadow(\\/name\\/(.*))"
            + "?\\/(update|delete|get)\\/(accepted|rejected|delta|documents)$");
    private final AtomicBoolean subscribed = new AtomicBoolean(false);

    /**
     * Constructor for PubSubIntegrator.
     *
     * @param pubSubClientWrapper             The PubSub client wrapper
     * @param deleteThingShadowRequestHandler handler class to handle the Delete Shadow request.
     * @param updateThingShadowRequestHandler handler class to handle the Update Shadow request.
     * @param getThingShadowRequestHandler    handler class to handle the Get Shadow request.
     */
    public PubSubIntegrator(PubSubClientWrapper pubSubClientWrapper,
                            DeleteThingShadowRequestHandler deleteThingShadowRequestHandler,
                            UpdateThingShadowRequestHandler updateThingShadowRequestHandler,
                            GetThingShadowRequestHandler getThingShadowRequestHandler) {
        this.pubSubClientWrapper = pubSubClientWrapper;
        this.deleteThingShadowRequestHandler = deleteThingShadowRequestHandler;
        this.updateThingShadowRequestHandler = updateThingShadowRequestHandler;
        this.getThingShadowRequestHandler = getThingShadowRequestHandler;
    }

    /**
     * Subscribes to the shadow topic over local PubSub.
     */
    public void subscribe() {
        if (this.subscribed.compareAndSet(false, true)) {
            this.pubSubClientWrapper.subscribe(this::handlePublishedMessage);
        }
    }

    /**
     * Unsubscribes to the shadow topic over local PubSub.
     */
    public void unsubscribe() {
        if (this.subscribed.compareAndSet(true, false)) {
            this.pubSubClientWrapper.unsubscribe(this::handlePublishedMessage);
        }

    }

    /**
     * Handle the new message that is published over local PubSub. Extract the necessary information from the shadow
     * topic to accurately route the message to the appropriate handler.
     * It will ignore the message if it is unable to extract all the necessary information from the shadow topic.
     *
     * @param publishEvent The message that is published over local PubSub.
     */
    private void handlePublishedMessage(PublishEvent publishEvent) {
        String topic = publishEvent.getTopic();

        if (isResponseMessage(topic)) {
            logger.atDebug().kv(LOG_TOPIC, topic)
                    .log("Not processing message since it is a response message to a shadow operation");
            return;
        }

        logger.atDebug().kv(LOG_TOPIC, topic).log("Processing new shadow operation message over local PubSub");
        ShadowRequest shadowRequest;
        try {
            shadowRequest = extractShadowFromTopic(topic);
        } catch (IllegalArgumentException e) {
            logger.atWarn()
                    .setCause(e)
                    .kv(LOG_TOPIC, topic)
                    .log("Unable to process shadow operation request over PubSub");
            return;
        }
        try {
            switch (shadowRequest.getOperation().toLowerCase()) {
                case "update":
                    UpdateThingShadowRequest request = new UpdateThingShadowRequest();
                    request.setThingName(shadowRequest.getThingName());
                    request.setShadowName(shadowRequest.getShadowName());
                    request.setPayload(publishEvent.getPayload());

                    this.updateThingShadowRequestHandler.handleRequest(request, SHADOW_MANAGER_NAME);
                    break;
                case "delete":
                    DeleteThingShadowRequest deleteRequest = new DeleteThingShadowRequest();
                    deleteRequest.setThingName(shadowRequest.getThingName());
                    deleteRequest.setShadowName(shadowRequest.getShadowName());

                    this.deleteThingShadowRequestHandler.handleRequest(deleteRequest, SHADOW_MANAGER_NAME);
                    break;
                case "get":
                    GetThingShadowRequest getRequest = new GetThingShadowRequest();
                    getRequest.setThingName(shadowRequest.getThingName());
                    getRequest.setShadowName(shadowRequest.getShadowName());

                    this.getThingShadowRequestHandler.handleRequest(getRequest, SHADOW_MANAGER_NAME);
                    break;
                default:
                    logger.atWarn()
                            .kv(LOG_THING_NAME_KEY, shadowRequest.getThingName())
                            .kv(LOG_SHADOW_NAME_KEY, shadowRequest.getShadowName())
                            .kv(LOG_OPERATION, shadowRequest.getOperation())
                            .log("Unable to perform shadow operation due to unknown operation value");
                    break;
            }
            logger.atDebug().kv(LOG_TOPIC, topic)
                    .log("Finished processing new shadow operation message over local PubSub");
        } catch (InvalidRequestParametersException | GreengrassCoreIPCError e) {
            // The only other exception that can be thrown AuthorizationException which will not happen since we are
            // sending the shadow manager service name to be authorized which will always be authorized.
            // Not setting the cause here since it would already be logged in the caller.
            logger.atWarn()
                    .kv(LOG_THING_NAME_KEY, shadowRequest.getThingName())
                    .kv(LOG_SHADOW_NAME_KEY, shadowRequest.getShadowName())
                    .kv(LOG_OPERATION, shadowRequest.getOperation())
                    .log("Unable to perform shadow operation");
        }
    }

    /**
     * Helper function to extract the thingName, shadowName and shadow operation from mqtt topic.
     *
     * @param topic PubSub topic on which the message was sent.
     * @return ShadowRequest object with shadow details
     */
    ShadowRequest extractShadowFromTopic(String topic) {
        final Matcher matcher = shadowPattern.matcher(topic);

        if (matcher.find() && matcher.groupCount() == 4) {
            String thingName = matcher.group(1);
            String shadowName = matcher.group(3);
            String operation = matcher.group(4);
            return new ShadowRequest(thingName, shadowName, operation);
        }
        logger.atWarn()
                .kv("topic", topic)
                .log("Unable to parse shadow topic for thing name, shadow name and shadow operation");
        throw new IllegalArgumentException("Unable to parse shadow topic");
    }

    /**
     * Checks if the message received is a response message or not.
     *
     * @param topic the topic on which the PubSub message was sent.
     * @return true if the message is a response message to a shadow operation; Else false.
     */
    boolean isResponseMessage(String topic) {
        final Matcher matcher = shadowResponsePattern.matcher(topic);
        return matcher.matches();
    }
}
