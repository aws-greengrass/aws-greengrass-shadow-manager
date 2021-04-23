/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.shadowmanager.sync;

import com.aws.greengrass.logging.api.Logger;
import com.aws.greengrass.logging.impl.LogManager;
import com.aws.greengrass.mqttclient.MqttClient;
import com.aws.greengrass.mqttclient.SubscribeRequest;
import com.aws.greengrass.mqttclient.UnsubscribeRequest;
import com.aws.greengrass.shadowmanager.model.LogEvents;
import com.aws.greengrass.shadowmanager.model.ShadowRequest;
import software.amazon.awssdk.crt.mqtt.MqttMessage;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.inject.Inject;

import static com.aws.greengrass.shadowmanager.model.Constants.LOG_TOPIC;
import static com.aws.greengrass.shadowmanager.model.Constants.SHADOW_DELETE_SUBSCRIPTION_TOPIC;
import static com.aws.greengrass.shadowmanager.model.Constants.SHADOW_UPDATE_SUBSCRIPTION_TOPIC;

/**
 * Class to subscribe to IoT Core Shadow topics.
 */
public class CloudDataClient {
    private static final Logger logger = LogManager.getLogger(CloudDataClient.class);
    private final SyncHandler syncHandler;
    private final MqttClient mqttClient;
    private final Set<String> subscribedUpdateShadowTopics = new HashSet<>();
    private final Set<String> subscribedDeleteShadowTopics = new HashSet<>();
    private final Pattern shadowPattern = Pattern.compile("\\$aws\\/things\\/(.*)\\/shadow(\\/name\\/(.*))?\\/");
    private final int maxRetryCount = 3;

    /**
     * Ctr for CloudDataClient.
     *
     * @param syncHandler Reference to the SyncHandler
     * @param mqttClient  MQTT client to connect to IoT Core
     */
    @Inject
    public CloudDataClient(SyncHandler syncHandler,
                           MqttClient mqttClient) {
        this.syncHandler = syncHandler;
        this.mqttClient = mqttClient;
    }

    /**
     * Updates and subscribes to set of update/delete topics.
     * TODO: determine correct input type based on who processes configuration
     *
     * @param topics Set of shadow topic prefixes to subscribe to the update/delete topic
     */
    public synchronized void updateSubscriptions(Set<String> topics) {
        Set<String> newUpdateTopics = new HashSet<>();
        Set<String> newDeleteTopics = new HashSet<>();
        topics.forEach(s -> {
            newUpdateTopics.add(s + SHADOW_UPDATE_SUBSCRIPTION_TOPIC);
            newDeleteTopics.add(s + SHADOW_DELETE_SUBSCRIPTION_TOPIC);
        });

        // keep track of update/delete topics separately to keep track of faulty subscriptions
        updateTopicSubscriptions(subscribedUpdateShadowTopics, newUpdateTopics, this::handleUpdate);
        updateTopicSubscriptions(subscribedDeleteShadowTopics, newDeleteTopics, this::handleDelete);
    }

    /**
     * Updates the subscriptions for a specific set of topics. This is generalized to apply to either update/delete
     * topics so that both topics can be tracked separately in case of failures.
     *
     * @param currentTopics Set of topics currently subscribed to
     * @param newTopics     New set of topics to subscribe to
     * @param callback      Callback function applied to specific topic
     */
    private void updateTopicSubscriptions(Set<String> currentTopics,
                                          Set<String> newTopics,
                                          Consumer<MqttMessage> callback) {
        Set<String> topicsToRemove = new HashSet<>(currentTopics);
        topicsToRemove.removeAll(newTopics);
        topicsToRemove.forEach(s -> {
            try {
                if (unsubscribeToShadow(s, callback)) {
                    currentTopics.remove(s);
                } else {
                    logger.atError()
                            .setEventType(LogEvents.MQTT_CLIENT_SUBSCRIPTION_ERROR.code())
                            .kv(LOG_TOPIC, s)
                            .log("Max unsubscription retries reached. Failed to unsubscribe to topic.");
                }
            } catch (InterruptedException | ExecutionException e) {
                logger.atError()
                        .setEventType(LogEvents.MQTT_CLIENT_SUBSCRIPTION_ERROR.code())
                        .kv(LOG_TOPIC, s)
                        .setCause(e)
                        .log("Failed to unsubscribe to cloud topic");
            }
        });

        Set<String> topicsToSubscribe = new HashSet<>(newTopics);
        topicsToSubscribe.removeAll(currentTopics);
        topicsToSubscribe.forEach(s -> {
            try {
                if (subscribeToShadow(s, callback)) {
                    currentTopics.add(s);
                } else {
                    logger.atError()
                            .setEventType(LogEvents.MQTT_CLIENT_SUBSCRIPTION_ERROR.code())
                            .kv(LOG_TOPIC, s)
                            .log("Max subscription retries reached. Failed to subscribe to topic.");
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();

            } catch (ExecutionException e) {
                logger.atError()
                        .setEventType(LogEvents.MQTT_CLIENT_SUBSCRIPTION_ERROR.code())
                        .kv(LOG_TOPIC, s)
                        .setCause(e)
                        .log("Failed to subscribe to cloud topic");
            }
        });
    }

    /**
     * Unsubscribes from all subscribed shadow topics.
     */
    public synchronized void clearSubscriptions() {
        unsubscribeToShadows(subscribedUpdateShadowTopics, this::handleUpdate);
        unsubscribeToShadows(subscribedDeleteShadowTopics, this::handleDelete);
    }

    /**
     * Unsubscribes to a given list of subscription topics.
     *
     * @param subscriptionList List of subscriptions to unsubscribe to
     * @param callback         Callback function applied to set of subscriptions
     */
    private void unsubscribeToShadows(Set<String> subscriptionList, Consumer<MqttMessage> callback) {
        subscriptionList.forEach(s -> {
            try {
                if (unsubscribeToShadow(s, callback)) {
                    subscriptionList.remove(s);
                } else {
                    logger.atError()
                            .setEventType(LogEvents.MQTT_CLIENT_SUBSCRIPTION_ERROR.code())
                            .kv(LOG_TOPIC, s)
                            .log("Max unsubscription retries reached. Failed to unsubscribe to topic.");
                }
            } catch (InterruptedException | ExecutionException e) {
                logger.atError()
                        .setEventType(LogEvents.MQTT_CLIENT_SUBSCRIPTION_ERROR.code())
                        .kv(LOG_TOPIC, s)
                        .setCause(e)
                        .log("Failed to unsubscribe from cloud topic");
            }
        });
    }

    /**
     * Subscribes to a shadow topic.
     *
     * @param topic    MQTT message from shadow topic
     * @param callback Callback function applied to specific topic
     * @return Whether the subscribe request succeeded
     */
    private boolean subscribeToShadow(String topic, Consumer<MqttMessage> callback)
            throws InterruptedException, ExecutionException {
        for (int i = 0; i < maxRetryCount; i++) {
            try {
                mqttClient.subscribe(SubscribeRequest.builder().topic(topic).callback(callback).build());
                return true;
            } catch (TimeoutException e) {
                logger.atWarn()
                        .setEventType(LogEvents.MQTT_CLIENT_SUBSCRIPTION_ERROR.code())
                        .kv(LOG_TOPIC, topic)
                        .setCause(e)
                        .log("Timeout occurred when subscribing to shadow topic.");
            }
        }

        return false;
    }

    /**
     * Unsubscribes to a shadow topic.
     *
     * @param topic    MQTT message from shadow topic
     * @param callback Callback function applied to specific topic
     * @return Whether the unsubscribes request succeeded
     */
    private boolean unsubscribeToShadow(String topic, Consumer<MqttMessage> callback)
            throws InterruptedException, ExecutionException {
        for (int i = 0; i < maxRetryCount; i++) {
            try {
                mqttClient.unsubscribe(UnsubscribeRequest.builder().topic(topic).callback(callback).build());
                return true;
            } catch (TimeoutException e) {
                logger.atWarn()
                        .setEventType(LogEvents.MQTT_CLIENT_SUBSCRIPTION_ERROR.code())
                        .kv(LOG_TOPIC, topic)
                        .setCause(e)
                        .log("Timeout occurred when unsubscribing to shadow topic.");
            }
        }

        return false;
    }

    /**
     * Calls on the SyncHandler to create a LocalUpdateSyncRequest with information from the mqtt message.
     *
     * @param message MQTT message from shadow topic
     */
    private void handleUpdate(MqttMessage message) {
        String topic = message.getTopic();
        ShadowRequest shadowRequest = extractShadowFromTopic(topic);
        String thingName = shadowRequest.getThingName();
        String shadowName = shadowRequest.getShadowName();
        syncHandler.pushLocalUpdateSyncRequest(thingName, shadowName, message.getPayload());
    }

    /**
     * Calls on the SyncHandler to create a LocalDeleteSyncRequest with information from the mqtt message.
     *
     * @param message MQTT message from shadow topic
     */
    private void handleDelete(MqttMessage message) {
        String topic = message.getTopic();
        ShadowRequest shadowRequest = extractShadowFromTopic(topic);
        String thingName = shadowRequest.getThingName();
        String shadowName = shadowRequest.getShadowName();
        syncHandler.pushLocalDeleteSyncRequest(thingName, shadowName);
    }

    /**
     * Helper function to extract the thingName and shadowName from mqtt topic.
     *
     * @param topic MQTT message from shadow topic
     * @return ShadowRequest object with shadow details
     */
    private ShadowRequest extractShadowFromTopic(String topic) {
        Matcher m = shadowPattern.matcher(topic);
        String thingName = m.group(1);
        String shadowName = m.group(3);
        return new ShadowRequest(thingName, shadowName);
    }
}
