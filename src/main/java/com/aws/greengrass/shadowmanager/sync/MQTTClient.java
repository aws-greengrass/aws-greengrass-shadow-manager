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
import com.aws.greengrass.shadowmanager.ShadowManagerDAO;
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
import static com.aws.greengrass.shadowmanager.model.Constants.SHADOW_PREFIX_REGEX;
import static com.aws.greengrass.shadowmanager.model.Constants.SHADOW_UPDATE_SUBSCRIPTION_TOPIC;

/**
 * Class to subscribe to IoT Core Shadow topics.
 */
public class MQTTClient {
    private static final Logger logger = LogManager.getLogger(MQTTClient.class);
    private final SyncHandler syncHandler;
    private final ShadowManagerDAO dao;
    private final MqttClient mqttClient;
    private final Set<String> subscribedUpdateShadowTopics = new HashSet<>();
    private final Set<String> subscribedDeleteShadowTopics = new HashSet<>();
    private final Pattern shadowPattern = Pattern.compile(SHADOW_PREFIX_REGEX);

    /**
     * Ctr for MQTTClient.
     *
     * @param syncHandler Reference to the SyncHandler
     * @param mqttClient  MQTT client to connect to IoT Core
     * @param dao         Local shadow database management
     */
    @Inject
    public MQTTClient(SyncHandler syncHandler,
                      MqttClient mqttClient,
                      ShadowManagerDAO dao) {
        this.syncHandler = syncHandler;
        this.mqttClient = mqttClient;
        this.dao = dao;
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
                unsubscribeFromShadow(s, callback);
                currentTopics.remove(s);
            } catch (InterruptedException | ExecutionException | TimeoutException e) {
                logger.atError()
                        .kv(LOG_TOPIC, s)
                        .setCause(e)
                        .log("Failed to unsubscribe to cloud topic");
            }
        });

        Set<String> topicsToSubscribe = new HashSet<>(newTopics);
        topicsToSubscribe.removeAll(currentTopics);
        topicsToSubscribe.forEach(s -> {
            try {
                subscribeToShadows(s, callback);
                currentTopics.add(s);
            } catch (InterruptedException | ExecutionException | TimeoutException e) {
                logger.atError()
                        .kv(LOG_TOPIC, s)
                        .setCause(e)
                        .log("Failed to subscribe to cloud topic");
            }
        });
    }


    /**
     * Unsubscribes from all topics.
     */
    public synchronized void clearSubscriptions() {
        subscribedUpdateShadowTopics.forEach(s -> {
            try {
                unsubscribeFromShadow(s, this::handleUpdate);
                subscribedUpdateShadowTopics.remove(s);
            } catch (InterruptedException | ExecutionException | TimeoutException e) {
                logger.atError()
                        .kv(LOG_TOPIC, s)
                        .setCause(e)
                        .log("Failed to unsubscribe from cloud topic");
            }
        });

        subscribedDeleteShadowTopics.forEach(s -> {
            try {
                unsubscribeFromShadow(s, this::handleDelete);
                subscribedDeleteShadowTopics.remove(s);
            } catch (InterruptedException | ExecutionException | TimeoutException e) {
                logger.atError()
                        .kv(LOG_TOPIC, s)
                        .setCause(e)
                        .log("Failed to unsubscribe from cloud topic");
            }
        });
    }

    /**
     * Subscribes to a specific shadow MQTT topic.
     *
     * @param topic    MQTT topic to subscribe to
     * @param callback callback function to handle messages from topic
     */
    private void subscribeToShadows(String topic, Consumer<MqttMessage> callback)
            throws InterruptedException, ExecutionException, TimeoutException {
        mqttClient.subscribe(SubscribeRequest.builder()
                .topic(topic)
                .callback(callback)
                .build());
    }

    /**
     * Unsubscribes to a specific shadow MQTT topic.
     *
     * @param topic    MQTT topic to subscribe to
     * @param callback callback function to handle messages from topic
     */
    private void unsubscribeFromShadow(String topic, Consumer<MqttMessage> callback)
            throws InterruptedException, ExecutionException, TimeoutException {
        mqttClient.unsubscribe(UnsubscribeRequest.builder()
                .topic(topic)
                .callback(callback)
                .build());
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
