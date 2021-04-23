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
     * @throws InterruptedException Interrupt occurred while trying to update subscriptions
     */
    public synchronized void updateSubscriptions(Set<String> topics) throws InterruptedException {
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
     * @throws InterruptedException Interrupt occurred while trying to update subscriptions
     */
    private void updateTopicSubscriptions(Set<String> currentTopics, Set<String> newTopics,
                                          Consumer<MqttMessage> callback) throws InterruptedException {
        Set<String> topicsToRemove = new HashSet<>(currentTopics);
        topicsToRemove.removeAll(newTopics);
        for (String topic : topicsToRemove) {
            unsubscribeToShadow(topic, callback);
            currentTopics.remove(topic);
        }

        Set<String> topicsToSubscribe = new HashSet<>(newTopics);
        topicsToSubscribe.removeAll(currentTopics);
        for (String topic : topicsToSubscribe) {
            subscribeToShadow(topic, callback);
            currentTopics.add(topic);
        }
    }

    /**
     * Unsubscribes from all subscribed shadow topics.
     *
     * @throws InterruptedException Interrupt occurred while trying to clear subscriptions
     */
    public synchronized void clearSubscriptions() throws InterruptedException {
        for (String topic : subscribedUpdateShadowTopics) {
            unsubscribeToShadow(topic, this::handleUpdate);
            subscribedUpdateShadowTopics.remove(topic);
        }

        for (String topic : subscribedDeleteShadowTopics) {
            unsubscribeToShadow(topic, this::handleDelete);
            subscribedDeleteShadowTopics.remove(topic);
        }
    }

    /**
     * Subscribes to a shadow topic.
     *
     * @param topic    MQTT message from shadow topic
     * @param callback Callback function applied to specific topic
     * @throws InterruptedException Interrupt occurred while trying to subscribe to a shadow
     */
    private void subscribeToShadow(String topic, Consumer<MqttMessage> callback)
            throws InterruptedException {
        while (true) {
            try {
                mqttClient.subscribe(SubscribeRequest.builder().topic(topic).callback(callback).build());
                break;
            } catch (TimeoutException e) {
                logger.atWarn()
                        .setEventType(LogEvents.MQTT_CLIENT_SUBSCRIPTION_ERROR.code())
                        .kv(LOG_TOPIC, topic)
                        .setCause(e)
                        .log("Timeout occurred when attempting to subscribe shadow topic... retrying");
            } catch (ExecutionException e) {
                logger.atWarn()
                        .setEventType(LogEvents.MQTT_CLIENT_SUBSCRIPTION_ERROR.code())
                        .kv(LOG_TOPIC, topic)
                        .setCause(e)
                        .log("Unexpected exception thrown attempting to subscribe shadow topic... retrying");
            }
        }
    }

    /**
     * Unsubscribes to a shadow topic.
     *
     * @param topic    MQTT message from shadow topic
     * @param callback Callback function applied to specific topic
     * @throws InterruptedException Interrupt occurred while trying to unsubscribe to a shadow
     */
    private void unsubscribeToShadow(String topic, Consumer<MqttMessage> callback)
            throws InterruptedException {
        while (true) {
            try {
                mqttClient.unsubscribe(UnsubscribeRequest.builder().topic(topic).callback(callback).build());
                break;
            } catch (TimeoutException e) {
                logger.atWarn()
                        .setEventType(LogEvents.MQTT_CLIENT_SUBSCRIPTION_ERROR.code())
                        .kv(LOG_TOPIC, topic)
                        .setCause(e)
                        .log("Timeout occurred when attempting to unsubscribe shadow topic... retrying");
            } catch (ExecutionException e) {
                logger.atWarn()
                        .setEventType(LogEvents.MQTT_CLIENT_SUBSCRIPTION_ERROR.code())
                        .kv(LOG_TOPIC, topic)
                        .setCause(e)
                        .log("Unexpected exception thrown attempting to unsubscribe shadow topic... retrying");
            }
        }
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
