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
import com.aws.greengrass.util.Pair;
import software.amazon.awssdk.crt.mqtt.MqttMessage;

import java.util.Collections;
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
    private Thread subscriberThread;
    private volatile boolean running = false;
    private static final int MAX_RETRY_COUNT = 5;

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
     * Updates and subscribes to set of update/delete topics for set of shadows.
     *
     * @param shadowSet Set of shadow topic prefixes to subscribe to the update/delete topic
     * @throws InterruptedException Interrupt occurred when trying to reset subscriber thread
     */
    public synchronized void updateSubscriptions(Set<Pair<String, String>> shadowSet) throws InterruptedException {
        Set<String> newUpdateTopics = new HashSet<>();
        Set<String> newDeleteTopics = new HashSet<>();

        for (Pair<String, String> shadow : shadowSet) {
            ShadowRequest request = new ShadowRequest(shadow.getLeft(), shadow.getRight());
            newUpdateTopics.add(request.getShadowTopicPrefix() + SHADOW_UPDATE_SUBSCRIPTION_TOPIC);
            newDeleteTopics.add(request.getShadowTopicPrefix() + SHADOW_DELETE_SUBSCRIPTION_TOPIC);
        }

        if (subscriberThread != null && subscriberThread.isAlive()) {
            running = false;
            Thread.sleep(1000);
        }

        running = true;
        subscriberThread = new Thread(() -> updateSubscriptions(newUpdateTopics, newDeleteTopics));
        subscriberThread.start();
    }

    /**
     * Updates subscriptions for set of update and delete shadow topics.
     *
     * @param updateTopics Set of update shadow topics to subscribe to
     * @param deleteTopics Set of delete shadow topics to subscribe to
     */
    private void updateSubscriptions(Set<String> updateTopics, Set<String> deleteTopics) {

        // get update topics to remove and subscribe
        Set<String> updateTopicsToRemove = new HashSet<>(subscribedUpdateShadowTopics);
        updateTopicsToRemove.removeAll(updateTopics);

        Set<String> updateTopicsToSubscribe = new HashSet<>(updateTopics);
        updateTopicsToSubscribe.removeAll(subscribedUpdateShadowTopics);

        Set<String> deleteTopicsToRemove = new HashSet<>(subscribedDeleteShadowTopics);
        deleteTopicsToRemove.removeAll(deleteTopics);

        Set<String> deleteTopicsToSubscribe = new HashSet<>(deleteTopics);
        deleteTopicsToSubscribe.removeAll(subscribedDeleteShadowTopics);

        int retryCount = 0;

        // TODO: Implement exponential backoff algorithm
        while (!updateTopicsToRemove.isEmpty() || !updateTopicsToSubscribe.isEmpty()
                || !deleteTopicsToRemove.isEmpty() || !deleteTopicsToSubscribe.isEmpty()
                && running) {
            try {
                Thread.sleep(retryCount * 1000);

                unsubscribeToShadows(subscribedUpdateShadowTopics, updateTopicsToRemove, this::handleUpdate);
                subscribeToShadows(subscribedUpdateShadowTopics, updateTopicsToSubscribe, this::handleUpdate);

                unsubscribeToShadows(subscribedDeleteShadowTopics, deleteTopicsToRemove, this::handleDelete);
                subscribeToShadows(subscribedDeleteShadowTopics, deleteTopicsToSubscribe, this::handleDelete);

                if (retryCount < MAX_RETRY_COUNT) {
                    retryCount++;
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                logger.atError()
                        .setEventType(LogEvents.CLOUD_DATA_CLIENT_SUBSCRIPTION_ERROR.code())
                        .setCause(e)
                        .log("Failed to update subscriptions");
                running = false;
                return;
            }
        }

        logger.atDebug()
                .setEventType(LogEvents.CLOUD_DATA_CLIENT_SUBSCRIPTION_ERROR.code())
                .log("Finished updating subscriptions");
        running = false;
    }

    /**
     * Unsubscribes from all subscribed shadow topics.
     *
     * @throws InterruptedException Interrupt occurred while trying to clear subscriptions
     */
    public synchronized void clearSubscriptions() throws InterruptedException {
        if (subscriberThread != null && subscriberThread.isAlive()) {
            running = false;
            Thread.sleep(1000);
        }

        subscriberThread = new Thread(() -> updateSubscriptions(Collections.emptySet(), Collections.emptySet()));
        subscriberThread.start();
    }

    /**
     * Unsubscribes to a given set of shadow topics.
     *
     * @param currentTopics       Set of shadow topics being tracked by the CloudDataClient
     * @param topicsToUnsubscribe Set of shadow topics to unsubscribe to
     * @param callback            Callback function applied to shadow topic
     * @throws InterruptedException Interrupt occurred while trying to unsubscribe to shadows
     */
    private void unsubscribeToShadows(Set<String> currentTopics, Set<String> topicsToUnsubscribe,
                                      Consumer<MqttMessage> callback) throws InterruptedException {
        Set<String> tempHashSet = new HashSet<>(topicsToUnsubscribe);
        for (String topic : tempHashSet) {
            try {
                mqttClient.unsubscribe(UnsubscribeRequest.builder().topic(topic).callback(callback).build());
                topicsToUnsubscribe.remove(topic);
                currentTopics.remove(topic);
            } catch (TimeoutException | ExecutionException e) {
                logger.atWarn()
                        .setEventType(LogEvents.CLOUD_DATA_CLIENT_SUBSCRIPTION_ERROR.code())
                        .kv(LOG_TOPIC, topic)
                        .setCause(e)
                        .log("Failed to unsubscribe to shadow topic");
            }
        }
    }

    /**
     * Subscribes to a given set of shadow topics.
     *
     * @param currentTopics     Set of shadow topics being tracked by the CloudDataClient
     * @param topicsToSubscribe Set of shadow topics to subscribe to
     * @param callback          Callback function applied to shadow topic
     * @throws InterruptedException Interrupt occurred while trying to subscribe to shadows
     */
    private void subscribeToShadows(Set<String> currentTopics, Set<String> topicsToSubscribe,
                                    Consumer<MqttMessage> callback) throws InterruptedException {
        Set<String> tempHashSet = new HashSet<>(topicsToSubscribe);
        for (String topic : tempHashSet) {
            try {
                mqttClient.subscribe(SubscribeRequest.builder().topic(topic).callback(callback).build());
                topicsToSubscribe.remove(topic);
                currentTopics.add(topic);
            } catch (TimeoutException | ExecutionException e) {
                logger.atWarn()
                        .setEventType(LogEvents.CLOUD_DATA_CLIENT_SUBSCRIPTION_ERROR.code())
                        .kv(LOG_TOPIC, topic)
                        .setCause(e)
                        .log("Failed to subscribe to shadow topic");
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
