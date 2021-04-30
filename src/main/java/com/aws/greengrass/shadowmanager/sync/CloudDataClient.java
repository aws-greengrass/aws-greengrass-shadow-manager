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
import com.aws.greengrass.shadowmanager.exception.SubscriptionRetryException;
import com.aws.greengrass.shadowmanager.model.LogEvents;
import com.aws.greengrass.shadowmanager.model.ShadowRequest;
import com.aws.greengrass.util.Pair;
import com.aws.greengrass.util.RetryUtils;
import software.amazon.awssdk.crt.mqtt.MqttMessage;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.inject.Inject;

import static com.aws.greengrass.shadowmanager.model.Constants.LOG_SHADOW_NAME_KEY;
import static com.aws.greengrass.shadowmanager.model.Constants.LOG_THING_NAME_KEY;
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
    private final Pattern shadowPattern = Pattern.compile("\\$aws\\/things\\/(.*)\\/shadow(\\/name\\/(.*))?"
            + "\\/(update|delete)\\/(accepted|rejected|delta|documents)");
    // TODO: use ExecutorService for managing threads
    private Thread subscriberThread;
    private static final RetryUtils.RetryConfig RETRY_CONFIG = RetryUtils.RetryConfig.builder()
            .maxAttempt(Integer.MAX_VALUE)
            .initialRetryInterval(Duration.of(3, ChronoUnit.SECONDS))
            .maxRetryInterval(Duration.of(1, ChronoUnit.MINUTES))
            .retryableExceptions(Collections.singletonList(SubscriptionRetryException.class))
            .build();

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
     * Stops the mqtt subscriber thread.
     */
    public void stopSubscribing() {
        if (subscriberThread != null && subscriberThread.isAlive()) {
            subscriberThread.interrupt();
        }
    }

    /**
     * Updates and subscribes to set of update/delete topics for set of shadows.
     *
     * @param shadowSet Set of shadow topic prefixes to subscribe to the update/delete topic
     */
    public void updateSubscriptions(Set<Pair<String, String>> shadowSet) {
        Set<String> newUpdateTopics = new HashSet<>();
        Set<String> newDeleteTopics = new HashSet<>();

        for (Pair<String, String> shadow : shadowSet) {
            ShadowRequest request = new ShadowRequest(shadow.getLeft(), shadow.getRight());
            newUpdateTopics.add(request.getShadowTopicPrefix() + SHADOW_UPDATE_SUBSCRIPTION_TOPIC);
            newDeleteTopics.add(request.getShadowTopicPrefix() + SHADOW_DELETE_SUBSCRIPTION_TOPIC);
        }

        stopSubscribing();
        subscriberThread = new Thread(() -> updateSubscriptions(newUpdateTopics, newDeleteTopics));
        subscriberThread.start();
    }

    /**
     * Updates subscriptions for set of update and delete shadow topics.
     *
     * @param updateTopics Set of update shadow topics to subscribe to
     * @param deleteTopics Set of delete shadow topics to subscribe to
     */
    private synchronized void updateSubscriptions(Set<String> updateTopics, Set<String> deleteTopics) {
        if (!mqttClient.connected()) {
            logger.atWarn()
                    .setEventType(LogEvents.CLOUD_DATA_CLIENT_SUBSCRIPTION_ERROR.code())
                    .log("Attempting to update subscriptions when offline");
            return;
        }

        // get update topics to remove and subscribe
        Set<String> updateTopicsToRemove = new HashSet<>(subscribedUpdateShadowTopics);
        updateTopicsToRemove.removeAll(updateTopics);

        Set<String> updateTopicsToSubscribe = new HashSet<>(updateTopics);
        updateTopicsToSubscribe.removeAll(subscribedUpdateShadowTopics);

        Set<String> deleteTopicsToRemove = new HashSet<>(subscribedDeleteShadowTopics);
        deleteTopicsToRemove.removeAll(deleteTopics);

        Set<String> deleteTopicsToSubscribe = new HashSet<>(deleteTopics);
        deleteTopicsToSubscribe.removeAll(subscribedDeleteShadowTopics);

        boolean success;
        try {
            success = RetryUtils.runWithRetry(RETRY_CONFIG, () -> {
                unsubscribeToShadows(subscribedUpdateShadowTopics, updateTopicsToRemove, this::handleUpdate);
                subscribeToShadows(subscribedUpdateShadowTopics, updateTopicsToSubscribe, this::handleUpdate);

                unsubscribeToShadows(subscribedDeleteShadowTopics, deleteTopicsToRemove, this::handleDelete);
                subscribeToShadows(subscribedDeleteShadowTopics, deleteTopicsToSubscribe, this::handleDelete);

                if (!updateTopicsToRemove.isEmpty() || !updateTopicsToSubscribe.isEmpty()
                        || !deleteTopicsToRemove.isEmpty() || !deleteTopicsToSubscribe.isEmpty()
                        && !Thread.currentThread().isInterrupted()) {

                    throw new SubscriptionRetryException("Missed shadow topics to (un)subscribe to");
                }

                // if interrupted then handle
                if (Thread.currentThread().isInterrupted()) {
                    Thread.currentThread().interrupt();
                    logger.atError()
                            .setEventType(LogEvents.CLOUD_DATA_CLIENT_SUBSCRIPTION_ERROR.code())
                            .log("Failed to update subscriptions");
                    return false;
                }

                return true;
            }, LogEvents.CLOUD_DATA_CLIENT_SUBSCRIPTION_ERROR.code(), logger);

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            logger.atError()
                    .setEventType(LogEvents.CLOUD_DATA_CLIENT_SUBSCRIPTION_ERROR.code())
                    .setCause(e)
                    .log("Failed to update subscriptions");
            return;
        } catch (Exception e) { // NOPMD - thrown by RetryUtils.runWithRetry()
            logger.atError()
                    .setEventType(LogEvents.CLOUD_DATA_CLIENT_SUBSCRIPTION_ERROR.code())
                    .setCause(e)
                    .log("Failed to update subscriptions");
            return;
        }

        if (success) {
            logger.atDebug()
                    .setEventType(LogEvents.CLOUD_DATA_CLIENT_SUBSCRIPTION_ERROR.code())
                    .log("Finished updating subscriptions");
        }
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
                logger.atDebug().log("Unsubscribed to {}", topic);
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
                logger.atDebug().log("Subscribed to {}", topic);
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
        logger.atDebug().kv(LOG_THING_NAME_KEY, thingName).kv(LOG_SHADOW_NAME_KEY, shadowName)
                .log("Received cloud update request");
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
        logger.atDebug().kv(LOG_THING_NAME_KEY, thingName).kv(LOG_SHADOW_NAME_KEY, shadowName)
                .log("Received cloud delete request");
        syncHandler.pushLocalDeleteSyncRequest(thingName, shadowName, message.getPayload());
    }

    /**
     * Helper function to extract the thingName and shadowName from mqtt topic.
     *
     * @param topic MQTT message from shadow topic
     * @return ShadowRequest object with shadow details
     */
    ShadowRequest extractShadowFromTopic(String topic) {
        final Matcher matcher = shadowPattern.matcher(topic);

        if (matcher.find()) {
            String thingName = matcher.group(1);
            String shadowName = matcher.group(3);
            return new ShadowRequest(thingName, shadowName);
        }
        logger.atWarn()
                .kv("topic", topic)
                .log("Unable to parse shadow topic for thing name and shadow name");
        throw new IllegalArgumentException("Unable to parse shadow topic for thing name and shadow name");
    }
}
