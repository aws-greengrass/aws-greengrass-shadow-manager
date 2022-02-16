/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */


package com.aws.greengrass.shadowmanager.sync;

import com.aws.greengrass.mqttclient.MqttClient;
import com.aws.greengrass.mqttclient.SubscribeRequest;
import com.aws.greengrass.mqttclient.UnsubscribeRequest;
import com.aws.greengrass.shadowmanager.exception.SubscriptionRetryException;
import com.aws.greengrass.shadowmanager.model.ShadowRequest;
import com.aws.greengrass.testcommons.testutilities.GGExtension;
import com.aws.greengrass.util.Pair;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static com.aws.greengrass.shadowmanager.model.Constants.CLASSIC_SHADOW_IDENTIFIER;
import static com.aws.greengrass.shadowmanager.model.Constants.SHADOW_DELETE_SUBSCRIPTION_TOPIC;
import static com.aws.greengrass.shadowmanager.model.Constants.SHADOW_UPDATE_SUBSCRIPTION_TOPIC;
import static com.aws.greengrass.testcommons.testutilities.ExceptionLogProtector.ignoreExceptionOfType;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

@ExtendWith({MockitoExtension.class, GGExtension.class})
class CloudDataClientTest {
    private final ExecutorService executorService = Executors.newSingleThreadExecutor();
    private static final Set<Pair<String, String>> SHADOW_SET = new HashSet<Pair<String, String>>() {{
        add(new Pair<>("thing1", "shadow1"));
        add(new Pair<>("thing2", "shadow2"));
        add(new Pair<>("thing3", "shadow3"));
    }};

    @Mock
    MqttClient mockMqttClient;

    @Mock
    SyncHandler mockSyncHandler;

    @Captor
    private ArgumentCaptor<SubscribeRequest> subscribeRequestCaptor;

    @Captor
    private ArgumentCaptor<UnsubscribeRequest> unsubscribeRequestCaptor;

    @BeforeEach
    void setup() throws InterruptedException, ExecutionException, TimeoutException {
        lenient().doNothing().when(mockMqttClient).subscribe(subscribeRequestCaptor.capture());
        lenient().doNothing().when(mockMqttClient).unsubscribe(unsubscribeRequestCaptor.capture());
        lenient().doReturn(true).when(mockMqttClient).connected();
    }

    private Set<String> getTopicSet(Set<Pair<String, String>> shadowSet) {
        Set<String> returnSet = new HashSet<>();

        for (Pair<String, String> shadow : shadowSet) {
            ShadowRequest request = new ShadowRequest(shadow.getLeft(), shadow.getRight());
            returnSet.add(request.getShadowTopicPrefix() + SHADOW_UPDATE_SUBSCRIPTION_TOPIC);
            returnSet.add(request.getShadowTopicPrefix() + SHADOW_DELETE_SUBSCRIPTION_TOPIC);
        }
        return returnSet;
    }

    @Test
    void GIVEN_set_of_shadows_when_empty_shadows_WHEN_update_subscriptions_THEN_subscriptions_updated() throws InterruptedException, TimeoutException, ExecutionException {
        Set<String> topicSet = getTopicSet(SHADOW_SET);
        CloudDataClient cloudDataClient = new CloudDataClient(mockSyncHandler, mockMqttClient, executorService);
        cloudDataClient.updateSubscriptions(SHADOW_SET);
        TimeUnit.MILLISECONDS.sleep(2000);

        verify(mockMqttClient, times(6)).subscribe(any(SubscribeRequest.class));
        verify(mockMqttClient, times(0)).unsubscribe(any(UnsubscribeRequest.class));

        for (int i = 0; i < subscribeRequestCaptor.getAllValues().size(); i++) {
            assertThat(topicSet, hasItem(subscribeRequestCaptor.getAllValues().get(i).getTopic()));
        }
    }

    @Test
    void GIVEN_existing_shadows_and_update_with_new_set_WHEN_update_subscriptions_THEN_subscriptions_updated(ExtensionContext context) throws InterruptedException, TimeoutException, ExecutionException {
        ignoreExceptionOfType(context, InterruptedException.class);
        final Set<Pair<String, String>> newShadowSet = new HashSet<Pair<String, String>>() {{
            add(new Pair<>("thing1", "shadow1"));
            add(new Pair<>("thing2", "shadow2"));
            add(new Pair<>("newThing5", "newShadow5"));
        }};

        final Set<String> deletedShadowSet = new HashSet<String>() {{
            ShadowRequest shadowRequest = new ShadowRequest("thing3", "shadow3");
            add(shadowRequest.getShadowTopicPrefix() + SHADOW_UPDATE_SUBSCRIPTION_TOPIC);
            add(shadowRequest.getShadowTopicPrefix() + SHADOW_DELETE_SUBSCRIPTION_TOPIC);
        }};

        Set<String> topicSet = getTopicSet(newShadowSet);
        topicSet.addAll(getTopicSet(SHADOW_SET));

        CloudDataClient cloudDataClient = new CloudDataClient(mockSyncHandler, mockMqttClient, executorService);
        cloudDataClient.updateSubscriptions(SHADOW_SET);
        TimeUnit.MILLISECONDS.sleep(2000);

        // update subscriptions again
        cloudDataClient.updateSubscriptions(newShadowSet);
        TimeUnit.MILLISECONDS.sleep(2000);

        verify(mockMqttClient, times(8)).subscribe(any(SubscribeRequest.class));
        verify(mockMqttClient, times(2)).unsubscribe(any(UnsubscribeRequest.class));

        // check values subscriptions
        for (int i = 0; i < subscribeRequestCaptor.getAllValues().size(); i++) {
            assertThat(topicSet, hasItem(subscribeRequestCaptor.getAllValues().get(i).getTopic()));
        }

        for (int i = 0; i < unsubscribeRequestCaptor.getAllValues().size(); i++) {
            assertThat(deletedShadowSet, hasItem(unsubscribeRequestCaptor.getAllValues().get(i).getTopic()));
        }
    }

    @Test
    void GIVEN_existing_shadows_and_update_with_no_shadows_WHEN_update_subscriptions_THEN_subscriptions_cleared(ExtensionContext context) throws InterruptedException, TimeoutException, ExecutionException {
        ignoreExceptionOfType(context, InterruptedException.class);
        CloudDataClient cloudDataClient = new CloudDataClient(mockSyncHandler, mockMqttClient, executorService);
        cloudDataClient.updateSubscriptions(SHADOW_SET);
        TimeUnit.MILLISECONDS.sleep(2000);

        cloudDataClient.updateSubscriptions(Collections.emptySet());
        TimeUnit.MILLISECONDS.sleep(2000);

        verify(mockMqttClient, times(6)).subscribe(any(SubscribeRequest.class));
        verify(mockMqttClient, times(6)).unsubscribe(any(UnsubscribeRequest.class));
    }

    @Test
    void GIVEN_no_shadows_and_update_with_no_shadows_WHEN_update_subscriptions_THEN_do_nothing(ExtensionContext context) throws InterruptedException, TimeoutException, ExecutionException {
        ignoreExceptionOfType(context, InterruptedException.class);
        CloudDataClient cloudDataClient = new CloudDataClient(mockSyncHandler, mockMqttClient, executorService);

        cloudDataClient.updateSubscriptions(Collections.emptySet());
        TimeUnit.MILLISECONDS.sleep(2000);

        verify(mockMqttClient, times(0)).subscribe(any(SubscribeRequest.class));
        verify(mockMqttClient, times(0)).unsubscribe(any(UnsubscribeRequest.class));
    }

    @Test
    void GIVEN_update_subscriptions_offline_WHEN_update_subscriptions_THEN_do_nothing(ExtensionContext context) throws InterruptedException, TimeoutException, ExecutionException {
        ignoreExceptionOfType(context, InterruptedException.class);
        CloudDataClient cloudDataClient = new CloudDataClient(mockSyncHandler, mockMqttClient, executorService);

        lenient().doReturn(false).when(mockMqttClient).connected();

        cloudDataClient.updateSubscriptions(Collections.emptySet());
        cloudDataClient.updateSubscriptions(SHADOW_SET);
        TimeUnit.MILLISECONDS.sleep(2000);

        verify(mockMqttClient, times(0)).subscribe(any(SubscribeRequest.class));
        verify(mockMqttClient, times(0)).unsubscribe(any(UnsubscribeRequest.class));
    }

    @Test
    void GIVEN_interrupt_called_during_subscribe_WHEN_update_subscriptions_THEN_exception_handled(ExtensionContext context) throws InterruptedException, TimeoutException, ExecutionException {
        ignoreExceptionOfType(context, InterruptedException.class);
        doThrow(InterruptedException.class).when(mockMqttClient).subscribe(any(SubscribeRequest.class));

        CloudDataClient cloudDataClient = new CloudDataClient(mockSyncHandler, mockMqttClient, executorService);
        assertDoesNotThrow(() -> cloudDataClient.updateSubscriptions(SHADOW_SET));
        TimeUnit.MILLISECONDS.sleep(2000);

        verify(mockMqttClient, times(1)).subscribe(any(SubscribeRequest.class));
        verify(mockMqttClient, times(0)).unsubscribe(any(UnsubscribeRequest.class));
    }

    @Test
    void GIVEN_interrupt_called_during_unsubscribe_WHEN_update_subscriptions_THEN_exception_handled(ExtensionContext context) throws InterruptedException, TimeoutException, ExecutionException {
        ignoreExceptionOfType(context, InterruptedException.class);
        doThrow(InterruptedException.class).when(mockMqttClient).unsubscribe(any(UnsubscribeRequest.class));

        CloudDataClient cloudDataClient = new CloudDataClient(mockSyncHandler, mockMqttClient, executorService);
        assertDoesNotThrow(() -> cloudDataClient.updateSubscriptions(SHADOW_SET));
        TimeUnit.MILLISECONDS.sleep(2000);

        assertDoesNotThrow(() -> cloudDataClient.updateSubscriptions(Collections.emptySet()));
        TimeUnit.MILLISECONDS.sleep(2000);

        verify(mockMqttClient, times(6)).subscribe(any(SubscribeRequest.class));
        verify(mockMqttClient, times(1)).unsubscribe(any(UnsubscribeRequest.class));
    }

    @ParameterizedTest
    @ValueSource(classes = {TimeoutException.class, ExecutionException.class})
    void GIVEN_poison_pill_subscription_WHEN_update_subscriptions_THEN_does_not_end_until_stop_called(Class clazz, ExtensionContext context) throws InterruptedException, TimeoutException, ExecutionException {
        ignoreExceptionOfType(context, InterruptedException.class);
        ignoreExceptionOfType(context, SubscriptionRetryException.class);
        ignoreExceptionOfType(context, clazz);
        doThrow(clazz).when(mockMqttClient).subscribe(any(SubscribeRequest.class));

        CloudDataClient cloudDataClient = new CloudDataClient(mockSyncHandler, mockMqttClient, executorService);
        cloudDataClient.updateSubscriptions(SHADOW_SET);
        TimeUnit.MILLISECONDS.sleep(5000);

        cloudDataClient.stopSubscribing();

        // expects that subscription call should be greater than 8 (expected without issues)
        verify(mockMqttClient, atLeast(9)).subscribe(any(SubscribeRequest.class));
        verify(mockMqttClient, times(0)).unsubscribe(any(UnsubscribeRequest.class));
    }

    @ParameterizedTest
    @ValueSource(classes = {TimeoutException.class, ExecutionException.class})
    void GIVEN_poison_pill_unsubscription_WHEN_update_subscriptions_THEN_does_not_end_until_stop_called(Class clazz, ExtensionContext context) throws InterruptedException, TimeoutException, ExecutionException {
        ignoreExceptionOfType(context, InterruptedException.class);
        ignoreExceptionOfType(context, SubscriptionRetryException.class);
        ignoreExceptionOfType(context, clazz);
        Set<String> topicSet = getTopicSet(SHADOW_SET);
        doThrow(clazz).when(mockMqttClient).unsubscribe(any(UnsubscribeRequest.class));

        CloudDataClient cloudDataClient = new CloudDataClient(mockSyncHandler, mockMqttClient, executorService);
        cloudDataClient.updateSubscriptions(SHADOW_SET);
        TimeUnit.MILLISECONDS.sleep(2000);

        cloudDataClient.updateSubscriptions(Collections.emptySet());
        TimeUnit.MILLISECONDS.sleep(5000);

        cloudDataClient.stopSubscribing();

        // expects that unsubscription call should be greater than 8 (expected without issues)
        verify(mockMqttClient, times(6)).subscribe(any(SubscribeRequest.class));
        verify(mockMqttClient, atLeast(9)).unsubscribe(any(UnsubscribeRequest.class));

        // check values subscriptions
        for (int i = 0; i < subscribeRequestCaptor.getAllValues().size(); i++) {
            assertThat(topicSet, hasItem(subscribeRequestCaptor.getAllValues().get(i).getTopic()));
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "$aws/things/MyThinge2e-1619675861291-941d61c9-c99c-43e1-bf31-411a58d1fc23/shadow/name/MyThingNamedShadowe2e-1619675861291-5d0fd60c-1ee6-4538-8876-825a/update/accepted",
            "$aws/things/MyThinge2e-1619675861291-941d61c9-c99c-43e1-bf31-411a58d1fc23/shadow/name/MyThingNamedShadowe2e-1619675861291-5d0fd60c-1ee6-4538-8876-825a/delete/accepted"})
    void GIVEN_good_shadow_topic_WHEN_extractShadowFromTopic_THEN_gets_correct_shadow_request(String topic) {
        CloudDataClient cloudDataClient = new CloudDataClient(mockSyncHandler, mockMqttClient, executorService);
        ShadowRequest request = cloudDataClient.extractShadowFromTopic(topic);
        assertThat(request.getThingName(), is("MyThinge2e-1619675861291-941d61c9-c99c-43e1-bf31-411a58d1fc23"));
        assertThat(request.getShadowName(), is("MyThingNamedShadowe2e-1619675861291-5d0fd60c-1ee6-4538-8876-825a"));
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "$aws/things/MyThinge2e-1619675861291-941d61c9-c99c-43e1-bf31-411a58d1fc23/shadow/update/accepted",
            "$aws/things/MyThinge2e-1619675861291-941d61c9-c99c-43e1-bf31-411a58d1fc23/shadow/delete/accepted"})
    void GIVEN_good_shadow_topic_for_classic_shadow_WHEN_extractShadowFromTopic_THEN_gets_correct_shadow_request(String topic) {
        CloudDataClient cloudDataClient = new CloudDataClient(mockSyncHandler, mockMqttClient, executorService);
        ShadowRequest request = cloudDataClient.extractShadowFromTopic(topic);
        assertThat(request.getThingName(), is("MyThinge2e-1619675861291-941d61c9-c99c-43e1-bf31-411a58d1fc23"));
        assertThat(request.getShadowName(), is(CLASSIC_SHADOW_IDENTIFIER));
    }

    @Test
    void GIVEN_100_synced_shadows_WHEN_unsubscribeForAllShadowsTopics_THEN_unsubscribes_to_200_shadow_topics()
            throws InterruptedException, ExecutionException, TimeoutException {
        CloudDataClient cloudDataClient = new CloudDataClient(mockSyncHandler, mockMqttClient, executorService);
        for (int i = 0; i < 100; i++) {
            cloudDataClient.getSubscribedUpdateShadowTopics().add("$aws/things/MyThing-" + i + "/shadow/update/accepted");
            cloudDataClient.getSubscribedDeleteShadowTopics().add("$aws/things/MyThing-" + i + "/shadow/delete/accepted");
        }

        cloudDataClient.unsubscribeForAllShadowsTopics();

        verify(mockMqttClient, times(200)).unsubscribe(any());
        assertThat(cloudDataClient.getSubscribedUpdateShadowTopics().size(), is(0));
        assertThat(cloudDataClient.getSubscribedDeleteShadowTopics().size(), is(0));
    }
}
