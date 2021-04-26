/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */


package com.aws.greengrass.shadowmanager.sync;

import com.aws.greengrass.mqttclient.MqttClient;
import com.aws.greengrass.mqttclient.SubscribeRequest;
import com.aws.greengrass.mqttclient.UnsubscribeRequest;
import com.aws.greengrass.testcommons.testutilities.GGExtension;
import com.aws.greengrass.util.Pair;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@ExtendWith({MockitoExtension.class, GGExtension.class})
public class CloudDataClientTest {

    private static final Set<Pair<String, String>> SHADOW_SET = new HashSet<Pair<String, String>>() {{
        add(new Pair<>("thing1", "shadow1"));
        add(new Pair<>("thing2", "shadow2"));
        add(new Pair<>("thing3", "shadow3"));
    }};
    
    @Mock
    MqttClient mockMqttClient;

    @Mock
    SyncHandler mockSyncHandler;

    @BeforeEach
    void setup() throws InterruptedException, ExecutionException, TimeoutException {
        lenient().doNothing().when(mockMqttClient).subscribe(any(SubscribeRequest.class));
        lenient().doNothing().when(mockMqttClient).unsubscribe(any(UnsubscribeRequest.class));
    }

    @Test
    void GIVEN_set_of_shadows_when_empty_shadows_WHEN_update_subscriptions_THEN_subscriptions_updated() throws InterruptedException, TimeoutException, ExecutionException {

        CloudDataClient cloudDataClient = new CloudDataClient(mockSyncHandler, mockMqttClient);
        cloudDataClient.updateSubscriptions(SHADOW_SET);
        Thread.sleep(2000);
        verify(mockMqttClient, times(6)).subscribe(any(SubscribeRequest.class));
        verify(mockMqttClient, times(0)).unsubscribe(any(UnsubscribeRequest.class));
    }

    @Test
    void GIVEN_existing_shadows_and_update_with_new_set_WHEN_update_subscriptions_THEN_subscriptions_updated() throws InterruptedException, TimeoutException, ExecutionException {

        final Set<Pair<String, String>> newShadowSet = new HashSet<Pair<String, String>>() {{
            add(new Pair<>("thing1", "shadow1"));
            add(new Pair<>("thing2", "shadow2"));
            add(new Pair<>("newThing5", "newShadow5"));
        }};

        CloudDataClient cloudDataClient = new CloudDataClient(mockSyncHandler, mockMqttClient);
        cloudDataClient.updateSubscriptions(SHADOW_SET);
        Thread.sleep(2000);

        // update subscriptions again
        cloudDataClient.updateSubscriptions(newShadowSet);
        Thread.sleep(2000);

        verify(mockMqttClient, times(8)).subscribe(any(SubscribeRequest.class));
        verify(mockMqttClient, times(2)).unsubscribe(any(UnsubscribeRequest.class));
    }

    @Test
    void GIVEN_existing_shadows_and_update_with_new_set_WHEN_clear_subscriptions_THEN_subscriptions_cleared() throws InterruptedException, TimeoutException, ExecutionException {
        CloudDataClient cloudDataClient = new CloudDataClient(mockSyncHandler, mockMqttClient);
        cloudDataClient.updateSubscriptions(SHADOW_SET);
        Thread.sleep(2000);

        cloudDataClient.clearSubscriptions();
        Thread.sleep(2000);

        verify(mockMqttClient, times(6)).subscribe(any(SubscribeRequest.class));
        verify(mockMqttClient, times(6)).unsubscribe(any(UnsubscribeRequest.class));
    }

}
