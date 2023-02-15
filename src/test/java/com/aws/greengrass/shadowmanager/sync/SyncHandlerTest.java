/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.shadowmanager.sync;


import com.aws.greengrass.shadowmanager.model.configuration.ThingShadowSyncConfiguration;
import com.aws.greengrass.shadowmanager.sync.model.BaseSyncRequest;
import com.aws.greengrass.shadowmanager.sync.model.CloudDeleteSyncRequest;
import com.aws.greengrass.shadowmanager.sync.model.CloudUpdateSyncRequest;
import com.aws.greengrass.shadowmanager.sync.model.DirectionWrapper;
import com.aws.greengrass.shadowmanager.sync.model.LocalDeleteSyncRequest;
import com.aws.greengrass.shadowmanager.sync.model.LocalUpdateSyncRequest;
import com.aws.greengrass.shadowmanager.sync.model.SyncContext;
import com.aws.greengrass.shadowmanager.sync.strategy.RealTimeSyncStrategy;
import com.aws.greengrass.shadowmanager.sync.strategy.SyncStrategyFactory;
import com.aws.greengrass.shadowmanager.sync.strategy.model.Strategy;
import com.aws.greengrass.testcommons.testutilities.GGExtension;
import com.aws.greengrass.util.Pair;
import com.fasterxml.jackson.databind.JsonNode;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Answers;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;


@ExtendWith({MockitoExtension.class, GGExtension.class})
class SyncHandlerTest {

    @Mock
    RealTimeSyncStrategy mockSyncStrategy;

    @Mock
    ExecutorService executorService;

    @Mock
    ScheduledExecutorService scheduledExecutorService;

    @Mock
    SyncStrategyFactory mockSyncStrategyFactory;

    DirectionWrapper direction = new DirectionWrapper();

    @Mock(answer = Answers.RETURNS_DEEP_STUBS)
    SyncContext context;

    @Captor
    ArgumentCaptor<BaseSyncRequest> syncRequestCaptor;

    SyncHandler syncHandler;

    @BeforeEach
    void setup() {
        syncHandler = new SyncHandler(executorService, scheduledExecutorService, mock(RequestBlockingQueue.class), direction);
        syncHandler.setOverallSyncStrategy(mockSyncStrategy);
    }

    @Test
    void GIVEN_not_started_WHEN_start_THEN_full_sync() throws InterruptedException {
        // GIVEN
        int numThreads = 3;

        List<Pair<String, String>> shadows = Arrays.asList(new Pair<>("a", "1"), new Pair<>("b", "2"));
        when(context.getDao().listSyncedShadows()).thenReturn(shadows);
        when(mockSyncStrategy.getRemainingCapacity()).thenReturn(1024);

        // WHEN
        syncHandler.start(context, numThreads);

        // THEN
        verify(mockSyncStrategy, times(1)).start(eq(context), eq(numThreads));
        verify(mockSyncStrategy, times(1)).clearSyncQueue();
        verify(mockSyncStrategy, times(shadows.size())).putSyncRequest(any());
    }

    @Test
    void GIVEN_started_WHEN_stop_THEN_stop_threads() {
        // GIVEN
        int numThreads = 1;

        List<Pair<String, String>> shadows = Arrays.asList(new Pair<>("a", "1"), new Pair<>("b", "2"));
        when(context.getDao().listSyncedShadows()).thenReturn(shadows);

        syncHandler.start(context, numThreads);

        // WHEN
        syncHandler.stop();

        // THEN
        verify(mockSyncStrategy, times(1)).stop();
    }

    @Test
    void GIVEN_sync_strategy_WHEN_setSyncStrategy_THEN_calls_sync_factory() {
        // GIVEN
        syncHandler = new SyncHandler(mockSyncStrategyFactory, mock(RequestBlockingQueue.class), direction);

        // WHEN
        syncHandler.setSyncStrategy(mock(Strategy.class));

        // THEN
        verify(mockSyncStrategyFactory, times(2)).createSyncStrategy(any(), any());
    }

    @Test
    void GIVEN_synced_shadows_WHEN_pushCloudUpdateSyncRequest_THEN_calls_overall_sync_strategy_put() throws InterruptedException {
        // GIVEN
        List<Pair<String, String>> shadows = Arrays.asList(new Pair<>("a", "1"), new Pair<>("b", "2"));
        when(context.getDao().listSyncedShadows()).thenReturn(shadows);

        doNothing().when(mockSyncStrategy).putSyncRequest(syncRequestCaptor.capture());
        Set<ThingShadowSyncConfiguration> syncConfigurations = new HashSet<>();
        syncConfigurations.add(ThingShadowSyncConfiguration.builder().thingName("a").shadowName("1").build());
        syncHandler.setSyncConfigurations(syncConfigurations);

        // WHEN
        syncHandler.pushCloudUpdateSyncRequest("a", "1", mock(JsonNode.class));

        // THEN
        verify(mockSyncStrategy, times(1)).putSyncRequest(any());
        assertThat(syncRequestCaptor.getValue(), is(instanceOf(CloudUpdateSyncRequest.class)));
    }

    @Test
    void GIVEN_synced_shadows_WHEN_pushLocalUpdateSyncRequest_THEN_calls_overall_sync_strategy_put() throws InterruptedException {
        // GIVEN
        List<Pair<String, String>> shadows = Arrays.asList(new Pair<>("a", "1"), new Pair<>("b", "2"));
        when(context.getDao().listSyncedShadows()).thenReturn(shadows);

        doNothing().when(mockSyncStrategy).putSyncRequest(syncRequestCaptor.capture());
        Set<ThingShadowSyncConfiguration> syncConfigurations = new HashSet<>();
        syncConfigurations.add(ThingShadowSyncConfiguration.builder().thingName("a").shadowName("1").build());
        syncHandler.setSyncConfigurations(syncConfigurations);

        // WHEN
        syncHandler.pushLocalUpdateSyncRequest("a", "1", new byte[0]);

        // THEN
        verify(mockSyncStrategy, times(1)).putSyncRequest(any());
        assertThat(syncRequestCaptor.getValue(), is(instanceOf(LocalUpdateSyncRequest.class)));
    }

    @Test
    void GIVEN_synced_shadows_WHEN_pushCloudDeleteSyncRequest_THEN_calls_overall_sync_strategy_put() throws InterruptedException {
        // GIVEN
        List<Pair<String, String>> shadows = Arrays.asList(new Pair<>("a", "1"), new Pair<>("b", "2"));
        when(context.getDao().listSyncedShadows()).thenReturn(shadows);

        doNothing().when(mockSyncStrategy).putSyncRequest(syncRequestCaptor.capture());
        Set<ThingShadowSyncConfiguration> syncConfigurations = new HashSet<>();
        syncConfigurations.add(ThingShadowSyncConfiguration.builder().thingName("a").shadowName("1").build());
        syncHandler.setSyncConfigurations(syncConfigurations);

        // WHEN
        syncHandler.pushCloudDeleteSyncRequest("a", "1");

        // THEN
        verify(mockSyncStrategy, times(1)).putSyncRequest(any());
        assertThat(syncRequestCaptor.getValue(), is(instanceOf(CloudDeleteSyncRequest.class)));
    }

    @Test
    void GIVEN_synced_shadows_WHEN_pushLocalDeleteSyncRequest_THEN_calls_overall_sync_strategy_put() throws InterruptedException {
        // GIVEN
        List<Pair<String, String>> shadows = Arrays.asList(new Pair<>("a", "1"), new Pair<>("b", "2"));
        when(context.getDao().listSyncedShadows()).thenReturn(shadows);

        doNothing().when(mockSyncStrategy).putSyncRequest(syncRequestCaptor.capture());
        Set<ThingShadowSyncConfiguration> syncConfigurations = new HashSet<>();
        syncConfigurations.add(ThingShadowSyncConfiguration.builder().thingName("a").shadowName("1").build());
        syncHandler.setSyncConfigurations(syncConfigurations);

        // WHEN
        syncHandler.pushLocalDeleteSyncRequest("a", "1", new byte[0]);

        // THEN
        verify(mockSyncStrategy, times(1)).putSyncRequest(any());
        assertThat(syncRequestCaptor.getValue(), is(instanceOf(LocalDeleteSyncRequest.class)));
    }
}
