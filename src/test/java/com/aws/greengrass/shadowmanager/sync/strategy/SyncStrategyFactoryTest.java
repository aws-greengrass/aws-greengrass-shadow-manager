/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.shadowmanager.sync.strategy;

import com.aws.greengrass.shadowmanager.sync.Retryer;
import com.aws.greengrass.shadowmanager.sync.strategy.model.Strategy;
import com.aws.greengrass.shadowmanager.sync.strategy.model.StrategyType;
import com.aws.greengrass.testcommons.testutilities.GGExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.concurrent.ExecutorService;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;

@ExtendWith({MockitoExtension.class, GGExtension.class})
class SyncStrategyFactoryTest {

    @Mock
    Retryer mockRetryer;

    @Mock
    ExecutorService mockSyncExecutorService;

    @Test
    void GIVEN_periodic_sync_strategy_WHEN_getSyncStrategy_THEN_gets_the_correct_sync_strategy_type() {
        SyncStrategyFactory factory = new SyncStrategyFactory(mockRetryer, mockSyncExecutorService);
        SyncStrategy syncStrategy = factory.getSyncStrategy(Strategy.builder().type(StrategyType.PERIODIC).delay(10L).build());
        assertThat(syncStrategy, is(instanceOf(PeriodicSyncStrategy.class)));
    }

    @Test
    void GIVEN_realTime_sync_strategy_WHEN_getSyncStrategy_THEN_gets_the_correct_sync_strategy_type() {
        SyncStrategyFactory factory = new SyncStrategyFactory(mockRetryer, mockSyncExecutorService);
        SyncStrategy syncStrategy = factory.getSyncStrategy(Strategy.builder().type(StrategyType.REALTIME).build());
        assertThat(syncStrategy, is(instanceOf(RealTimeSyncStrategy.class)));
    }
}
