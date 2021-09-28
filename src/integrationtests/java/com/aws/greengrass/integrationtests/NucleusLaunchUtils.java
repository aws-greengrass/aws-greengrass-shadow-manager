/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.integrationtests;

import com.aws.greengrass.dependency.State;
import com.aws.greengrass.lifecyclemanager.GlobalStateChangeListener;
import com.aws.greengrass.lifecyclemanager.GreengrassService;
import com.aws.greengrass.lifecyclemanager.Kernel;
import com.aws.greengrass.mqttclient.MqttClient;
import com.aws.greengrass.shadowmanager.AuthorizationHandlerWrapper;
import com.aws.greengrass.shadowmanager.ShadowManager;
import com.aws.greengrass.shadowmanager.ShadowManagerDAOImpl;
import com.aws.greengrass.shadowmanager.ShadowManagerDatabase;
import com.aws.greengrass.shadowmanager.exception.RetryableException;
import com.aws.greengrass.shadowmanager.sync.IotDataPlaneClientFactory;
import com.aws.greengrass.shadowmanager.sync.SyncHandler;
import com.aws.greengrass.shadowmanager.sync.strategy.PeriodicSyncStrategy;
import com.aws.greengrass.shadowmanager.sync.strategy.RealTimeSyncStrategy;
import com.aws.greengrass.shadowmanager.sync.strategy.SyncStrategy;
import com.aws.greengrass.testcommons.testutilities.GGServiceTestUtil;
import com.aws.greengrass.util.RetryUtils;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Answers;
import org.mockito.Mock;

import java.nio.file.Path;
import java.time.Duration;
import java.util.Collections;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.lenient;

public class NucleusLaunchUtils extends GGServiceTestUtil {
    private static final long TEST_TIME_OUT_SEC = 30L;

    public Kernel kernel;
    public ShadowManager shadowManager;
    GlobalStateChangeListener listener;
    @TempDir
    Path rootDir;
    @Mock(answer = Answers.RETURNS_DEEP_STUBS)
    IotDataPlaneClientFactory iotDataPlaneClientFactory;
    @Mock
    MqttClient mqttClient;
    @Mock(answer = Answers.RETURNS_DEEP_STUBS)
    ShadowManagerDAOImpl dao;
    @Mock
    AuthorizationHandlerWrapper mockAuthorizationHandlerWrapper;
    @Mock
    ShadowManagerDatabase mockShadowManagerDatabase;

    @Deprecated()
    public void startNucleusWithConfig(String configFile) throws InterruptedException {
        startNucleusWithConfig(configFile, State.RUNNING, false, false, false);
    }

    @Deprecated()
    void startNucleusWithConfig(String configFile, boolean mockCloud, boolean mockDao) throws InterruptedException {
        startNucleusWithConfig(configFile, State.RUNNING, false, mockCloud, mockDao);
    }

    @Deprecated()
    void startNucleusWithConfig(String configFile, State expectedState, boolean mockDatabase) throws InterruptedException {
        startNucleusWithConfig(configFile, expectedState, mockDatabase, false, true);
    }

    void startNucleusWithConfig(NucleusLaunchUtilsConfig config) throws InterruptedException {
        CountDownLatch shadowManagerRunning = new CountDownLatch(1);
        AtomicBoolean isSyncMocked = new AtomicBoolean(false);
        kernel.parseArgs("-r", rootDir.toAbsolutePath().toString(), "-i",
                getClass().getResource(config.getConfigFile()).toString());
        listener = (GreengrassService service, State was, State newState) -> {
            if (service.getName().equals(ShadowManager.SERVICE_NAME) && service.getState().equals(config.getExpectedState())) {
                shadowManager = (ShadowManager) service;
                shadowManagerRunning.countDown();
            }
        };
        kernel.getContext().addGlobalStateChangeListener(listener);
        kernel.getContext().put(MqttClient.class, mqttClient);
        if (config.isMqttConnected()) {
            // assume we are always connected
            if (config.isResetRetryConfig()) {
                lenient().when(mqttClient.connected()).thenAnswer(invocation -> isSyncMocked.get());
            } else {
                lenient().when(mqttClient.connected()).thenAnswer(invocation -> true);
            }
        }


        if (config.isMockDatabase()) {
            kernel.getContext().put(ShadowManagerDatabase.class, mockShadowManagerDatabase);
            kernel.getContext().put(AuthorizationHandlerWrapper.class, mockAuthorizationHandlerWrapper);
        }
        if (config.isMockCloud()) {
            kernel.getContext().put(IotDataPlaneClientFactory.class, iotDataPlaneClientFactory);
        }
        if (config.isMockDao()) {
            kernel.getContext().put(ShadowManagerDAOImpl.class, dao);
        }
        SyncHandler syncHandler = kernel.getContext().get(SyncHandler.class);
        SyncStrategy realTimeSyncStrategy = syncHandler.getOverallSyncStrategy();
        kernel.getContext().put(RealTimeSyncStrategy.class, (RealTimeSyncStrategy) realTimeSyncStrategy);
        ExecutorService es = kernel.getContext().get(ExecutorService.class);
        ScheduledExecutorService ses = kernel.getContext().get(ScheduledExecutorService.class);
        // set retry config to only try once so we can test failures earlier
        kernel.launch();

        assertTrue(shadowManagerRunning.await(TEST_TIME_OUT_SEC, TimeUnit.SECONDS));

        if (config.isResetRetryConfig()) {
            realTimeSyncStrategy.stop();
            RetryUtils.RetryConfig retryConfig = RetryUtils.RetryConfig.builder()
                    .maxAttempt(1)
                    .maxRetryInterval(Duration.ofSeconds(1))
                    .retryableExceptions(Collections.singletonList(RetryableException.class))
                    .build();
            SyncStrategy syncStrategy;
            if (RealTimeSyncStrategy.class.equals(config.getSyncClazz())) {
                syncStrategy = new RealTimeSyncStrategy(es, ((RealTimeSyncStrategy) realTimeSyncStrategy).getRetryer(), retryConfig);
                kernel.getContext().put(RealTimeSyncStrategy.class, (RealTimeSyncStrategy) syncStrategy);
            } else {
                syncStrategy = new PeriodicSyncStrategy(ses, ((RealTimeSyncStrategy) realTimeSyncStrategy).getRetryer(), 3, retryConfig);
                kernel.getContext().put(PeriodicSyncStrategy.class, (PeriodicSyncStrategy) syncStrategy);

            }
            syncHandler.setOverallSyncStrategy(syncStrategy);
            isSyncMocked.set(true);
            shadowManager.startSyncingShadows(ShadowManager.StartSyncInfo.builder().build());
        }

    }

    @Deprecated()
    void startNucleusWithConfig(String configFile, State expectedState, boolean mockDatabase, boolean mockCloud,
                                boolean mockDao) throws InterruptedException {
        CountDownLatch shadowManagerRunning = new CountDownLatch(1);
        kernel.parseArgs("-r", rootDir.toAbsolutePath().toString(), "-i",
                getClass().getResource(configFile).toString());
        listener = (GreengrassService service, State was, State newState) -> {
            if (service.getName().equals(ShadowManager.SERVICE_NAME) && service.getState().equals(expectedState)) {
                shadowManagerRunning.countDown();
                shadowManager = (ShadowManager) service;
            }
        };
        kernel.getContext().addGlobalStateChangeListener(listener);
        kernel.getContext().put(MqttClient.class, mqttClient);
        // assume we are always connected
        lenient().when(mqttClient.connected()).thenReturn(true);

        if (mockDatabase) {
            kernel.getContext().put(ShadowManagerDatabase.class, mockShadowManagerDatabase);
            kernel.getContext().put(AuthorizationHandlerWrapper.class, mockAuthorizationHandlerWrapper);
        }
        if (mockCloud) {
            kernel.getContext().put(IotDataPlaneClientFactory.class, iotDataPlaneClientFactory);
        }
        if (mockDao) {
            kernel.getContext().put(ShadowManagerDAOImpl.class, dao);
        }
        // set retry config to only try once so we can test failures earlier
        kernel.launch();

        assertTrue(shadowManagerRunning.await(TEST_TIME_OUT_SEC, TimeUnit.SECONDS));
    }
}