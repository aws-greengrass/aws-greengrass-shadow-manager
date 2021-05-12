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
import com.aws.greengrass.shadowmanager.sync.IotDataPlaneClientFactory;
import com.aws.greengrass.testcommons.testutilities.GGServiceTestUtil;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Answers;
import org.mockito.Mock;

import java.nio.file.Path;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

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

    public void startNucleusWithConfig(String configFile) throws InterruptedException {
        startNucleusWithConfig(configFile, State.RUNNING, false, false, false);
    }

    void startNucleusWithConfig(String configFile, boolean mockCloud, boolean mockDao) throws InterruptedException {
        startNucleusWithConfig(configFile, State.RUNNING, false, mockCloud, mockDao);
    }

    void startNucleusWithConfig(String configFile, State expectedState, boolean mockDatabase) throws InterruptedException {
        startNucleusWithConfig(configFile, expectedState, mockDatabase, false, true);
    }

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
        kernel.launch();

        assertTrue(shadowManagerRunning.await(TEST_TIME_OUT_SEC, TimeUnit.SECONDS));
    }
}
