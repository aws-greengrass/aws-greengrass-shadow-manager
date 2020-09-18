/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.shadowmanager;

import com.aws.greengrass.dependency.State;
import com.aws.greengrass.lifecyclemanager.GreengrassService;
import com.aws.greengrass.lifecyclemanager.GlobalStateChangeListener;
import com.aws.greengrass.lifecyclemanager.Kernel;
import com.aws.greengrass.testcommons.testutilities.GGExtension;
import com.aws.greengrass.testcommons.testutilities.GGServiceTestUtil;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.junit.jupiter.MockitoExtension;

import java.nio.file.Path;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

@ExtendWith({MockitoExtension.class, GGExtension.class})
public class ShadowManagerTest extends GGServiceTestUtil {
    private static final long TEST_TIME_OUT_SEC = 30L;

    private Kernel kernel;
    private GlobalStateChangeListener listener;

    @TempDir
    Path rootDir;

    @BeforeEach
    void setup() {
        kernel = new Kernel();
    }

    @AfterEach
    void cleanup() {
        kernel.shutdown();
    }

    private void startKernelWithConfig(String configFileName) throws InterruptedException {
        CountDownLatch shadowManagerRunning = new CountDownLatch(1);
        kernel.parseArgs("-r", rootDir.toAbsolutePath().toString(), "-i",
                getClass().getResource(configFileName).toString());
        listener = (GreengrassService service, State was, State newState) -> {
            if (service.getName().equals(ShadowManager.SERVICE_NAME) && service.getState().equals(State.RUNNING)) {
                shadowManagerRunning.countDown();
            }
        };
        kernel.getContext().addGlobalStateChangeListener(listener);
        kernel.launch();

        Assertions.assertTrue(shadowManagerRunning.await(TEST_TIME_OUT_SEC, TimeUnit.SECONDS));
    }

    @Test
    void GIVEN_Evergreen_with_shadow_manager_WHEN_start_kernel_THEN_shadow_manager_starts_successfully() throws Exception {
        startKernelWithConfig("config.yaml");
    }

}
