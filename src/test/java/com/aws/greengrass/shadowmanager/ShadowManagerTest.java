/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.shadowmanager;

import com.aws.greengrass.authorization.exceptions.AuthorizationException;
import com.aws.greengrass.dependency.State;
import com.aws.greengrass.lifecyclemanager.GreengrassService;
import com.aws.greengrass.lifecyclemanager.GlobalStateChangeListener;
import com.aws.greengrass.lifecyclemanager.Kernel;
import com.aws.greengrass.shadowmanager.model.LogEvents;
import com.aws.greengrass.testcommons.testutilities.GGExtension;
import com.aws.greengrass.testcommons.testutilities.GGServiceTestUtil;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.net.URL;
import java.nio.file.Path;
import java.sql.SQLException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static com.aws.greengrass.testcommons.testutilities.ExceptionLogProtector.ignoreExceptionOfType;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;

@ExtendWith({MockitoExtension.class, GGExtension.class})
public class ShadowManagerTest extends GGServiceTestUtil {
    private static final long TEST_TIME_OUT_SEC = 30L;
    private static final String DEFAULT_CONFIG = "config.yaml";

    private Kernel kernel;
    private GlobalStateChangeListener listener;

    @TempDir
    Path rootDir;

    @Mock
    AuthorizationHandlerWrapper mockAuthorizationHandlerWrapper;

    @Mock
    ShadowManagerDatabase mockShadowManagerDatabase;

    @Mock
    ShadowManagerDAOImpl mockShadowManagerDAOImpl;

    @BeforeEach
    void setup() {
        kernel = new Kernel();
    }

    @AfterEach
    void cleanup() {
        kernel.shutdown();
    }

    private void startNucleusWithConfig(String configFile, State expectedState) throws InterruptedException {
        CountDownLatch shadowManagerRunning = new CountDownLatch(1);
        URL resource = ShadowManagerTest.class.getResource("json_shadow_examples/good_initial_document.json");
        URL resource2 = ShadowManagerTest.class.getResource("config.yaml");
        kernel.parseArgs("-r", rootDir.toAbsolutePath().toString(), "-i",
                getClass().getResource(configFile).toString());
        listener = (GreengrassService service, State was, State newState) -> {
            if (service.getName().equals(ShadowManager.SERVICE_NAME) && service.getState().equals(expectedState)) {
                shadowManagerRunning.countDown();
            }
        };
        kernel.getContext().addGlobalStateChangeListener(listener);
        kernel.getContext().put(ShadowManagerDatabase.class, mockShadowManagerDatabase);
        kernel.getContext().put(ShadowManagerDAOImpl.class, mockShadowManagerDAOImpl);
        kernel.getContext().put(AuthorizationHandlerWrapper.class, mockAuthorizationHandlerWrapper);
        kernel.launch();

        assertTrue(shadowManagerRunning.await(TEST_TIME_OUT_SEC, TimeUnit.SECONDS));
    }

    @Test
    void GIVEN_Greengrass_with_shadow_manager_WHEN_start_nucleus_THEN_shadow_manager_starts_successfully() throws Exception {
        startNucleusWithConfig(DEFAULT_CONFIG, State.RUNNING);
    }

    @Test
    void GIVEN_Greengrass_with_shadow_manager_WHEN_database_install_fails_THEN_service_errors(ExtensionContext context) throws Exception {
        ignoreExceptionOfType(context, SQLException.class);

        doThrow(SQLException.class).when(mockShadowManagerDatabase).install();
        startNucleusWithConfig(DEFAULT_CONFIG, State.ERRORED);
    }

    @Test
    void GIVEN_Greengrass_with_shadow_manager_WHEN_nucleus_shutdown_THEN_shadow_manager_database_closes() throws Exception {
        startNucleusWithConfig(DEFAULT_CONFIG, State.RUNNING);
        kernel.shutdown();
        verify(mockShadowManagerDatabase, atLeastOnce()).close();
    }

    @Test
    void GIVEN_invalid_component_registration_WHEN_startup_THEN_shadow_manager_still_starts(ExtensionContext context) throws Exception {
        ignoreExceptionOfType(context, AuthorizationException.class);
        doThrow(new AuthorizationException("Test")).when(mockAuthorizationHandlerWrapper).registerComponent(any(), any());

        // Failing to register component does not break ShadowManager
        assertDoesNotThrow(() -> startNucleusWithConfig(DEFAULT_CONFIG, State.RUNNING));
    }

    @Test
    void GIVEN_shadow_manager_WHEN_log_event_occurs_THEN_code_returned() {
        for(LogEvents logEvent : LogEvents.values()) {
            assertFalse(logEvent.code().isEmpty());
        }
    }
}
