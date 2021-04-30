/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.integrationtests;

import com.aws.greengrass.authorization.exceptions.AuthorizationException;
import com.aws.greengrass.dependency.State;
import com.aws.greengrass.lifecyclemanager.GlobalStateChangeListener;
import com.aws.greengrass.lifecyclemanager.GreengrassService;
import com.aws.greengrass.lifecyclemanager.Kernel;
import com.aws.greengrass.mqttclient.MqttClient;
import com.aws.greengrass.shadowmanager.AuthorizationHandlerWrapper;
import com.aws.greengrass.shadowmanager.ShadowManager;
import com.aws.greengrass.shadowmanager.ShadowManagerDAOImpl;
import com.aws.greengrass.shadowmanager.ShadowManagerDatabase;
import com.aws.greengrass.shadowmanager.exception.SkipSyncRequestException;
import com.aws.greengrass.shadowmanager.model.LogEvents;
import com.aws.greengrass.shadowmanager.model.dao.SyncInformation;
import com.aws.greengrass.testcommons.testutilities.GGExtension;
import com.aws.greengrass.testcommons.testutilities.GGServiceTestUtil;
import com.aws.greengrass.util.Pair;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.nio.file.Path;
import java.sql.SQLException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static com.aws.greengrass.componentmanager.KernelConfigResolver.CONFIGURATION_CONFIG_KEY;
import static com.aws.greengrass.shadowmanager.TestUtils.SAMPLE_EXCEPTION_MESSAGE;
import static com.aws.greengrass.shadowmanager.TestUtils.THING_NAME;
import static com.aws.greengrass.shadowmanager.model.Constants.CONFIGURATION_CLASSIC_SHADOW_TOPIC;
import static com.aws.greengrass.shadowmanager.model.Constants.CONFIGURATION_NAMED_SHADOWS_TOPIC;
import static com.aws.greengrass.shadowmanager.model.Constants.CONFIGURATION_SHADOW_DOCUMENTS_TOPIC;
import static com.aws.greengrass.shadowmanager.model.Constants.CONFIGURATION_SYNCHRONIZATION_TOPIC;
import static com.aws.greengrass.shadowmanager.model.Constants.CONFIGURATION_THING_NAME_TOPIC;
import static com.aws.greengrass.testcommons.testutilities.ExceptionLogProtector.ignoreExceptionOfType;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

@ExtendWith({MockitoExtension.class, GGExtension.class})
class ShadowManagerTest extends GGServiceTestUtil {
    private static final long TEST_TIME_OUT_SEC = 30L;
    private static final String DEFAULT_CONFIG = "config.yaml";
    private static final byte[] BASE_DOCUMENT = "{\"version\": 1, \"state\": {\"reported\": {\"name\": \"The Beatles\"}}}".getBytes();
    public static final String THING_NAME2 = "testThingName2";

    private Kernel kernel;
    private ShadowManager shadowManager;
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

    private void startNucleusWithConfig(String configFile, State expectedState, boolean mockDatabase) throws InterruptedException {
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
        if (mockDatabase) {
            kernel.getContext().put(ShadowManagerDatabase.class, mockShadowManagerDatabase);
            kernel.getContext().put(ShadowManagerDAOImpl.class, mockShadowManagerDAOImpl);
            kernel.getContext().put(AuthorizationHandlerWrapper.class, mockAuthorizationHandlerWrapper);
        }
        kernel.launch();

        assertTrue(shadowManagerRunning.await(TEST_TIME_OUT_SEC, TimeUnit.SECONDS));
    }

    @Test
    void GIVEN_Greengrass_with_shadow_manager_WHEN_start_nucleus_THEN_shadow_manager_starts_successfully() throws Exception {
        startNucleusWithConfig(DEFAULT_CONFIG, State.RUNNING, false);
    }

    @Test
    void GIVEN_Greengrass_with_shadow_manager_WHEN_database_install_fails_THEN_service_errors(ExtensionContext context) throws Exception {
        ignoreExceptionOfType(context, SQLException.class);

        doThrow(SQLException.class).when(mockShadowManagerDatabase).install();
        startNucleusWithConfig(DEFAULT_CONFIG, State.ERRORED, true);
    }

    @Test
    void GIVEN_Greengrass_with_shadow_manager_WHEN_nucleus_shutdown_THEN_shadow_manager_database_closes() throws Exception {
        startNucleusWithConfig(DEFAULT_CONFIG, State.RUNNING, true);
        kernel.shutdown();
        verify(mockShadowManagerDatabase, atLeastOnce()).close();
    }

    @Test
    void GIVEN_invalid_component_registration_WHEN_startup_THEN_shadow_manager_still_starts(ExtensionContext context) throws Exception {
        ignoreExceptionOfType(context, AuthorizationException.class);
        doThrow(new AuthorizationException(SAMPLE_EXCEPTION_MESSAGE)).when(mockAuthorizationHandlerWrapper).registerComponent(any(), any());

        // Failing to register component does not break ShadowManager
        assertDoesNotThrow(() -> startNucleusWithConfig(DEFAULT_CONFIG, State.RUNNING, true));
    }

    @Test
    void GIVEN_shadow_manager_WHEN_log_event_occurs_THEN_code_returned() {
        for(LogEvents logEvent : LogEvents.values()) {
            assertFalse(logEvent.code().isEmpty());
        }
    }

    private void createThingShadowSyncInfo(ShadowManagerDAOImpl impl, String thingName) {
        long epochMinus60Seconds = Instant.now().minusSeconds(60).getEpochSecond();
        for (int i = 0; i < 5; i++) {
            SyncInformation syncInformation = SyncInformation.builder()
                    .thingName(thingName)
                    .shadowName("Shadow-" + i)
                    .cloudDeleted(false)
                    .cloudVersion(1)
                    .cloudUpdateTime(epochMinus60Seconds)
                    .lastSyncedDocument(BASE_DOCUMENT)
                    .build();
            assertTrue(impl.updateSyncInformation(syncInformation));
        }
    }

    @Test
    @SuppressWarnings("PMD.CloseResource")
    void GIVEN_existing_sync_information_WHEN_config_updates_THEN_removed_sync_information_for_removed_shadows(ExtensionContext context) throws Exception {
        ignoreExceptionOfType(context, SkipSyncRequestException.class);
        MqttClient mqttClient = mock(MqttClient.class);
        lenient().when(mqttClient.connected()).thenReturn(false);

        kernel.getContext().put(MqttClient.class, mqttClient);
        startNucleusWithConfig(DEFAULT_CONFIG, State.RUNNING, false);
        ShadowManagerDAOImpl impl = kernel.getContext().get(ShadowManagerDAOImpl.class);
        createThingShadowSyncInfo(impl, THING_NAME);
        createThingShadowSyncInfo(impl, THING_NAME2);

        List<Pair<String, String>> allSyncedShadowNames = impl.listSyncedShadows();
        assertThat(allSyncedShadowNames, containsInAnyOrder(
                new Pair<>(THING_NAME, "Shadow-0"),
                new Pair<>(THING_NAME, "Shadow-1"),
                new Pair<>(THING_NAME, "Shadow-2"),
                new Pair<>(THING_NAME, "Shadow-3"),
                new Pair<>(THING_NAME, "Shadow-4"),
                new Pair<>(THING_NAME2, "Shadow-0"),
                new Pair<>(THING_NAME2, "Shadow-1"),
                new Pair<>(THING_NAME2, "Shadow-2"),
                new Pair<>(THING_NAME2, "Shadow-3"),
                new Pair<>(THING_NAME2, "Shadow-4")));


        List<Map<String, Object>> shadowDocumentsList = new ArrayList<>();
        Map<String, Object> thingAMap = new HashMap<>();
        thingAMap.put(CONFIGURATION_THING_NAME_TOPIC, THING_NAME);
        thingAMap.put(CONFIGURATION_CLASSIC_SHADOW_TOPIC, false);
        thingAMap.put(CONFIGURATION_NAMED_SHADOWS_TOPIC, Arrays.asList("Shadow-0", "Shadow-1"));
        Map<String, Object> thingBMap = new HashMap<>();
        thingBMap.put(CONFIGURATION_THING_NAME_TOPIC, THING_NAME2);
        thingBMap.put(CONFIGURATION_NAMED_SHADOWS_TOPIC, Arrays.asList("Shadow-0", "Shadow-5"));
        shadowDocumentsList.add(thingAMap);
        shadowDocumentsList.add(thingBMap);

        shadowManager.getConfig().lookupTopics(CONFIGURATION_CONFIG_KEY).lookupTopics(CONFIGURATION_SYNCHRONIZATION_TOPIC)
                .replaceAndWait(Collections.singletonMap(CONFIGURATION_SHADOW_DOCUMENTS_TOPIC, shadowDocumentsList));
        //TimeUnit.SECONDS.sleep(20);

        allSyncedShadowNames = impl.listSyncedShadows();
        assertThat(allSyncedShadowNames, containsInAnyOrder(
                new Pair<>(THING_NAME, "Shadow-0"),
                new Pair<>(THING_NAME, "Shadow-1"),
                new Pair<>(THING_NAME2, ""),
                new Pair<>(THING_NAME2, "Shadow-0"),
                new Pair<>(THING_NAME2, "Shadow-5")));
    }
}
