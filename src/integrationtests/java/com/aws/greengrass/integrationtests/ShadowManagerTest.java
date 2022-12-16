/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.integrationtests;

import com.aws.greengrass.authorization.exceptions.AuthorizationException;
import com.aws.greengrass.dependency.State;
import com.aws.greengrass.lifecyclemanager.Kernel;
import com.aws.greengrass.mqttclient.MqttClient;
import com.aws.greengrass.security.SecurityService;
import com.aws.greengrass.shadowmanager.ShadowManagerDAOImpl;
import com.aws.greengrass.shadowmanager.exception.IoTDataPlaneClientCreationException;
import com.aws.greengrass.shadowmanager.exception.RetryableException;
import com.aws.greengrass.shadowmanager.exception.SkipSyncRequestException;
import com.aws.greengrass.shadowmanager.model.LogEvents;
import com.aws.greengrass.shadowmanager.model.dao.SyncInformation;
import com.aws.greengrass.shadowmanager.sync.IotDataPlaneClientFactory;
import com.aws.greengrass.shadowmanager.sync.IotDataPlaneClientWrapper;
import com.aws.greengrass.shadowmanager.sync.SyncHandler;
import com.aws.greengrass.shadowmanager.sync.model.Direction;
import com.aws.greengrass.shadowmanager.sync.strategy.BaseSyncStrategy;
import com.aws.greengrass.shadowmanager.sync.strategy.PeriodicSyncStrategy;
import com.aws.greengrass.shadowmanager.sync.strategy.RealTimeSyncStrategy;
import com.aws.greengrass.testcommons.testutilities.GGExtension;
import com.aws.greengrass.util.Pair;
import com.aws.greengrass.util.exceptions.TLSAuthException;
import org.flywaydb.core.api.FlywayException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.mockito.junit.jupiter.MockitoExtension;
import software.amazon.awssdk.services.iotdataplane.model.GetThingShadowResponse;
import software.amazon.awssdk.services.iotdataplane.model.ResourceNotFoundException;

import javax.net.ssl.KeyManager;

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
import static com.aws.greengrass.shadowmanager.model.Constants.CONFIGURATION_STRATEGY_TOPIC;
import static com.aws.greengrass.shadowmanager.model.Constants.CONFIGURATION_SYNCHRONIZATION_TOPIC;
import static com.aws.greengrass.shadowmanager.model.Constants.CONFIGURATION_SYNC_DIRECTION_TOPIC;
import static com.aws.greengrass.shadowmanager.model.Constants.CONFIGURATION_THING_NAME_TOPIC;

import static com.aws.greengrass.testcommons.testutilities.ExceptionLogProtector.ignoreExceptionOfType;
import static com.github.grantwest.eventually.EventuallyLambdaMatcher.eventuallyEval;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith({MockitoExtension.class, GGExtension.class})
class ShadowManagerTest extends NucleusLaunchUtils {
    private static final String DEFAULT_CONFIG = "config.yaml";
    private static final byte[] BASE_DOCUMENT = "{\"version\": 1, \"state\": {\"reported\": {\"name\": \"The Beatles\"}}}".getBytes();
    public static final String THING_NAME2 = "testThingName2";

    @BeforeEach
    void setup() {
        // Set this property for kernel to scan its own classpath to find plugins
        System.setProperty("aws.greengrass.scanSelfClasspath", "true");
        kernel = new Kernel();
    }

    @AfterEach
    void cleanup() {
        kernel.shutdown();
    }

    @Test
    void GIVEN_Greengrass_with_shadow_manager_WHEN_start_nucleus_THEN_shadow_manager_starts_successfully() throws Exception {
        startNucleusWithConfig(DEFAULT_CONFIG, State.RUNNING, false);
    }

    @Test
    void GIVEN_Greengrass_with_shadow_manager_WHEN_database_install_fails_THEN_service_errors(ExtensionContext context) throws Exception {
        ignoreExceptionOfType(context, FlywayException.class);

        doThrow(FlywayException.class).when(mockShadowManagerDatabase).install();
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
        startNucleusWithConfig(NucleusLaunchUtilsConfig.builder()
                .configFile(DEFAULT_CONFIG)
                .mqttConnected(false)
                .build());
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

        allSyncedShadowNames = impl.listSyncedShadows();
        assertThat(allSyncedShadowNames, containsInAnyOrder(
                new Pair<>(THING_NAME, "Shadow-0"),
                new Pair<>(THING_NAME, "Shadow-1"),
                new Pair<>(THING_NAME2, ""),
                new Pair<>(THING_NAME2, "Shadow-0"),
                new Pair<>(THING_NAME2, "Shadow-5")));
    }


    @Test
    @SuppressWarnings("PMD.CloseResource")
    void GIVEN_shadow_manager_WHEN_individual_config_resets_THEN_respond_to_config_updates(ExtensionContext context)
            throws Exception {
        ignoreExceptionOfType(context, SkipSyncRequestException.class);
        MqttClient mqttClient = mock(MqttClient.class);
        lenient().when(mqttClient.connected()).thenReturn(false);


        kernel.getContext().put(MqttClient.class, mqttClient);
        startNucleusWithConfig(NucleusLaunchUtilsConfig.builder()
                .configFile(DEFAULT_CONFIG)
                .mqttConnected(false)
                .build());
        SyncHandler syncHandler = kernel.getContext().get(SyncHandler.class);
        shadowManager.getConfig().lookupTopics(CONFIGURATION_CONFIG_KEY, CONFIGURATION_SYNCHRONIZATION_TOPIC)
                .lookup(CONFIGURATION_SYNC_DIRECTION_TOPIC).withValue(Direction.DEVICE_TO_CLOUD.getCode());
        kernel.getContext().waitForPublishQueueToClear();
        assertThat(syncHandler.getSyncDirection(), is(Direction.DEVICE_TO_CLOUD));

        shadowManager.getConfig().lookupTopics(CONFIGURATION_CONFIG_KEY, CONFIGURATION_SYNCHRONIZATION_TOPIC)
                .lookup(CONFIGURATION_SYNC_DIRECTION_TOPIC).remove();
        kernel.getContext().waitForPublishQueueToClear();
        assertThat(syncHandler.getSyncDirection(), is(Direction.BETWEEN_DEVICE_AND_CLOUD));

        shadowManager.getConfig().lookupTopics(CONFIGURATION_CONFIG_KEY, CONFIGURATION_SYNCHRONIZATION_TOPIC)
                .lookup(CONFIGURATION_SYNC_DIRECTION_TOPIC).withValue(Direction.DEVICE_TO_CLOUD.getCode());
        kernel.getContext().waitForPublishQueueToClear();

        assertThat(syncHandler.getSyncDirection(), is(Direction.DEVICE_TO_CLOUD));
    }

    @Test
    @SuppressWarnings("PMD.CloseResource")
    void GIVEN_shadow_manager_WHEN_strategy_config_resets_THEN_respond_to_config_updates(ExtensionContext context)
            throws Exception {
        ignoreExceptionOfType(context, SkipSyncRequestException.class);
        MqttClient mqttClient = mock(MqttClient.class);
        lenient().when(mqttClient.connected()).thenReturn(false);


        kernel.getContext().put(MqttClient.class, mqttClient);
        startNucleusWithConfig(NucleusLaunchUtilsConfig.builder()
                .configFile("periodic_sync.yaml")
                .mqttConnected(false)
                .syncClazz(PeriodicSyncStrategy.class)
                .build());

        SyncHandler syncHandler = kernel.getContext().get(SyncHandler.class);
        assertThat(syncHandler.getOverallSyncStrategy(), instanceOf(PeriodicSyncStrategy.class));

        shadowManager.getConfig().lookupTopics(CONFIGURATION_CONFIG_KEY, CONFIGURATION_STRATEGY_TOPIC).remove();
        kernel.getContext().waitForPublishQueueToClear();
        assertThat(syncHandler.getOverallSyncStrategy(), instanceOf(RealTimeSyncStrategy.class));

        Map<String, Object> periodicStrategy = new HashMap<>();
        periodicStrategy.put("delay", "30");
        periodicStrategy.put("type","periodic");
        shadowManager.getConfig().lookupTopics(CONFIGURATION_CONFIG_KEY, CONFIGURATION_STRATEGY_TOPIC).replaceAndWait(periodicStrategy);
        kernel.getContext().waitForPublishQueueToClear();
        assertThat(syncHandler.getOverallSyncStrategy(), instanceOf(PeriodicSyncStrategy.class));
    }

    @Test
    void GIVEN_cryptoKeyProviderService_WHEN_get_client_THEN_wait_for_cryptoservice(ExtensionContext context) throws InterruptedException, TLSAuthException, IoTDataPlaneClientCreationException {
        ignoreExceptionOfType(context, TLSAuthException.class);
        ignoreExceptionOfType(context, RetryableException.class);
        ignoreExceptionOfType(context, ResourceNotFoundException.class);

        IotDataPlaneClientFactory factory = spy(kernel.getContext().get(IotDataPlaneClientFactory.class));
        kernel.getContext().put(IotDataPlaneClientFactory.class, factory);
        IotDataPlaneClientWrapper wrapper = spy(new FakeIotDataPlaneClientWrapper(factory));
        kernel.getContext().put(IotDataPlaneClientWrapper.class, wrapper);

        startNucleusWithConfig(NucleusLaunchUtilsConfig.builder()
                .configFile("sync.yaml")
                .mqttConnected(true)
                .mockCloud(false)
                .build());
        BaseSyncStrategy syncStragy = kernel.getContext().get(RealTimeSyncStrategy.class);
        assertThat("syncing has started", s::isSyncing, eventuallyEval(is(true)));
        verify(wrapper, timeout(5000).atLeast(1)).getThingShadow("Thing1", "");

        CountDownLatch cdl = new CountDownLatch(1);
        SecurityService ss= mock(SecurityService.class);
        when(ss.getDeviceIdentityKeyManagers()).thenAnswer((invocation)->{
            cdl.countDown();
            return new KeyManager[0];
        });
        kernel.getContext().put(SecurityService.class, ss);
        assertThat("request is retried with a new client",cdl.await(10, TimeUnit.SECONDS), is(true));
        verify(wrapper, timeout(5000).atLeast(2)).getThingShadow("Thing1", "");
    }

    static class FakeIotDataPlaneClientWrapper extends  IotDataPlaneClientWrapper{
        IotDataPlaneClientFactory factory;

        public FakeIotDataPlaneClientWrapper(IotDataPlaneClientFactory iotDataPlaneClientFactory) {
            super(iotDataPlaneClientFactory);
            factory = iotDataPlaneClientFactory;
        }

        @Override
        public GetThingShadowResponse getThingShadow(String thingName, String shadowName) throws IoTDataPlaneClientCreationException {
            factory.getIotDataPlaneClient();
            throw ResourceNotFoundException.builder().build();
        }
    }

}
