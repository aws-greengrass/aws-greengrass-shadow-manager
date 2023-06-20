/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.shadowmanager;

import com.aws.greengrass.authorization.exceptions.AuthorizationException;
import com.aws.greengrass.config.Topic;
import com.aws.greengrass.config.Topics;
import com.aws.greengrass.config.UnsupportedInputTypeException;
import com.aws.greengrass.dependency.Context;
import com.aws.greengrass.dependency.State;
import com.aws.greengrass.deployment.DeviceConfiguration;
import com.aws.greengrass.mqttclient.CallbackEventManager;
import com.aws.greengrass.mqttclient.MqttClient;
import com.aws.greengrass.shadowmanager.exception.InvalidConfigurationException;
import com.aws.greengrass.shadowmanager.exception.InvalidRequestParametersException;
import com.aws.greengrass.shadowmanager.ipc.InboundRateLimiter;
import com.aws.greengrass.shadowmanager.ipc.PubSubClientWrapper;
import com.aws.greengrass.shadowmanager.model.configuration.ShadowSyncConfiguration;
import com.aws.greengrass.shadowmanager.model.configuration.ThingShadowSyncConfiguration;
import com.aws.greengrass.shadowmanager.model.dao.SyncInformation;
import com.aws.greengrass.shadowmanager.sync.CloudDataClient;
import com.aws.greengrass.shadowmanager.sync.IotDataPlaneClientWrapper;
import com.aws.greengrass.shadowmanager.sync.SyncConfigurationUpdater;
import com.aws.greengrass.shadowmanager.sync.SyncHandler;
import com.aws.greengrass.shadowmanager.sync.model.Direction;
import com.aws.greengrass.shadowmanager.sync.model.DirectionWrapper;
import com.aws.greengrass.shadowmanager.sync.model.SyncContext;
import com.aws.greengrass.shadowmanager.sync.strategy.SyncStrategy;
import com.aws.greengrass.shadowmanager.sync.strategy.model.Strategy;
import com.aws.greengrass.shadowmanager.sync.strategy.model.StrategyType;
import com.aws.greengrass.shadowmanager.util.ShadowWriteSynchronizeHelper;
import com.aws.greengrass.testcommons.testutilities.GGExtension;
import com.aws.greengrass.testcommons.testutilities.GGServiceTestUtil;
import com.aws.greengrass.util.Pair;
import com.fasterxml.jackson.databind.exc.MismatchedInputException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.NullAndEmptySource;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import software.amazon.awssdk.aws.greengrass.GreengrassCoreIPCService;
import software.amazon.awssdk.crt.mqtt.MqttClientConnectionEvents;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import static com.aws.greengrass.componentmanager.KernelConfigResolver.CONFIGURATION_CONFIG_KEY;
import static com.aws.greengrass.deployment.DeviceConfiguration.DEVICE_PARAM_THING_NAME;
import static com.aws.greengrass.shadowmanager.ShadowManager.SERVICE_NAME;
import static com.aws.greengrass.shadowmanager.model.Constants.CONFIGURATION_CLASSIC_SHADOW_TOPIC;
import static com.aws.greengrass.shadowmanager.model.Constants.CONFIGURATION_CORE_THING_TOPIC;
import static com.aws.greengrass.shadowmanager.model.Constants.CONFIGURATION_NAMED_SHADOWS_TOPIC;
import static com.aws.greengrass.shadowmanager.model.Constants.CONFIGURATION_SHADOW_DOCUMENTS_MAP_TOPIC;
import static com.aws.greengrass.shadowmanager.model.Constants.CONFIGURATION_SHADOW_DOCUMENTS_TOPIC;
import static com.aws.greengrass.shadowmanager.model.Constants.CONFIGURATION_STRATEGY_TOPIC;
import static com.aws.greengrass.shadowmanager.model.Constants.CONFIGURATION_SYNCHRONIZATION_TOPIC;
import static com.aws.greengrass.shadowmanager.model.Constants.CONFIGURATION_SYNC_DIRECTION_TOPIC;
import static com.aws.greengrass.shadowmanager.model.Constants.CONFIGURATION_THING_NAME_TOPIC;
import static com.aws.greengrass.shadowmanager.model.Constants.STRATEGY_TYPE_REAL_TIME;
import static com.aws.greengrass.testcommons.testutilities.ExceptionLogProtector.ignoreExceptionOfType;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anySet;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atMostOnce;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static software.amazon.awssdk.aws.greengrass.GreengrassCoreIPCService.DELETE_THING_SHADOW;
import static software.amazon.awssdk.aws.greengrass.GreengrassCoreIPCService.GET_THING_SHADOW;
import static software.amazon.awssdk.aws.greengrass.GreengrassCoreIPCService.LIST_NAMED_SHADOWS_FOR_THING;
import static software.amazon.awssdk.aws.greengrass.GreengrassCoreIPCService.UPDATE_THING_SHADOW;

@ExtendWith({MockitoExtension.class, GGExtension.class})
class ShadowManagerUnitTest extends GGServiceTestUtil {
    private final static String THING_NAME_A = "thingNameA";
    private final static String THING_NAME_B = "thingNameB";
    private final static String THING_NAME_C = "thingNameC";
    private final static String KERNEL_THING = "kernelThing";

    @Mock
    private ShadowManagerDatabase mockDatabase;
    @Mock
    private ShadowManagerDAOImpl mockDao;
    @Mock
    private AuthorizationHandlerWrapper mockAuthorizationHandlerWrapper;
    @Mock
    private PubSubClientWrapper mockPubSubClientWrapper;
    @Mock
    private InboundRateLimiter mockInboundRateLimiter;
    @Mock
    private DeviceConfiguration mockDeviceConfiguration;
    @Mock
    private ShadowWriteSynchronizeHelper mockSynchronizeHelper;
    @Mock
    private SyncHandler mockSyncHandler;
    @Mock
    private SyncConfigurationUpdater mockSyncConfigurationUpdater;
    @Mock
    private IotDataPlaneClientWrapper mockIotDataPlaneClientWrapper;
    @Mock
    private CloudDataClient mockCloudDataClient;
    @Mock
    private MqttClient mockMqttClient;
    @Mock
    private GreengrassCoreIPCService mockGreengrassCoreIPCService;

    @Captor
    private ArgumentCaptor<MqttClientConnectionEvents> mqttCallbacksCaptor;
    @Captor
    private ArgumentCaptor<CallbackEventManager.OnConnectCallback> mqtOnConnectCallbackCaptor;
    @Captor
    private ArgumentCaptor<Strategy> strategyCaptor;
    private final DirectionWrapper direction = new DirectionWrapper();
    private ShadowManager shadowManager;

    @BeforeEach
    public void setup() {
        serviceFullName = "aws.greengrass.ShadowManager";
        initializeMockedConfig();
        when(mockSyncHandler.getSyncConfigurationUpdater()).then(i -> mockSyncConfigurationUpdater);
        shadowManager = new ShadowManager(config, mockDatabase, mockDao, mockAuthorizationHandlerWrapper,
                mockPubSubClientWrapper, mockInboundRateLimiter, mockDeviceConfiguration, mockSynchronizeHelper,
                mockIotDataPlaneClientWrapper, mockSyncHandler, mockCloudDataClient, mockMqttClient, direction);
        lenient().when(config.lookupTopics(CONFIGURATION_CONFIG_KEY))
                .thenReturn(Topics.of(context, CONFIGURATION_CONFIG_KEY, null));
        // These are added to not break the existing unit tests. Will be removed later.
        lenient().when(config.getContext().get(InboundRateLimiter.class)).thenReturn(mockInboundRateLimiter);
        lenient().when(config.getContext().get(IotDataPlaneClientWrapper.class)).thenReturn(mockIotDataPlaneClientWrapper);
    }

    @AfterEach
    void tearDown() throws IOException {
       context.close();
    }

    @ParameterizedTest
    @EnumSource(Direction.class)
    void GIVEN_current_direction_WHEN_updated_to_BIDRECTIONAL_THEN_appropriately_handles_the_sync_direction(Direction current) {
        ShadowManager s = spy(shadowManager);

        lenient().doReturn(true).when(s).inState(eq(State.RUNNING));

        s.setSyncConfiguration(ShadowSyncConfiguration.builder().syncConfigurations(new HashSet<>()).build());
        ThingShadowSyncConfiguration syncConfiguration = mock(ThingShadowSyncConfiguration.class);
        s.getSyncConfiguration().getSyncConfigurations().add(syncConfiguration);

        lenient().when(mockMqttClient.connected()).thenReturn(true);
        SyncStrategy mockSyncStrategy = mock(SyncStrategy.class);
        lenient().when(mockSyncHandler.getOverallSyncStrategy()).thenReturn(mockSyncStrategy);
        direction.setDirection(current);
        Topic directionTopic = Topic.of(context, CONFIGURATION_SYNC_DIRECTION_TOPIC, Direction.BETWEEN_DEVICE_AND_CLOUD.getCode());
        when(config.lookup(CONFIGURATION_CONFIG_KEY, CONFIGURATION_SYNCHRONIZATION_TOPIC, CONFIGURATION_SYNC_DIRECTION_TOPIC))
                .thenReturn(directionTopic);
        s.install(ShadowManager.InstallConfig.builder().configureSyncDirectionConfig(true).build());

        assertFalse(s.isErrored());
        assertEquals(Direction.BETWEEN_DEVICE_AND_CLOUD, direction.get());

        if (Direction.BETWEEN_DEVICE_AND_CLOUD.equals(current)) {
            verify(mockSyncHandler, never()).start(any(), anyInt());
            verify(mockCloudDataClient, never()).updateSubscriptions(any());
            verify(mockSyncHandler, never()).stop();
            return;
        }

        if (Direction.DEVICE_TO_CLOUD.equals(current)) {
            verify(mockSyncHandler, never()).start(any(), anyInt());
            verify(mockCloudDataClient, times(1)).updateSubscriptions(any());
        } else {
            verify(mockCloudDataClient, never()).updateSubscriptions(any());
        }
        verify(mockSyncHandler, never()).stop();
        verify(mockDao, never()).listSyncedShadows();
        verify(mockDao, never()).deleteSyncInformation(anyString(), anyString());
        verify(mockDao, never()).insertSyncInfoIfNotExists(any());
    }

    @ParameterizedTest
    @EnumSource(Direction.class)
    void GIVEN_current_direction_WHEN_updated_to_FROMDEVICEONLY_THEN_appropriately_handles_the_sync_direction(Direction current) {
        ShadowManager s = spy(shadowManager);

        lenient().doReturn(true).when(s).inState(eq(State.RUNNING));
        s.setSyncConfiguration(ShadowSyncConfiguration.builder().syncConfigurations(new HashSet<>()).build());
        ThingShadowSyncConfiguration syncConfiguration = mock(ThingShadowSyncConfiguration.class);
        s.getSyncConfiguration().getSyncConfigurations().add(syncConfiguration);

        lenient().when(mockMqttClient.connected()).thenReturn(true);
        SyncStrategy mockSyncStrategy = mock(SyncStrategy.class);
        lenient().when(mockSyncHandler.getOverallSyncStrategy()).thenReturn(mockSyncStrategy);
        direction.setDirection(current);
        Topic directionTopic = Topic.of(context, CONFIGURATION_SYNC_DIRECTION_TOPIC, Direction.DEVICE_TO_CLOUD.getCode());
        when(config.lookup(CONFIGURATION_CONFIG_KEY, CONFIGURATION_SYNCHRONIZATION_TOPIC, CONFIGURATION_SYNC_DIRECTION_TOPIC))
                .thenReturn(directionTopic);
        s.install(ShadowManager.InstallConfig.builder().configureSyncDirectionConfig(true).build());

        assertFalse(s.isErrored());
        assertEquals(Direction.DEVICE_TO_CLOUD, direction.get());

        if (Direction.DEVICE_TO_CLOUD.equals(current)) {
            verify(mockSyncHandler, never()).start(any(), anyInt());
            verify(mockCloudDataClient, never()).updateSubscriptions(any());
            verify(mockSyncHandler, never()).stop();
            return;
        }

        verify(mockCloudDataClient, times(1)).stopSubscribing();
        verify(mockSyncHandler, never()).start(any(), anyInt());
        verify(mockCloudDataClient, never()).updateSubscriptions(any());
        verify(mockSyncHandler, never()).stop();

        verify(mockDao, never()).listSyncedShadows();
        verify(mockDao, never()).deleteSyncInformation(anyString(), anyString());
        verify(mockDao, never()).insertSyncInfoIfNotExists(any());
    }

    @ParameterizedTest
    @EnumSource(Direction.class)
    void GIVEN_current_direction_WHEN_updated_to_FROMCLOUDONLY_THEN_appropriately_handles_the_sync_direction(Direction current) {
        ShadowManager s = spy(shadowManager);

        lenient().doReturn(true).when(s).inState(eq(State.RUNNING));
        s.setSyncConfiguration(ShadowSyncConfiguration.builder().syncConfigurations(new HashSet<>()).build());
        ThingShadowSyncConfiguration syncConfiguration = mock(ThingShadowSyncConfiguration.class);
        s.getSyncConfiguration().getSyncConfigurations().add(syncConfiguration);

        lenient().when(mockMqttClient.connected()).thenReturn(true);
        SyncStrategy mockSyncStrategy = mock(SyncStrategy.class);
        lenient().when(mockSyncHandler.getOverallSyncStrategy()).thenReturn(mockSyncStrategy);
        direction.setDirection(current);
        Topic directionTopic = Topic.of(context, CONFIGURATION_SYNC_DIRECTION_TOPIC, Direction.CLOUD_TO_DEVICE.getCode());
        when(config.lookup(CONFIGURATION_CONFIG_KEY, CONFIGURATION_SYNCHRONIZATION_TOPIC, CONFIGURATION_SYNC_DIRECTION_TOPIC))
                .thenReturn(directionTopic);
        s.install(ShadowManager.InstallConfig.builder().configureSyncDirectionConfig(true).build());

        assertFalse(s.isErrored());
        assertEquals(Direction.CLOUD_TO_DEVICE, direction.get());

        if (Direction.CLOUD_TO_DEVICE.equals(current)) {
            verify(mockSyncHandler, never()).start(any(), anyInt());
            verify(mockCloudDataClient, never()).updateSubscriptions(any());
            verify(mockSyncHandler, never()).stop();
            return;
        }

        if (Direction.BETWEEN_DEVICE_AND_CLOUD.equals(current)) {
            verify(mockCloudDataClient, never()).updateSubscriptions(any());
        } else {
            verify(mockCloudDataClient, times(1)).updateSubscriptions(any());
        }

        verify(mockSyncHandler, never()).stop();
        verify(mockSyncHandler, never()).start(any(), anyInt());
        verify(mockDao, never()).listSyncedShadows();
        verify(mockDao, never()).deleteSyncInformation(anyString(), anyString());
        verify(mockDao, never()).insertSyncInfoIfNotExists(any());
    }

    @Test
    void GIVEN_bad_sync_direction_WHEN_initialize_THEN_throws_exception(ExtensionContext extensionContext) {
        ignoreExceptionOfType(extensionContext, IllegalArgumentException.class);
        Topic syncDirectionTopic = Topic.of(context, CONFIGURATION_SYNC_DIRECTION_TOPIC, "badValue");
        when(config.lookup(CONFIGURATION_CONFIG_KEY, CONFIGURATION_SYNCHRONIZATION_TOPIC, CONFIGURATION_SYNC_DIRECTION_TOPIC))
                .thenReturn(syncDirectionTopic);
        ShadowManager s = spy(shadowManager);

        lenient().doReturn(true).when(s).inState(eq(State.RUNNING));

        s.install(ShadowManager.InstallConfig.builder().configureSyncDirectionConfig(true).build());
        assertTrue(s.isErrored());
    }

    @Test
    void GIVEN_good_sync_configuration_WHEN_initialize_THEN_processes_configuration_correctly() throws UnsupportedInputTypeException {
        Topic thingNameTopic = mock(Topic.class);
        Topics configTopics = Topics.of(context, CONFIGURATION_SYNCHRONIZATION_TOPIC, null);
        List<Map<String, Object>> shadowDocumentsList = new ArrayList<>();
        Map<String, Object> thingAMap = new HashMap<>();
        thingAMap.put(CONFIGURATION_THING_NAME_TOPIC, THING_NAME_A);
        thingAMap.put(CONFIGURATION_CLASSIC_SHADOW_TOPIC, false);
        thingAMap.put(CONFIGURATION_NAMED_SHADOWS_TOPIC, Arrays.asList("foo", "bar"));
        Map<String, Object> thingBMap = new HashMap<>();
        thingBMap.put(CONFIGURATION_THING_NAME_TOPIC, THING_NAME_B);
        thingBMap.put(CONFIGURATION_NAMED_SHADOWS_TOPIC, Collections.singletonList("foo2"));
        shadowDocumentsList.add(thingAMap);
        shadowDocumentsList.add(thingBMap);
        configTopics.createLeafChild(CONFIGURATION_SHADOW_DOCUMENTS_TOPIC).withValueChecked(shadowDocumentsList);
        Topics systemConfigTopics = configTopics.createInteriorChild(CONFIGURATION_CORE_THING_TOPIC);
        systemConfigTopics.createLeafChild(CONFIGURATION_CLASSIC_SHADOW_TOPIC).withValue("true");
        systemConfigTopics.createLeafChild(CONFIGURATION_NAMED_SHADOWS_TOPIC).withValue(Collections.singletonList("boo2"));

        when(thingNameTopic.getOnce()).thenReturn(KERNEL_THING);
        when(config.lookupTopics(CONFIGURATION_CONFIG_KEY, CONFIGURATION_SYNCHRONIZATION_TOPIC))
                .thenReturn(configTopics);
        when(mockDeviceConfiguration.getThingName()).thenReturn(thingNameTopic);
        shadowManager.install(ShadowManager.InstallConfig.builder().configureSynchronizeConfig(true).build());

        verify(thingNameTopic, times(1)).subscribeGeneric(any());
        verify(thingNameTopic, times(0)).remove(any());
        assertFalse(shadowManager.isErrored());

        assertThat(shadowManager.getSyncConfiguration().getSyncConfigurations(),
                containsInAnyOrder(
                        ThingShadowSyncConfiguration.builder().thingName(KERNEL_THING).shadowName("").build(),
                        ThingShadowSyncConfiguration.builder().thingName(KERNEL_THING).shadowName("boo2").build(),
                        ThingShadowSyncConfiguration.builder().thingName(THING_NAME_A).shadowName("foo").build(),
                        ThingShadowSyncConfiguration.builder().thingName(THING_NAME_A).shadowName("bar").build(),
                        ThingShadowSyncConfiguration.builder().thingName(THING_NAME_B).shadowName("").build(),
                        ThingShadowSyncConfiguration.builder().thingName(THING_NAME_B).shadowName("foo2").build()));
    }

    @Test
    void GIVEN_good_sync_configuration_without_nucleus_thing_config_in_list_WHEN_initialize_THEN_processes_configuration_correctly() throws UnsupportedInputTypeException {
        Topic thingNameTopic = mock(Topic.class);
        Topics configTopics = Topics.of(context, CONFIGURATION_SYNCHRONIZATION_TOPIC, null);
        List<Map<String, Object>> shadowDocumentsList = new ArrayList<>();
        Map<String, Object> thingAMap = new HashMap<>();
        thingAMap.put(CONFIGURATION_THING_NAME_TOPIC, THING_NAME_A);
        thingAMap.put(CONFIGURATION_CLASSIC_SHADOW_TOPIC, false);
        thingAMap.put(CONFIGURATION_NAMED_SHADOWS_TOPIC, Arrays.asList("foo", "bar"));
        Map<String, Object> thingBMap = new HashMap<>();
        thingBMap.put(CONFIGURATION_THING_NAME_TOPIC, THING_NAME_B);
        thingBMap.put(CONFIGURATION_NAMED_SHADOWS_TOPIC, Collections.singletonList("foo2"));
        shadowDocumentsList.add(thingAMap);
        shadowDocumentsList.add(thingBMap);
        configTopics.createLeafChild(CONFIGURATION_SHADOW_DOCUMENTS_TOPIC).withValueChecked(shadowDocumentsList);

        when(thingNameTopic.getOnce()).thenReturn(KERNEL_THING);
        when(config.lookupTopics(CONFIGURATION_CONFIG_KEY, CONFIGURATION_SYNCHRONIZATION_TOPIC))
                .thenReturn(configTopics);
        when(mockDeviceConfiguration.getThingName()).thenReturn(thingNameTopic);
        shadowManager.install(ShadowManager.InstallConfig.builder().configureSynchronizeConfig(true).build());

        verify(thingNameTopic, times(0)).subscribeGeneric(any());
        verify(thingNameTopic, times(1)).remove(any());
        assertFalse(shadowManager.isErrored());
        assertThat(shadowManager.getSyncConfiguration().getSyncConfigurations(),
                containsInAnyOrder(
                        ThingShadowSyncConfiguration.builder().thingName(THING_NAME_A).shadowName("foo").build(),
                        ThingShadowSyncConfiguration.builder().thingName(THING_NAME_A).shadowName("bar").build(),
                        ThingShadowSyncConfiguration.builder().thingName(THING_NAME_B).shadowName("").build(),
                        ThingShadowSyncConfiguration.builder().thingName(THING_NAME_B).shadowName("foo2").build()));
    }

    @Test
    void GIVEN_good_sync_configuration_without_nucleus_thing_config_in_map_WHEN_initialize_THEN_processes_configuration_correctly() {
        Topic thingNameTopic = mock(Topic.class);
        Topics configTopics = Topics.of(context, CONFIGURATION_SYNCHRONIZATION_TOPIC, null);
        Topics thingConfigTopics = configTopics.createInteriorChild(CONFIGURATION_SHADOW_DOCUMENTS_MAP_TOPIC);
        Topics thingATopics = thingConfigTopics.createInteriorChild(THING_NAME_A);
        thingATopics.createLeafChild(CONFIGURATION_CLASSIC_SHADOW_TOPIC).withValue(false);
        thingATopics.createLeafChild(CONFIGURATION_NAMED_SHADOWS_TOPIC).withValue(Arrays.asList("foo", "bar"));
        Topics thingBTopics = thingConfigTopics.createInteriorChild(THING_NAME_B);
        thingBTopics.createLeafChild(CONFIGURATION_NAMED_SHADOWS_TOPIC).withValue(Collections.singletonList("foo2"));

        when(thingNameTopic.getOnce()).thenReturn(KERNEL_THING);
        when(config.lookupTopics(CONFIGURATION_CONFIG_KEY, CONFIGURATION_SYNCHRONIZATION_TOPIC))
                .thenReturn(configTopics);
        when(mockDeviceConfiguration.getThingName()).thenReturn(thingNameTopic);
        shadowManager.install(ShadowManager.InstallConfig.builder().configureSynchronizeConfig(true).build());

        verify(thingNameTopic, times(0)).subscribeGeneric(any());
        verify(thingNameTopic, times(1)).remove(any());
        assertFalse(shadowManager.isErrored());
        assertThat(shadowManager.getSyncConfiguration().getSyncConfigurations(),
                containsInAnyOrder(
                        ThingShadowSyncConfiguration.builder().thingName(THING_NAME_A).shadowName("foo").build(),
                        ThingShadowSyncConfiguration.builder().thingName(THING_NAME_A).shadowName("bar").build(),
                        ThingShadowSyncConfiguration.builder().thingName(THING_NAME_B).shadowName("").build(),
                        ThingShadowSyncConfiguration.builder().thingName(THING_NAME_B).shadowName("foo2").build()));
    }

    @Test
    void GIVEN_good_sync_configuration_without_nucleus_thing_config_in_map_and_list_WHEN_initialize_THEN_processes_configuration_correctly() throws UnsupportedInputTypeException {
        Topic thingNameTopic = mock(Topic.class);
        Topics configTopics = Topics.of(context, CONFIGURATION_SYNCHRONIZATION_TOPIC, null);
        Topics thingConfigTopics = configTopics.createInteriorChild(CONFIGURATION_SHADOW_DOCUMENTS_MAP_TOPIC);
        Topics thingATopics = thingConfigTopics.createInteriorChild(THING_NAME_A);
        thingATopics.createLeafChild(CONFIGURATION_CLASSIC_SHADOW_TOPIC).withValue(false);
        thingATopics.createLeafChild(CONFIGURATION_NAMED_SHADOWS_TOPIC).withValue(Arrays.asList("foo", "bar"));
        Topics thingBTopics = thingConfigTopics.createInteriorChild(THING_NAME_B);
        thingBTopics.createLeafChild(CONFIGURATION_NAMED_SHADOWS_TOPIC).withValue(Collections.singletonList("foo2"));
        List<Map<String, Object>> shadowDocumentsList = new ArrayList<>();
        Map<String, Object> thingCMap = new HashMap<>();
        thingCMap.put(CONFIGURATION_THING_NAME_TOPIC, THING_NAME_C);
        thingCMap.put(CONFIGURATION_CLASSIC_SHADOW_TOPIC, true);
        thingCMap.put(CONFIGURATION_NAMED_SHADOWS_TOPIC, Collections.singletonList("foo100"));
        Map<String, Object> thingBMap = new HashMap<>();
        thingBMap.put(CONFIGURATION_THING_NAME_TOPIC, THING_NAME_B);
        thingBMap.put(CONFIGURATION_NAMED_SHADOWS_TOPIC, Collections.singletonList("foo2"));
        shadowDocumentsList.add(thingCMap);
        shadowDocumentsList.add(thingBMap);
        configTopics.createLeafChild(CONFIGURATION_SHADOW_DOCUMENTS_TOPIC).withValueChecked(shadowDocumentsList);

        when(thingNameTopic.getOnce()).thenReturn(KERNEL_THING);
        when(config.lookupTopics(CONFIGURATION_CONFIG_KEY, CONFIGURATION_SYNCHRONIZATION_TOPIC))
                .thenReturn(configTopics);
        when(mockDeviceConfiguration.getThingName()).thenReturn(thingNameTopic);
        shadowManager.install(ShadowManager.InstallConfig.builder().configureSynchronizeConfig(true).build());

        verify(thingNameTopic, times(0)).subscribeGeneric(any());
        verify(thingNameTopic, times(1)).remove(any());
        assertFalse(shadowManager.isErrored());
        assertThat(shadowManager.getSyncConfiguration().getSyncConfigurations(),
                containsInAnyOrder(
                        ThingShadowSyncConfiguration.builder().thingName(THING_NAME_A).shadowName("foo").build(),
                        ThingShadowSyncConfiguration.builder().thingName(THING_NAME_A).shadowName("bar").build(),
                        ThingShadowSyncConfiguration.builder().thingName(THING_NAME_C).shadowName("").build(),
                        ThingShadowSyncConfiguration.builder().thingName(THING_NAME_C).shadowName("foo100").build(),
                        ThingShadowSyncConfiguration.builder().thingName(THING_NAME_B).shadowName("").build(),
                        ThingShadowSyncConfiguration.builder().thingName(THING_NAME_B).shadowName("foo2").build()));
    }

    @Test
    void GIVEN_good_sync_configuration_with_only_nucleus_thing_config_WHEN_thing_name_changes_THEN_updates_nucleus_configuration_correctly() throws UnsupportedInputTypeException {
        Topic thingNameTopic = Topic.of(context, DEVICE_PARAM_THING_NAME, KERNEL_THING);
        Topics configTopics = Topics.of(context, CONFIGURATION_SYNCHRONIZATION_TOPIC, null);
        Topics systemConfigTopics = configTopics.createInteriorChild(CONFIGURATION_CORE_THING_TOPIC);
        systemConfigTopics.createLeafChild(CONFIGURATION_CLASSIC_SHADOW_TOPIC).withValue("true");
        systemConfigTopics.createLeafChild(CONFIGURATION_NAMED_SHADOWS_TOPIC).withValue(Collections.singletonList("boo2"));

        when(config.lookupTopics(CONFIGURATION_CONFIG_KEY, CONFIGURATION_SYNCHRONIZATION_TOPIC))
                .thenReturn(configTopics);
        when(mockDeviceConfiguration.getThingName()).thenReturn(thingNameTopic);
        shadowManager.install(ShadowManager.InstallConfig.builder().configureSynchronizeConfig(true).build());

        assertFalse(shadowManager.isErrored());
        assertThat(shadowManager.getSyncConfiguration().getSyncConfigurations(),
                containsInAnyOrder(
                        ThingShadowSyncConfiguration.builder().thingName(KERNEL_THING).shadowName("").build(),
                        ThingShadowSyncConfiguration.builder().thingName(KERNEL_THING).shadowName("boo2").build()));
    }

    @Test
    void GIVEN_bad_type_of_nucleus_sync_configuration_WHEN_initialize_THEN_service_errors(ExtensionContext extensionContext) throws UnsupportedInputTypeException {
        ignoreExceptionOfType(extensionContext, InvalidConfigurationException.class);
        Topic thingNameTopic = Topic.of(context, DEVICE_PARAM_THING_NAME, KERNEL_THING);
        Topics configTopics = Topics.of(context, CONFIGURATION_SYNCHRONIZATION_TOPIC, null);
        List<Map<String, Object>> shadowDocumentsList = new ArrayList<>();
        Map<String, Object> thingAMap = new HashMap<>();
        thingAMap.put(CONFIGURATION_CLASSIC_SHADOW_TOPIC, false);
        thingAMap.put(CONFIGURATION_NAMED_SHADOWS_TOPIC, Arrays.asList("foo", "bar"));
        shadowDocumentsList.add(thingAMap);
        configTopics.createLeafChild(CONFIGURATION_CORE_THING_TOPIC).withValueChecked(shadowDocumentsList);
        configTopics.createLeafChild(CONFIGURATION_SHADOW_DOCUMENTS_TOPIC).withValueChecked(null);

        when(config.lookupTopics(CONFIGURATION_CONFIG_KEY, CONFIGURATION_SYNCHRONIZATION_TOPIC))
                .thenReturn(configTopics);
        when(mockDeviceConfiguration.getThingName()).thenReturn(thingNameTopic);
        shadowManager.install(ShadowManager.InstallConfig.builder().configureSynchronizeConfig(true).build());
        assertTrue(shadowManager.isErrored());
    }

    @Test
    void GIVEN_bad_field_in_thing_sync_configuration_WHEN_initialize_THEN_service_errors(ExtensionContext extensionContext) throws UnsupportedInputTypeException {
        ignoreExceptionOfType(extensionContext, MismatchedInputException.class);
        ignoreExceptionOfType(extensionContext, InvalidConfigurationException.class);
        Topics configTopics = Topics.of(context, CONFIGURATION_SYNCHRONIZATION_TOPIC, null);
        configTopics.createLeafChild(CONFIGURATION_CORE_THING_TOPIC).withValueChecked(null);
        List<Map<String, Object>> shadowDocumentsList = new ArrayList<>();
        Map<String, Object> thingAMap = new HashMap<>();
        thingAMap.put(CONFIGURATION_THING_NAME_TOPIC, THING_NAME_A);
        thingAMap.put(CONFIGURATION_CLASSIC_SHADOW_TOPIC, false);
        thingAMap.put(CONFIGURATION_NAMED_SHADOWS_TOPIC, "foo");
        shadowDocumentsList.add(thingAMap);
        configTopics.createLeafChild(CONFIGURATION_SHADOW_DOCUMENTS_TOPIC).withValueChecked(shadowDocumentsList);

        Topic thingNameTopic = Topic.of(context, DEVICE_PARAM_THING_NAME, KERNEL_THING);
        when(mockDeviceConfiguration.getThingName()).thenReturn(thingNameTopic);
        when(config.lookupTopics(CONFIGURATION_CONFIG_KEY, CONFIGURATION_SYNCHRONIZATION_TOPIC))
                .thenReturn(configTopics);
        shadowManager.install(ShadowManager.InstallConfig.builder().configureSynchronizeConfig(true).build());
        assertTrue(shadowManager.isErrored());
    }

    @Test
    void GIVEN_bad_type_of_thing_sync_configuration_WHEN_initialize_THEN_service_errors(ExtensionContext extensionContext) throws UnsupportedInputTypeException {
        ignoreExceptionOfType(extensionContext, InvalidConfigurationException.class);
        Topics configTopics = Topics.of(context, CONFIGURATION_SYNCHRONIZATION_TOPIC, null);
        configTopics.createLeafChild(CONFIGURATION_CORE_THING_TOPIC).withValueChecked(null);
        Topics shadowDocumentsTopics = configTopics.createInteriorChild(CONFIGURATION_SHADOW_DOCUMENTS_TOPIC);
        shadowDocumentsTopics.createLeafChild(CONFIGURATION_CLASSIC_SHADOW_TOPIC).withValue("true");
        shadowDocumentsTopics.createLeafChild(CONFIGURATION_NAMED_SHADOWS_TOPIC).withValue(Collections.singletonList("boo2"));
        shadowDocumentsTopics.createLeafChild(CONFIGURATION_THING_NAME_TOPIC).withValue(THING_NAME_A);

        when(config.lookupTopics(CONFIGURATION_CONFIG_KEY, CONFIGURATION_SYNCHRONIZATION_TOPIC))
                .thenReturn(configTopics);
        shadowManager.install(ShadowManager.InstallConfig.builder().configureSynchronizeConfig(true).build());
        assertTrue(shadowManager.isErrored());
    }

    @ParameterizedTest
    @MethodSource("com.aws.greengrass.shadowmanager.TestUtils#invalidShadowNames")
    void GIVEN_bad_shadow_names_WHEN_initialize_THEN_service_errors(String shadowName, ExtensionContext extensionContext) throws UnsupportedInputTypeException {
        ignoreExceptionOfType(extensionContext, InvalidConfigurationException.class);
        ignoreExceptionOfType(extensionContext, InvalidRequestParametersException.class);
        Topic thingNameTopic = mock(Topic.class);
        Topics configTopics = Topics.of(context, CONFIGURATION_SYNCHRONIZATION_TOPIC, null);
        List<Map<String, Object>> shadowDocumentsList = new ArrayList<>();
        Map<String, Object> thingAMap = new HashMap<>();
        thingAMap.put(CONFIGURATION_THING_NAME_TOPIC, THING_NAME_A);
        thingAMap.put(CONFIGURATION_CLASSIC_SHADOW_TOPIC, false);
        thingAMap.put(CONFIGURATION_NAMED_SHADOWS_TOPIC, Collections.singletonList(shadowName));
        Map<String, Object> thingBMap = new HashMap<>();
        thingBMap.put(CONFIGURATION_THING_NAME_TOPIC, THING_NAME_B);
        thingBMap.put(CONFIGURATION_NAMED_SHADOWS_TOPIC, Collections.singletonList("foo2"));
        shadowDocumentsList.add(thingAMap);
        shadowDocumentsList.add(thingBMap);
        configTopics.createLeafChild(CONFIGURATION_SHADOW_DOCUMENTS_TOPIC).withValueChecked(shadowDocumentsList);

        when(thingNameTopic.getOnce()).thenReturn(KERNEL_THING);
        when(config.lookupTopics(CONFIGURATION_CONFIG_KEY, CONFIGURATION_SYNCHRONIZATION_TOPIC))
                .thenReturn(configTopics);
        when(mockDeviceConfiguration.getThingName()).thenReturn(thingNameTopic);
        shadowManager.install(ShadowManager.InstallConfig.builder().configureSynchronizeConfig(true).build());
        assertTrue(shadowManager.isErrored());
    }

    @ParameterizedTest
    @NullAndEmptySource
    @MethodSource("com.aws.greengrass.shadowmanager.TestUtils#invalidThingNames")
    void GIVEN_bad_thing_names_WHEN_initialize_THEN_service_errors(String thingName, ExtensionContext extensionContext) throws UnsupportedInputTypeException {
        ignoreExceptionOfType(extensionContext, InvalidConfigurationException.class);
        ignoreExceptionOfType(extensionContext, InvalidRequestParametersException.class);
        Topic thingNameTopic = mock(Topic.class);
        Topics configTopics = Topics.of(context, CONFIGURATION_SYNCHRONIZATION_TOPIC, null);
        List<Map<String, Object>> shadowDocumentsList = new ArrayList<>();
        Map<String, Object> thingAMap = new HashMap<>();
        thingAMap.put(CONFIGURATION_THING_NAME_TOPIC, thingName);
        thingAMap.put(CONFIGURATION_CLASSIC_SHADOW_TOPIC, false);
        thingAMap.put(CONFIGURATION_NAMED_SHADOWS_TOPIC, Arrays.asList("foo", "bar"));
        Map<String, Object> thingBMap = new HashMap<>();
        thingBMap.put(CONFIGURATION_THING_NAME_TOPIC, THING_NAME_B);
        thingBMap.put(CONFIGURATION_NAMED_SHADOWS_TOPIC, Collections.singletonList("foo2"));
        shadowDocumentsList.add(thingAMap);
        shadowDocumentsList.add(thingBMap);
        configTopics.createLeafChild(CONFIGURATION_SHADOW_DOCUMENTS_TOPIC).withValueChecked(shadowDocumentsList);

        when(thingNameTopic.getOnce()).thenReturn(KERNEL_THING);
        when(config.lookupTopics(CONFIGURATION_CONFIG_KEY, CONFIGURATION_SYNCHRONIZATION_TOPIC))
                .thenReturn(configTopics);
        when(mockDeviceConfiguration.getThingName()).thenReturn(thingNameTopic);

        shadowManager.install(ShadowManager.InstallConfig.builder().configureSynchronizeConfig(true).build());
        assertTrue(shadowManager.isErrored());
    }

    @Test
    void GIVEN_mqtt_client_callbacks_WHEN_onConnectionInterrupted_THEN_stops_sync_handler_and_unsubscribes() throws AuthorizationException {
        shadowManager.setGreengrassCoreIPCService(mockGreengrassCoreIPCService);
        shadowManager.setSyncConfiguration(ShadowSyncConfiguration.builder().syncConfigurations(new HashSet<>()).build());
        shadowManager.getSyncConfiguration().getSyncConfigurations().add(mock(ThingShadowSyncConfiguration.class));
        doNothing().when(mockMqttClient).addToCallbackEvents(mqtOnConnectCallbackCaptor.capture(), mqttCallbacksCaptor.capture());
        when(mockDeviceConfiguration.isDeviceConfiguredToTalkToCloud()).thenReturn(true);
        shadowManager.postInject();
        verify(mockAuthorizationHandlerWrapper, times(1)).registerComponent(eq(SERVICE_NAME), anySet());
        verify(mockGreengrassCoreIPCService, times(1)).setOperationHandler(eq(GET_THING_SHADOW), any(Function.class));
        verify(mockGreengrassCoreIPCService, times(1)).setOperationHandler(eq(DELETE_THING_SHADOW), any(Function.class));
        verify(mockGreengrassCoreIPCService, times(1)).setOperationHandler(eq(UPDATE_THING_SHADOW), any(Function.class));
        verify(mockGreengrassCoreIPCService, times(1)).setOperationHandler(eq(LIST_NAMED_SHADOWS_FOR_THING), any(Function.class));

        assertThat(mqttCallbacksCaptor.getValue(), is(notNullValue()));
        assertThat(mqtOnConnectCallbackCaptor.getValue(), is(notNullValue()));

        mqttCallbacksCaptor.getValue().onConnectionInterrupted(0);
        verify(mockCloudDataClient, times(1)).stopSubscribing();
        verify(mockSyncHandler, times(1)).stop();
        verify(mockPubSubClientWrapper, atMostOnce()).subscribe(any());
    }

    @Test
    void GIVEN_shadow_manager_WHEN_startup_THEN_updates_stored_config_and_starts_sync_handler_and_unsubscribes() {
        createSyncConfigForSingleShadow("thing", "shadow");
        when(mockDao.listSyncedShadows()).thenReturn(Collections.singletonList(new Pair<>("foo", "bar")));

        when(mockMqttClient.connected()).thenReturn(true);
        shadowManager.startup();

        verify(mockCloudDataClient, times(1)).updateSubscriptions(anySet());
        verify(mockSyncHandler, times(1)).start(any(SyncContext.class), anyInt());

        verify(mockDao, times(1)).deleteSyncInformation("foo", "bar");

        ArgumentCaptor<SyncInformation> captor = ArgumentCaptor.forClass(SyncInformation.class);
        verify(mockDao, times(1)).insertSyncInfoIfNotExists(captor.capture());
        assertThat(captor.getValue().getThingName(), is("thing"));
        assertThat(captor.getValue().getShadowName(), is("shadow"));
    }

    private void createSyncConfigForSingleShadow(String thing, String shadow) {
        shadowManager.setSyncConfiguration(ShadowSyncConfiguration.builder().syncConfigurations(new HashSet<>()).build());
        ThingShadowSyncConfiguration config = mock(ThingShadowSyncConfiguration.class);
        when(config.getThingName()).thenReturn(thing);
        when(config.getShadowName()).thenReturn(shadow);
        shadowManager.getSyncConfiguration().getSyncConfigurations().add(config);
    }

    @Test
    void GIVEN_shadow_manager_WHEN_shutdown_THEN_shuts_down_gracefully() throws IOException {
        shadowManager.setGreengrassCoreIPCService(mockGreengrassCoreIPCService);
        shadowManager.setSyncConfiguration(ShadowSyncConfiguration.builder().syncConfigurations(new HashSet<>()).build());
        shadowManager.getSyncConfiguration().getSyncConfigurations().add(mock(ThingShadowSyncConfiguration.class));
        doNothing().when(mockMqttClient).addToCallbackEvents(mqtOnConnectCallbackCaptor.capture(), mqttCallbacksCaptor.capture());
        when(mockDeviceConfiguration.isDeviceConfiguredToTalkToCloud()).thenReturn(true);
        shadowManager.postInject();
        assertDoesNotThrow(() -> shadowManager.shutdown());
        verify(mockDatabase, atMostOnce()).close();
        verify(mockInboundRateLimiter, atMostOnce()).clear();
        verify(mockPubSubClientWrapper, atMostOnce()).unsubscribe(any());
    }

    @Test
    void GIVEN_installed_WHEN_running_and_config_updated_THEN_sync_restarted() throws Exception {
        when(mockMqttClient.connected()).thenReturn(true);

        ShadowManager s = spy(shadowManager);

        doReturn(false, true).when(s).inState(eq(State.RUNNING));

        try (Context context = new Context()){
            Topic thingNameTopic = mock(Topic.class);
            Topics configTopics = Topics.of(context, CONFIGURATION_SYNCHRONIZATION_TOPIC, null);
            List<Map<String, Object>> shadowDocumentsList = new ArrayList<>();
            Map<String, Object> thingAMap = new HashMap<>();
            thingAMap.put(CONFIGURATION_THING_NAME_TOPIC, THING_NAME_A);
            thingAMap.put(CONFIGURATION_CLASSIC_SHADOW_TOPIC, false);
            thingAMap.put(CONFIGURATION_NAMED_SHADOWS_TOPIC, Arrays.asList("foo"));
            Map<String, Object> thingBMap = new HashMap<>();
            thingBMap.put(CONFIGURATION_THING_NAME_TOPIC, THING_NAME_B);
            thingBMap.put(CONFIGURATION_NAMED_SHADOWS_TOPIC, Collections.singletonList("foo2"));
            shadowDocumentsList.add(thingAMap);
            shadowDocumentsList.add(thingBMap);
            configTopics.createLeafChild(CONFIGURATION_SHADOW_DOCUMENTS_TOPIC).withValueChecked(shadowDocumentsList);

            when(thingNameTopic.getOnce()).thenReturn(KERNEL_THING);
            when(config.lookupTopics(CONFIGURATION_CONFIG_KEY)).thenReturn(configTopics);
            when(config.lookupTopics(CONFIGURATION_CONFIG_KEY, CONFIGURATION_SYNCHRONIZATION_TOPIC)).thenReturn(configTopics);
            when(mockDeviceConfiguration.getThingName()).thenReturn(thingNameTopic);
            s.install(ShadowManager.InstallConfig.builder().configureSynchronizeConfig(true).build());

            // no dao access for sync during install
            verify(mockDao, never()).listSyncedShadows();

            // no restart of sync handler
            verify(mockSyncHandler, never()).stop();
            verify(mockSyncHandler, never()).start(any(SyncContext.class), anyInt());

            reset(mockSyncHandler, mockDao);

            when(mockDao.listSyncedShadows()).thenReturn(Collections.emptyList());

            CountDownLatch latch = new CountDownLatch(1);
            doAnswer(invocation -> {
                latch.countDown();
                return null;
            }).when(mockSyncHandler).start(any(SyncContext.class), anyInt());

            // WHEN
            configTopics.lookup(CONFIGURATION_SHADOW_DOCUMENTS_TOPIC).withValueChecked(Arrays.asList(thingAMap));

            // THEN
            assertThat("synchandler started", latch.await(10, TimeUnit.SECONDS), is(true));

            ArgumentCaptor<SyncInformation> captor = ArgumentCaptor.forClass(SyncInformation.class);

            verify(mockDao, times(1)).insertSyncInfoIfNotExists(captor.capture());
            assertThat(captor.getValue().getThingName(), is(THING_NAME_A));
            assertThat(captor.getValue().getShadowName(), is("foo"));
        }
    }

    @ParameterizedTest
    @CsvSource({"realTime,30", "periodic,30", "periodic,300"})
    void GIVEN_good_strategy_config_WHEN_initialize_THEN_correctly_sets_strategy_in_synchandler(String strategyType, long interval) {
        Topics strategyTopics = Topics.of(context, CONFIGURATION_STRATEGY_TOPIC, null);
        strategyTopics.createLeafChild("type").withValue(strategyType);
        strategyTopics.createLeafChild("delay").withValue(interval);

        ShadowManager s = spy(shadowManager);

        doReturn(true).when(s).inState(eq(State.RUNNING));
        when(config.lookupTopics(CONFIGURATION_CONFIG_KEY, CONFIGURATION_STRATEGY_TOPIC))
                .thenReturn(strategyTopics);
        doNothing().when(shadowManager.getSyncHandler()).setSyncStrategy(strategyCaptor.capture());
        s.setSyncConfiguration(ShadowSyncConfiguration.builder().syncConfigurations(new HashSet<>()).build());
        ThingShadowSyncConfiguration config = mock(ThingShadowSyncConfiguration.class);
        s.getSyncConfiguration().getSyncConfigurations().add(config);

        when(mockMqttClient.connected()).thenReturn(true);
        s.install(ShadowManager.InstallConfig.builder().configureStrategyConfig(true).build());

        assertFalse(s.isErrored());
        assertThat(strategyCaptor.getValue(), is(notNullValue()));
        if (STRATEGY_TYPE_REAL_TIME.equals(strategyType)) {
            assertThat(strategyCaptor.getValue().getType(), is(StrategyType.REALTIME));
        } else {
            assertThat(strategyCaptor.getValue().getType(), is(StrategyType.PERIODIC));
        }
        assertThat(strategyCaptor.getValue().getDelay(), is(equalTo(interval)));
        verify(mockSyncHandler, times(1)).stop();
        verify(mockSyncHandler, times(1)).start(any(), anyInt());
        verify(mockCloudDataClient, never()).updateSubscriptions(any());
    }

    @Test
    void GIVEN_bad_value_strategy_config_WHEN_initialize_THEN_service_errors(ExtensionContext extensionContext) {
        ignoreExceptionOfType(extensionContext, InvalidConfigurationException.class);
        Topics strategyTopics = Topics.of(context, CONFIGURATION_STRATEGY_TOPIC, null);
        strategyTopics.createLeafChild("type").withValue("real Time");

        when(config.lookupTopics(CONFIGURATION_CONFIG_KEY, CONFIGURATION_STRATEGY_TOPIC)).thenReturn(strategyTopics);
        shadowManager.install(ShadowManager.InstallConfig.builder().configureStrategyConfig(true).build());

        assertTrue(shadowManager.isErrored());
        verify(mockSyncHandler, never()).setSyncStrategy(any());
    }

    @Test
    void GIVEN_sync_strategy_WHEN_change_THEN_syncing_restarts() {
        Topics strategyTopics = Topics.of(context, CONFIGURATION_STRATEGY_TOPIC, null);
        strategyTopics.createLeafChild("type").withValue("realTime");
        strategyTopics.createLeafChild("delay").withValue(30);

        ShadowManager s = spy(shadowManager);

        // set up mocks so sync start gets called
        when(config.lookupTopics(CONFIGURATION_CONFIG_KEY, CONFIGURATION_STRATEGY_TOPIC))
                .thenReturn(strategyTopics);
        when(mockMqttClient.connected()).thenReturn(true);
        Set<ThingShadowSyncConfiguration> syncConfigs = new HashSet<ThingShadowSyncConfiguration>() {{
            add(ThingShadowSyncConfiguration.builder().thingName("foo").shadowName("bar").build());
        }};

        ShadowSyncConfiguration config = ShadowSyncConfiguration.builder()
                .syncConfigurations(syncConfigs)
                .build();
        s.setSyncConfiguration(config);
        doReturn(true,true).when(s).inState(eq(State.RUNNING));
        s.install(ShadowManager.InstallConfig.builder().configureStrategyConfig(true).build());
        reset(mockSyncHandler);
        // GIVEN
        Strategy update = Strategy.builder().type(StrategyType.PERIODIC).delay(500L).build();
        // WHEN
        Strategy ret = s.replaceStrategyIfNecessary(Strategy.DEFAULT_STRATEGY, update);
        // THEN
        assertThat(ret, equalTo(update));
        verify(mockSyncHandler, times(1)).stop();
        verify(mockSyncHandler, times(1)).start(any(), anyInt());
    }

    @Test
    void GIVEN_sync_strategy_WHEN_no_change_THEN_syncing_does_not_restart(ExtensionContext extensionContext) {
        ShadowManager s = spy(shadowManager);

        // GIVEN / WHEN
        Strategy ret = s.replaceStrategyIfNecessary(Strategy.DEFAULT_STRATEGY, Strategy.DEFAULT_STRATEGY);
        // THEN
        assertThat(ret, equalTo(Strategy.DEFAULT_STRATEGY));
        verify(mockSyncHandler, never()).stop();
        verify(mockSyncHandler, never()).start(any(), anyInt());
    }
}
