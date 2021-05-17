/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.shadowmanager;

import com.aws.greengrass.authorization.exceptions.AuthorizationException;
import com.aws.greengrass.config.Topic;
import com.aws.greengrass.config.Topics;
import com.aws.greengrass.config.UnsupportedInputTypeException;
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
import com.aws.greengrass.shadowmanager.sync.SyncHandler;
import com.aws.greengrass.shadowmanager.sync.model.SyncContext;
import com.aws.greengrass.shadowmanager.util.ShadowWriteSynchronizeHelper;
import com.aws.greengrass.shadowmanager.util.Validator;
import com.aws.greengrass.testcommons.testutilities.GGExtension;
import com.aws.greengrass.testcommons.testutilities.GGServiceTestUtil;
import com.fasterxml.jackson.databind.exc.MismatchedInputException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.NullAndEmptySource;
import org.junit.jupiter.params.provider.ValueSource;
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
import java.util.function.Function;

import static com.aws.greengrass.componentmanager.KernelConfigResolver.CONFIGURATION_CONFIG_KEY;
import static com.aws.greengrass.deployment.DeviceConfiguration.DEVICE_PARAM_THING_NAME;
import static com.aws.greengrass.shadowmanager.ShadowManager.SERVICE_NAME;
import static com.aws.greengrass.shadowmanager.model.Constants.CONFIGURATION_CLASSIC_SHADOW_TOPIC;
import static com.aws.greengrass.shadowmanager.model.Constants.CONFIGURATION_MAX_DOC_SIZE_LIMIT_B_TOPIC;
import static com.aws.greengrass.shadowmanager.model.Constants.CONFIGURATION_MAX_LOCAL_REQUESTS_RATE_PER_THING_TOPIC;
import static com.aws.greengrass.shadowmanager.model.Constants.CONFIGURATION_MAX_TOTAL_LOCAL_REQUESTS_RATE;
import static com.aws.greengrass.shadowmanager.model.Constants.CONFIGURATION_MAX_OUTBOUND_UPDATES_PS_TOPIC;
import static com.aws.greengrass.shadowmanager.model.Constants.CONFIGURATION_NAMED_SHADOWS_TOPIC;
import static com.aws.greengrass.shadowmanager.model.Constants.CONFIGURATION_NUCLEUS_THING_TOPIC;
import static com.aws.greengrass.shadowmanager.model.Constants.CONFIGURATION_RATE_LIMITS_TOPIC;
import static com.aws.greengrass.shadowmanager.model.Constants.CONFIGURATION_SHADOW_DOCUMENTS_TOPIC;
import static com.aws.greengrass.shadowmanager.model.Constants.CONFIGURATION_SYNCHRONIZATION_TOPIC;
import static com.aws.greengrass.shadowmanager.model.Constants.CONFIGURATION_THING_NAME_TOPIC;
import static com.aws.greengrass.shadowmanager.model.Constants.DEFAULT_DOCUMENT_SIZE;
import static com.aws.greengrass.shadowmanager.model.Constants.MAX_SHADOW_DOCUMENT_SIZE;
import static com.aws.greengrass.testcommons.testutilities.ExceptionLogProtector.ignoreExceptionOfType;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anySet;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
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
    private final static String KERNEL_THING = "kernelThing";
    private final static int RATE_LIMIT = 500;
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
    private IotDataPlaneClientWrapper mockIotDataPlaneClientWrapper;
    @Mock
    private CloudDataClient mockCloudDataClient;
    @Mock
    private MqttClient mockMqttClient;
    @Mock
    private GreengrassCoreIPCService mockGreengrassCoreIPCService;

    @Captor
    private ArgumentCaptor<Integer> intObjectCaptor;
    @Captor
    private ArgumentCaptor<MqttClientConnectionEvents> mqttCallbacksCaptor;
    @Captor
    private ArgumentCaptor<CallbackEventManager.OnConnectCallback> mqtOnConnectCallbackCaptor;

    private ShadowManager shadowManager;

    @BeforeEach
    public void setup() {
        serviceFullName = "aws.greengrass.ShadowManager";
        initializeMockedConfig();
        shadowManager = new ShadowManager(config, mockDatabase, mockDao, mockAuthorizationHandlerWrapper,
                mockPubSubClientWrapper, mockInboundRateLimiter, mockDeviceConfiguration, mockSynchronizeHelper,
                mockIotDataPlaneClientWrapper, mockSyncHandler, mockCloudDataClient, mockMqttClient);

        Topic maxDocSizeTopic = Topic.of(context, CONFIGURATION_MAX_DOC_SIZE_LIMIT_B_TOPIC, DEFAULT_DOCUMENT_SIZE);

        lenient().when(config.lookup(CONFIGURATION_CONFIG_KEY, CONFIGURATION_MAX_DOC_SIZE_LIMIT_B_TOPIC))
                .thenReturn(maxDocSizeTopic);
        lenient().when(config.lookupTopics(CONFIGURATION_CONFIG_KEY, CONFIGURATION_SYNCHRONIZATION_TOPIC))
                .thenReturn(mock(Topics.class));
        lenient().when(config.lookupTopics(CONFIGURATION_CONFIG_KEY, CONFIGURATION_RATE_LIMITS_TOPIC))
                .thenReturn(mock(Topics.class));
    }

    @ParameterizedTest
    @ValueSource(ints = {DEFAULT_DOCUMENT_SIZE, MAX_SHADOW_DOCUMENT_SIZE})
    void GIVEN_good_max_doc_size_WHEN_initialize_THEN_updates_max_doc_size_correctly(int maxDocSize) {
        Topic maxDocSizeTopic = Topic.of(context, CONFIGURATION_MAX_DOC_SIZE_LIMIT_B_TOPIC, maxDocSize);
        when(config.lookup(CONFIGURATION_CONFIG_KEY, CONFIGURATION_MAX_DOC_SIZE_LIMIT_B_TOPIC))
                .thenReturn(maxDocSizeTopic);
        shadowManager.install();

        assertFalse(shadowManager.isErrored());
        assertThat(Validator.getMaxShadowDocumentSize(), is(maxDocSize));
        verify(mockDao, times(0)).insertSyncInfoIfNotExists(any(SyncInformation.class));
    }

    @ParameterizedTest
    @ValueSource(ints = {MAX_SHADOW_DOCUMENT_SIZE + 1, -1})
    void GIVEN_bad_max_doc_size_WHEN_initialize_THEN_throws_exception(int maxDocSize, ExtensionContext extensionContext) {
        ignoreExceptionOfType(extensionContext, InvalidConfigurationException.class);
        Topic maxDocSizeTopic = Topic.of(context, CONFIGURATION_MAX_DOC_SIZE_LIMIT_B_TOPIC, maxDocSize);
        when(config.lookup(CONFIGURATION_CONFIG_KEY, CONFIGURATION_MAX_DOC_SIZE_LIMIT_B_TOPIC))
                .thenReturn(maxDocSizeTopic);
        shadowManager.install();
        assertTrue(shadowManager.isErrored());
        verify(mockDao, times(0)).insertSyncInfoIfNotExists(any(SyncInformation.class));
    }

    @Test
    void GIVEN_good_max_outbound_rate_WHEN_initialize_THEN_outbound_rate_updated() throws UnsupportedInputTypeException {
        Topics rateLimitsTopics = Topics.of(context, CONFIGURATION_RATE_LIMITS_TOPIC, null);
        rateLimitsTopics.createLeafChild(CONFIGURATION_MAX_OUTBOUND_UPDATES_PS_TOPIC).withValueChecked(RATE_LIMIT);
        when(config.lookupTopics(CONFIGURATION_CONFIG_KEY, CONFIGURATION_RATE_LIMITS_TOPIC))
                .thenReturn(rateLimitsTopics);

        shadowManager.install();

        assertFalse(shadowManager.isErrored());
        verify(mockInboundRateLimiter, times(0)).setRate(anyInt());
        verify(mockInboundRateLimiter, times(0)).setTotalRate(anyInt());
        verify(mockIotDataPlaneClientWrapper, times(1)).setRate(intObjectCaptor.capture());
        assertThat(intObjectCaptor.getValue(), is(notNullValue()));
        assertThat(intObjectCaptor.getValue(), is(RATE_LIMIT));
    }

    @Test
    void GIVEN_bad_max_outbound_rate_WHEN_initialize_THEN_throws_exception(ExtensionContext extensionContext) throws UnsupportedInputTypeException {
        ignoreExceptionOfType(extensionContext, InvalidConfigurationException.class);
        Topics rateLimitsTopics = Topics.of(context, CONFIGURATION_RATE_LIMITS_TOPIC, null);
        rateLimitsTopics.createLeafChild(CONFIGURATION_MAX_OUTBOUND_UPDATES_PS_TOPIC).withValueChecked(-1);
        when(config.lookupTopics(CONFIGURATION_CONFIG_KEY, CONFIGURATION_RATE_LIMITS_TOPIC))
                .thenReturn(rateLimitsTopics);

        shadowManager.install();

        assertTrue(shadowManager.isErrored());
        verify(mockInboundRateLimiter, times(0)).setRate(anyInt());
        verify(mockInboundRateLimiter, times(0)).setTotalRate(anyInt());
        verify(mockIotDataPlaneClientWrapper, times(0)).setRate(anyInt());
    }

    @Test
    void GIVEN_good_overall_inbound_rate_WHEN_initialize_THEN_updates_overall_inbound_rate_correctly() throws UnsupportedInputTypeException {
        Topics rateLimitsTopics = Topics.of(context, CONFIGURATION_RATE_LIMITS_TOPIC, null);
        rateLimitsTopics.createLeafChild(CONFIGURATION_MAX_TOTAL_LOCAL_REQUESTS_RATE).withValueChecked(RATE_LIMIT);
        when(config.lookupTopics(CONFIGURATION_CONFIG_KEY, CONFIGURATION_RATE_LIMITS_TOPIC))
                .thenReturn(rateLimitsTopics);

        shadowManager.install();

        assertFalse(shadowManager.isErrored());
        verify(mockIotDataPlaneClientWrapper, times(0)).setRate(anyInt());
        verify(mockInboundRateLimiter, times(0)).setRate(anyInt());
        verify(mockInboundRateLimiter, times(1)).setTotalRate(intObjectCaptor.capture());
        assertThat(intObjectCaptor.getValue(), is(notNullValue()));
        assertThat(intObjectCaptor.getValue(), is(RATE_LIMIT));
    }

    @Test
    void GIVEN_bad_overall_inbound_rate_WHEN_initialize_THEN_throws_exception(ExtensionContext extensionContext) throws UnsupportedInputTypeException {
        ignoreExceptionOfType(extensionContext, InvalidConfigurationException.class);
        Topics rateLimitsTopics = Topics.of(context, CONFIGURATION_RATE_LIMITS_TOPIC, null);
        rateLimitsTopics.createLeafChild(CONFIGURATION_MAX_TOTAL_LOCAL_REQUESTS_RATE).withValueChecked(-1);
        when(config.lookupTopics(CONFIGURATION_CONFIG_KEY, CONFIGURATION_RATE_LIMITS_TOPIC))
                .thenReturn(rateLimitsTopics);

        shadowManager.install();

        assertTrue(shadowManager.isErrored());
        verify(mockInboundRateLimiter, times(0)).setRate(anyInt());
        verify(mockInboundRateLimiter, times(0)).setTotalRate(anyInt());
        verify(mockIotDataPlaneClientWrapper, times(0)).setRate(anyInt());
    }

    @Test
    void GIVEN_good_inbound_rate_per_thing_WHEN_initialize_THEN_updates_inbound_rate_per_thing_correctly() throws UnsupportedInputTypeException {
        Topics rateLimitsTopics = Topics.of(context, CONFIGURATION_RATE_LIMITS_TOPIC, null);
        rateLimitsTopics.createLeafChild(CONFIGURATION_MAX_LOCAL_REQUESTS_RATE_PER_THING_TOPIC).withValueChecked(RATE_LIMIT);
        when(config.lookupTopics(CONFIGURATION_CONFIG_KEY, CONFIGURATION_RATE_LIMITS_TOPIC))
                .thenReturn(rateLimitsTopics);

        shadowManager.install();

        assertFalse(shadowManager.isErrored());
        verify(mockInboundRateLimiter, times(0)).setTotalRate(anyInt());
        verify(mockIotDataPlaneClientWrapper, times(0)).setRate(anyInt());
        verify(mockInboundRateLimiter, times(1)).setRate(intObjectCaptor.capture());
        assertThat(intObjectCaptor.getValue(), is(notNullValue()));
        assertThat(intObjectCaptor.getValue(), is(RATE_LIMIT));
    }

    @Test
    void GIVEN_bad_inbound_rate_per_thing_WHEN_initialize_THEN_throws_exception(ExtensionContext extensionContext) throws UnsupportedInputTypeException {
        ignoreExceptionOfType(extensionContext, InvalidConfigurationException.class);
        Topics rateLimitsTopics = Topics.of(context, CONFIGURATION_RATE_LIMITS_TOPIC, null);
        rateLimitsTopics.createLeafChild(CONFIGURATION_MAX_LOCAL_REQUESTS_RATE_PER_THING_TOPIC).withValueChecked(-1);
        when(config.lookupTopics(CONFIGURATION_CONFIG_KEY, CONFIGURATION_RATE_LIMITS_TOPIC))
                .thenReturn(rateLimitsTopics);

        shadowManager.install();

        assertTrue(shadowManager.isErrored());
        verify(mockInboundRateLimiter, times(0)).setRate(anyInt());
        verify(mockInboundRateLimiter, times(0)).setTotalRate(anyInt());
        verify(mockIotDataPlaneClientWrapper, times(0)).setRate(anyInt());
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
        Topics systemConfigTopics = configTopics.createInteriorChild(CONFIGURATION_NUCLEUS_THING_TOPIC);
        systemConfigTopics.createLeafChild(CONFIGURATION_CLASSIC_SHADOW_TOPIC).withValue("true");
        systemConfigTopics.createLeafChild(CONFIGURATION_NAMED_SHADOWS_TOPIC).withValue(Collections.singletonList("boo2"));

        when(thingNameTopic.getOnce()).thenReturn(KERNEL_THING);
        when(config.lookupTopics(CONFIGURATION_CONFIG_KEY, CONFIGURATION_SYNCHRONIZATION_TOPIC))
                .thenReturn(configTopics);
        when(mockDeviceConfiguration.getThingName()).thenReturn(thingNameTopic);
        shadowManager.install();

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
        verify(mockDao, times(6)).insertSyncInfoIfNotExists(any(SyncInformation.class));
    }

    @Test
    void GIVEN_good_sync_configuration_without_nucleus_thing_config_WHEN_initialize_THEN_processes_configuration_correctly() throws UnsupportedInputTypeException {
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
        shadowManager.install();

        verify(thingNameTopic, times(0)).subscribeGeneric(any());
        verify(thingNameTopic, times(1)).remove(any());
        assertFalse(shadowManager.isErrored());
        assertThat(shadowManager.getSyncConfiguration().getSyncConfigurations(),
                containsInAnyOrder(
                        ThingShadowSyncConfiguration.builder().thingName(THING_NAME_A).shadowName("foo").build(),
                        ThingShadowSyncConfiguration.builder().thingName(THING_NAME_A).shadowName("bar").build(),
                        ThingShadowSyncConfiguration.builder().thingName(THING_NAME_B).shadowName("").build(),
                        ThingShadowSyncConfiguration.builder().thingName(THING_NAME_B).shadowName("foo2").build()));
        verify(mockDao, times(4)).insertSyncInfoIfNotExists(any(SyncInformation.class));
    }

    @Test
    void GIVEN_good_sync_configuration_with_only_nucleus_thing_config_WHEN_thing_name_changes_THEN_updates_nucleus_configuration_correctly() throws UnsupportedInputTypeException {
        Topic thingNameTopic = Topic.of(context, DEVICE_PARAM_THING_NAME, KERNEL_THING);
        Topics configTopics = Topics.of(context, CONFIGURATION_SYNCHRONIZATION_TOPIC, null);
        Topics systemConfigTopics = configTopics.createInteriorChild(CONFIGURATION_NUCLEUS_THING_TOPIC);
        systemConfigTopics.createLeafChild(CONFIGURATION_CLASSIC_SHADOW_TOPIC).withValue("true");
        systemConfigTopics.createLeafChild(CONFIGURATION_NAMED_SHADOWS_TOPIC).withValue(Collections.singletonList("boo2"));

        when(config.lookupTopics(CONFIGURATION_CONFIG_KEY, CONFIGURATION_SYNCHRONIZATION_TOPIC))
                .thenReturn(configTopics);
        when(mockDeviceConfiguration.getThingName()).thenReturn(thingNameTopic);
        shadowManager.install();

        assertFalse(shadowManager.isErrored());
        assertThat(shadowManager.getSyncConfiguration().getSyncConfigurations(),
                containsInAnyOrder(
                        ThingShadowSyncConfiguration.builder().thingName(KERNEL_THING).shadowName("").build(),
                        ThingShadowSyncConfiguration.builder().thingName(KERNEL_THING).shadowName("boo2").build()));

        verify(mockDao, times(2)).insertSyncInfoIfNotExists(any(SyncInformation.class));

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
        configTopics.createLeafChild(CONFIGURATION_NUCLEUS_THING_TOPIC).withValueChecked(shadowDocumentsList);
        configTopics.createLeafChild(CONFIGURATION_SHADOW_DOCUMENTS_TOPIC).withValueChecked(null);

        when(config.lookupTopics(CONFIGURATION_CONFIG_KEY, CONFIGURATION_SYNCHRONIZATION_TOPIC))
                .thenReturn(configTopics);
        when(mockDeviceConfiguration.getThingName()).thenReturn(thingNameTopic);
        shadowManager.install();
        assertTrue(shadowManager.isErrored());
        verify(mockDao, times(0)).insertSyncInfoIfNotExists(any(SyncInformation.class));
    }

    @Test
    void GIVEN_bad_field_in_thing_sync_configuration_WHEN_initialize_THEN_service_errors(ExtensionContext extensionContext) throws UnsupportedInputTypeException {
        ignoreExceptionOfType(extensionContext, MismatchedInputException.class);
        ignoreExceptionOfType(extensionContext, InvalidConfigurationException.class);
        Topic maxDocSizeTopic = Topic.of(context, CONFIGURATION_MAX_DOC_SIZE_LIMIT_B_TOPIC, DEFAULT_DOCUMENT_SIZE);
        Topics configTopics = Topics.of(context, CONFIGURATION_SYNCHRONIZATION_TOPIC, null);
        configTopics.createLeafChild(CONFIGURATION_NUCLEUS_THING_TOPIC).withValueChecked(null);
        List<Map<String, Object>> shadowDocumentsList = new ArrayList<>();
        Map<String, Object> thingAMap = new HashMap<>();
        thingAMap.put(CONFIGURATION_THING_NAME_TOPIC, THING_NAME_A);
        thingAMap.put(CONFIGURATION_CLASSIC_SHADOW_TOPIC, false);
        thingAMap.put(CONFIGURATION_NAMED_SHADOWS_TOPIC, "foo");
        shadowDocumentsList.add(thingAMap);
        configTopics.createLeafChild(CONFIGURATION_SHADOW_DOCUMENTS_TOPIC).withValueChecked(shadowDocumentsList);

        Topic thingNameTopic = Topic.of(context, DEVICE_PARAM_THING_NAME, KERNEL_THING);
        when(mockDeviceConfiguration.getThingName()).thenReturn(thingNameTopic);
        when(config.lookup(CONFIGURATION_CONFIG_KEY, CONFIGURATION_MAX_DOC_SIZE_LIMIT_B_TOPIC))
                .thenReturn(maxDocSizeTopic);
        when(config.lookupTopics(CONFIGURATION_CONFIG_KEY, CONFIGURATION_SYNCHRONIZATION_TOPIC))
                .thenReturn(configTopics);
        shadowManager.install();
        assertTrue(shadowManager.isErrored());
        verify(mockDao, times(0)).insertSyncInfoIfNotExists(any(SyncInformation.class));
    }

    @Test
    void GIVEN_bad_type_of_thing_sync_configuration_WHEN_initialize_THEN_service_errors(ExtensionContext extensionContext) throws UnsupportedInputTypeException {
        ignoreExceptionOfType(extensionContext, InvalidConfigurationException.class);
        Topic maxDocSizeTopic = Topic.of(context, CONFIGURATION_MAX_DOC_SIZE_LIMIT_B_TOPIC, DEFAULT_DOCUMENT_SIZE);
        Topics configTopics = Topics.of(context, CONFIGURATION_SYNCHRONIZATION_TOPIC, null);
        configTopics.createLeafChild(CONFIGURATION_NUCLEUS_THING_TOPIC).withValueChecked(null);
        Topics shadowDocumentsTopics = configTopics.createInteriorChild(CONFIGURATION_SHADOW_DOCUMENTS_TOPIC);
        shadowDocumentsTopics.createLeafChild(CONFIGURATION_CLASSIC_SHADOW_TOPIC).withValue("true");
        shadowDocumentsTopics.createLeafChild(CONFIGURATION_NAMED_SHADOWS_TOPIC).withValue(Collections.singletonList("boo2"));
        shadowDocumentsTopics.createLeafChild(CONFIGURATION_THING_NAME_TOPIC).withValue(THING_NAME_A);

        when(config.lookup(CONFIGURATION_CONFIG_KEY, CONFIGURATION_MAX_DOC_SIZE_LIMIT_B_TOPIC))
                .thenReturn(maxDocSizeTopic);
        when(config.lookupTopics(CONFIGURATION_CONFIG_KEY, CONFIGURATION_SYNCHRONIZATION_TOPIC))
                .thenReturn(configTopics);
        shadowManager.install();
        assertTrue(shadowManager.isErrored());
        verify(mockDao, times(0)).insertSyncInfoIfNotExists(any(SyncInformation.class));
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
        shadowManager.install();
        assertTrue(shadowManager.isErrored());
        verify(mockDao, times(0)).insertSyncInfoIfNotExists(any(SyncInformation.class));
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

        shadowManager.install();
        assertTrue(shadowManager.isErrored());
        verify(mockDao, times(0)).insertSyncInfoIfNotExists(any(SyncInformation.class));
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
    }

    @Test
    void GIVEN_shadow_manager_WHEN_startSyncHandler_THEN_starts_sync_handler_and_unsubscribes() {
        shadowManager.setSyncConfiguration(ShadowSyncConfiguration.builder().syncConfigurations(new HashSet<>()).build());
        shadowManager.getSyncConfiguration().getSyncConfigurations().add(mock(ThingShadowSyncConfiguration.class));
        when(mockMqttClient.connected()).thenReturn(true);
        shadowManager.startSyncingShadows();

        verify(mockCloudDataClient, times(1)).updateSubscriptions(anySet());
        verify(mockSyncHandler, times(1)).start(any(SyncContext.class), anyInt());
    }

    @Test
    void GIVEN_shadow_manager_db_WHEN_shutdown_throws_io_exception_THEN_catches_exception(ExtensionContext extensionContext) throws IOException {
        ignoreExceptionOfType(extensionContext, IOException.class);
        doThrow(IOException.class).when(mockDatabase).close();
        assertDoesNotThrow(() -> shadowManager.shutdown());
    }
}
