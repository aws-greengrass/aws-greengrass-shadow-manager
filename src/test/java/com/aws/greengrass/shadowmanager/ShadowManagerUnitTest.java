/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.shadowmanager;

import com.aws.greengrass.config.Topic;
import com.aws.greengrass.config.Topics;
import com.aws.greengrass.config.UnsupportedInputTypeException;
import com.aws.greengrass.config.WhatHappened;
import com.aws.greengrass.deployment.DeviceConfiguration;
import com.aws.greengrass.shadowmanager.exception.InvalidConfigurationException;
import com.aws.greengrass.shadowmanager.ipc.PubSubClientWrapper;
import com.aws.greengrass.shadowmanager.ipc.Validator;
import com.aws.greengrass.testcommons.testutilities.GGExtension;
import com.aws.greengrass.testcommons.testutilities.GGServiceTestUtil;
import com.fasterxml.jackson.databind.exc.UnrecognizedPropertyException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.aws.greengrass.componentmanager.KernelConfigResolver.CONFIGURATION_CONFIG_KEY;
import static com.aws.greengrass.deployment.DeviceConfiguration.DEVICE_PARAM_THING_NAME;
import static com.aws.greengrass.shadowmanager.model.Constants.CONFIGURATION_CLASSIC_SHADOW_TOPIC;
import static com.aws.greengrass.shadowmanager.model.Constants.CONFIGURATION_MAX_DISK_UTILIZATION_MB_TOPIC;
import static com.aws.greengrass.shadowmanager.model.Constants.CONFIGURATION_MAX_DOC_SIZE_LIMIT_B_TOPIC;
import static com.aws.greengrass.shadowmanager.model.Constants.CONFIGURATION_MAX_OUTBOUND_UPDATES_PS_TOPIC;
import static com.aws.greengrass.shadowmanager.model.Constants.CONFIGURATION_NAMED_SHADOWS_TOPIC;
import static com.aws.greengrass.shadowmanager.model.Constants.CONFIGURATION_NUCLEUS_THING_TOPIC;
import static com.aws.greengrass.shadowmanager.model.Constants.CONFIGURATION_PROVIDE_SYNC_STATUS_TOPIC;
import static com.aws.greengrass.shadowmanager.model.Constants.CONFIGURATION_SHADOW_DOCUMENTS_TOPIC;
import static com.aws.greengrass.shadowmanager.model.Constants.CONFIGURATION_SYNCHRONIZATION_TOPIC;
import static com.aws.greengrass.shadowmanager.model.Constants.CONFIGURATION_THING_NAME_TOPIC;
import static com.aws.greengrass.shadowmanager.model.Constants.DEFAULT_DISK_UTILIZATION_SIZE_B;
import static com.aws.greengrass.shadowmanager.model.Constants.DEFAULT_DOCUMENT_SIZE;
import static com.aws.greengrass.shadowmanager.model.Constants.MAX_SHADOW_DOCUMENT_SIZE;
import static com.aws.greengrass.testcommons.testutilities.ExceptionLogProtector.ignoreExceptionOfType;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith({MockitoExtension.class, GGExtension.class})
class ShadowManagerUnitTest extends GGServiceTestUtil {
    private final static String THING_NAME_A = "thingNameA";
    private final static String THING_NAME_B = "thingNameB";
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
    private DeviceConfiguration mockDeviceConfiguration;
    @Captor
    private ArgumentCaptor<Integer> intObjectCaptor;


    @BeforeEach
    public void setup() {
        serviceFullName = "aws.greengrass.ShadowManager";
        initializeMockedConfig();
    }

    @ParameterizedTest
    @ValueSource(ints = {DEFAULT_DISK_UTILIZATION_SIZE_B, DEFAULT_DISK_UTILIZATION_SIZE_B + 1})
    void GIVEN_good_max_disk_utilization_WHEN_initialize_THEN_updates_max_disk_utilization_correctly(int maxDiskUtilizationMB) {
        Topic maxDiskUtilizationMBTopic = Topic.of(context, CONFIGURATION_MAX_DISK_UTILIZATION_MB_TOPIC, maxDiskUtilizationMB);
        Topic maxDocSizeTopic = Topic.of(context, CONFIGURATION_MAX_DOC_SIZE_LIMIT_B_TOPIC, DEFAULT_DOCUMENT_SIZE);
        when(config.lookup(CONFIGURATION_CONFIG_KEY, CONFIGURATION_MAX_DISK_UTILIZATION_MB_TOPIC))
                .thenReturn(maxDiskUtilizationMBTopic);
        when(config.lookup(CONFIGURATION_CONFIG_KEY, CONFIGURATION_MAX_DOC_SIZE_LIMIT_B_TOPIC))
                .thenReturn(maxDocSizeTopic);
        when(config.lookupTopics(CONFIGURATION_CONFIG_KEY, CONFIGURATION_SYNCHRONIZATION_TOPIC))
                .thenReturn(mock(Topics.class));
        ShadowManager shadowManager = new ShadowManager(config, mockDatabase, mockDao, mockAuthorizationHandlerWrapper, mockPubSubClientWrapper, mockDeviceConfiguration);
        shadowManager.install();
        assertFalse(shadowManager.isErrored());
        verify(mockDatabase, times(1)).setMaxDiskUtilization(intObjectCaptor.capture());
        assertThat(intObjectCaptor.getValue(), is(notNullValue()));
        assertThat(intObjectCaptor.getValue(), is(maxDiskUtilizationMB));
    }

    @ParameterizedTest
    @ValueSource(ints = {-1})
    void GIVEN_bad_max_disk_utilization_WHEN_initialize_THEN_updates_max_disk_utilization_correctly(int maxDiskUtilizationMB, ExtensionContext extensionContext) {
        ignoreExceptionOfType(extensionContext, InvalidConfigurationException.class);
        Topic maxDiskUtilizationMBTopic = Topic.of(context, CONFIGURATION_MAX_DISK_UTILIZATION_MB_TOPIC, maxDiskUtilizationMB);
        Topic maxDocSizeTopic = Topic.of(context, CONFIGURATION_MAX_DOC_SIZE_LIMIT_B_TOPIC, DEFAULT_DOCUMENT_SIZE);
        when(config.lookup(CONFIGURATION_CONFIG_KEY, CONFIGURATION_MAX_DISK_UTILIZATION_MB_TOPIC))
                .thenReturn(maxDiskUtilizationMBTopic);
        when(config.lookup(CONFIGURATION_CONFIG_KEY, CONFIGURATION_MAX_DOC_SIZE_LIMIT_B_TOPIC))
                .thenReturn(maxDocSizeTopic);
        when(config.lookupTopics(CONFIGURATION_CONFIG_KEY, CONFIGURATION_SYNCHRONIZATION_TOPIC))
                .thenReturn(mock(Topics.class));
        ShadowManager shadowManager = new ShadowManager(config, mockDatabase, mockDao, mockAuthorizationHandlerWrapper, mockPubSubClientWrapper, mockDeviceConfiguration);
        shadowManager.install();
        assertTrue(shadowManager.isErrored());
    }

    @ParameterizedTest
    @ValueSource(ints = {DEFAULT_DOCUMENT_SIZE, MAX_SHADOW_DOCUMENT_SIZE})
    void GIVEN_good_max_doc_size_WHEN_initialize_THEN_updates_max_doc_size_correctly(int maxDocSize) {
        Topic maxDiskUtilizationMBTopic = Topic.of(context, CONFIGURATION_MAX_DISK_UTILIZATION_MB_TOPIC, DEFAULT_DISK_UTILIZATION_SIZE_B);
        Topic maxDocSizeTopic = Topic.of(context, CONFIGURATION_MAX_DOC_SIZE_LIMIT_B_TOPIC, maxDocSize);
        when(config.lookup(CONFIGURATION_CONFIG_KEY, CONFIGURATION_MAX_DISK_UTILIZATION_MB_TOPIC))
                .thenReturn(maxDiskUtilizationMBTopic);
        when(config.lookup(CONFIGURATION_CONFIG_KEY, CONFIGURATION_MAX_DOC_SIZE_LIMIT_B_TOPIC))
                .thenReturn(maxDocSizeTopic);
        when(config.lookupTopics(CONFIGURATION_CONFIG_KEY, CONFIGURATION_SYNCHRONIZATION_TOPIC))
                .thenReturn(mock(Topics.class));
        ShadowManager shadowManager = new ShadowManager(config, mockDatabase, mockDao, mockAuthorizationHandlerWrapper, mockPubSubClientWrapper, mockDeviceConfiguration);
        shadowManager.install();

        assertFalse(shadowManager.isErrored());
        assertThat(Validator.getMaxShadowDocumentSize(), is(maxDocSize));
    }

    @ParameterizedTest
    @ValueSource(ints = {MAX_SHADOW_DOCUMENT_SIZE + 1, -1})
    void GIVEN_bad_max_doc_size_WHEN_initialize_THEN_throws_exception(int maxDocSize, ExtensionContext extensionContext) {
        ignoreExceptionOfType(extensionContext, InvalidConfigurationException.class);
        Topic maxDiskUtilizationMBTopic = Topic.of(context, CONFIGURATION_MAX_DISK_UTILIZATION_MB_TOPIC, DEFAULT_DISK_UTILIZATION_SIZE_B);
        Topic maxDocSizeTopic = Topic.of(context, CONFIGURATION_MAX_DOC_SIZE_LIMIT_B_TOPIC, maxDocSize);
        when(config.lookup(CONFIGURATION_CONFIG_KEY, CONFIGURATION_MAX_DISK_UTILIZATION_MB_TOPIC))
                .thenReturn(maxDiskUtilizationMBTopic);
        when(config.lookup(CONFIGURATION_CONFIG_KEY, CONFIGURATION_MAX_DOC_SIZE_LIMIT_B_TOPIC))
                .thenReturn(maxDocSizeTopic);
        when(config.lookupTopics(CONFIGURATION_CONFIG_KEY, CONFIGURATION_SYNCHRONIZATION_TOPIC))
                .thenReturn(mock(Topics.class));
        ShadowManager shadowManager = new ShadowManager(config, mockDatabase, mockDao, mockAuthorizationHandlerWrapper, mockPubSubClientWrapper, mockDeviceConfiguration);
        shadowManager.install();
        assertTrue(shadowManager.isErrored());
    }

    @Test
    void GIVEN_good_sync_configuration_WHEN_initialize_THEN_processes_configuration_correctly() throws UnsupportedInputTypeException {
        Topic thingNameTopic = mock(Topic.class);
        Topic maxDiskUtilizationMBTopic = Topic.of(context, CONFIGURATION_MAX_DISK_UTILIZATION_MB_TOPIC, DEFAULT_DISK_UTILIZATION_SIZE_B);
        Topic maxDocSizeTopic = Topic.of(context, CONFIGURATION_MAX_DOC_SIZE_LIMIT_B_TOPIC, DEFAULT_DOCUMENT_SIZE);
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
        configTopics.createLeafChild(CONFIGURATION_MAX_OUTBOUND_UPDATES_PS_TOPIC).withValueChecked(500);
        configTopics.createLeafChild(CONFIGURATION_PROVIDE_SYNC_STATUS_TOPIC).withValueChecked(true);
        Topics systemConfigTopics = configTopics.createInteriorChild(CONFIGURATION_NUCLEUS_THING_TOPIC);
        systemConfigTopics.createLeafChild(CONFIGURATION_CLASSIC_SHADOW_TOPIC).withValue("true");
        systemConfigTopics.createLeafChild(CONFIGURATION_NAMED_SHADOWS_TOPIC).withValue(Collections.singletonList("boo2"));

        when(thingNameTopic.getOnce()).thenReturn(KERNEL_THING);
        when(config.lookup(CONFIGURATION_CONFIG_KEY, CONFIGURATION_MAX_DISK_UTILIZATION_MB_TOPIC))
                .thenReturn(maxDiskUtilizationMBTopic);
        when(config.lookup(CONFIGURATION_CONFIG_KEY, CONFIGURATION_MAX_DOC_SIZE_LIMIT_B_TOPIC))
                .thenReturn(maxDocSizeTopic);
        when(config.lookupTopics(CONFIGURATION_CONFIG_KEY, CONFIGURATION_SYNCHRONIZATION_TOPIC))
                .thenReturn(configTopics);
        when(mockDeviceConfiguration.getThingName()).thenReturn(thingNameTopic);
        ShadowManager shadowManager = new ShadowManager(config, mockDatabase, mockDao, mockAuthorizationHandlerWrapper, mockPubSubClientWrapper, mockDeviceConfiguration);
        shadowManager.install();

        verify(thingNameTopic, times(1)).subscribeGeneric(any());
        verify(thingNameTopic, times(0)).remove(any());
        assertFalse(shadowManager.isErrored());
        assertThat(shadowManager.getSyncConfiguration().getSyncConfigurationList().size(), is(3));
        shadowManager.getSyncConfiguration().getSyncConfigurationList().forEach(thingShadowSyncConfiguration -> {
            if (KERNEL_THING.equals(thingShadowSyncConfiguration.getThingName())) {
                assertThat(thingShadowSyncConfiguration.getSyncNamedShadows(), is(Collections.singletonList("boo2")));
            } else if (THING_NAME_A.equals(thingShadowSyncConfiguration.getThingName())) {
                assertThat(thingShadowSyncConfiguration.getSyncNamedShadows(), is(Arrays.asList("foo", "bar")));
            } else if (THING_NAME_B.equals(thingShadowSyncConfiguration.getThingName())) {
                assertThat(thingShadowSyncConfiguration.getSyncNamedShadows(), is(Collections.singletonList("foo2")));
            } else {
                fail("Encountered unknown thing in sync configurations list: " + thingShadowSyncConfiguration.getThingName());
            }
        });
        assertThat(shadowManager.getSyncConfiguration().getMaxOutboundSyncUpdatesPerSecond(), is(500));
        assertThat(shadowManager.getSyncConfiguration().isProvideSyncStatus(), is(true));
    }

    @Test
    void GIVEN_good_sync_configuration_without_nucleus_thing_config_WHEN_initialize_THEN_processes_configuration_correctly() throws UnsupportedInputTypeException {
        Topic thingNameTopic = mock(Topic.class);
        Topic maxDiskUtilizationMBTopic = Topic.of(context, CONFIGURATION_MAX_DISK_UTILIZATION_MB_TOPIC, DEFAULT_DISK_UTILIZATION_SIZE_B);
        Topic maxDocSizeTopic = Topic.of(context, CONFIGURATION_MAX_DOC_SIZE_LIMIT_B_TOPIC, DEFAULT_DOCUMENT_SIZE);
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
        configTopics.createLeafChild(CONFIGURATION_MAX_OUTBOUND_UPDATES_PS_TOPIC).withValueChecked(500);
        configTopics.createLeafChild(CONFIGURATION_PROVIDE_SYNC_STATUS_TOPIC).withValueChecked(true);
        Topics systemConfigTopics = configTopics.createInteriorChild(CONFIGURATION_NUCLEUS_THING_TOPIC);
        systemConfigTopics.createLeafChild(CONFIGURATION_CLASSIC_SHADOW_TOPIC).withValue("true");
        systemConfigTopics.createLeafChild(CONFIGURATION_NAMED_SHADOWS_TOPIC).withValue(Collections.singletonList("boo2"));

        when(thingNameTopic.getOnce()).thenReturn(KERNEL_THING);
        when(config.lookup(CONFIGURATION_CONFIG_KEY, CONFIGURATION_MAX_DISK_UTILIZATION_MB_TOPIC))
                .thenReturn(maxDiskUtilizationMBTopic);
        when(config.lookup(CONFIGURATION_CONFIG_KEY, CONFIGURATION_MAX_DOC_SIZE_LIMIT_B_TOPIC))
                .thenReturn(maxDocSizeTopic);
        when(config.lookupTopics(CONFIGURATION_CONFIG_KEY, CONFIGURATION_SYNCHRONIZATION_TOPIC))
                .thenReturn(configTopics);
        when(mockDeviceConfiguration.getThingName()).thenReturn(thingNameTopic);
        ShadowManager shadowManager = new ShadowManager(config, mockDatabase, mockDao, mockAuthorizationHandlerWrapper, mockPubSubClientWrapper, mockDeviceConfiguration);
        shadowManager.install();

        verify(thingNameTopic, times(1)).subscribeGeneric(any());
        verify(thingNameTopic, times(0)).remove(any());
        assertFalse(shadowManager.isErrored());
        assertThat(shadowManager.getSyncConfiguration().getSyncConfigurationList().size(), is(3));
        shadowManager.getSyncConfiguration().getSyncConfigurationList().forEach(thingShadowSyncConfiguration -> {
            if (KERNEL_THING.equals(thingShadowSyncConfiguration.getThingName())) {
                assertThat(thingShadowSyncConfiguration.getSyncNamedShadows(), is(Collections.singletonList("boo2")));
            } else if (THING_NAME_A.equals(thingShadowSyncConfiguration.getThingName())) {
                assertThat(thingShadowSyncConfiguration.getSyncNamedShadows(), is(Arrays.asList("foo", "bar")));
            } else if (THING_NAME_B.equals(thingShadowSyncConfiguration.getThingName())) {
                assertThat(thingShadowSyncConfiguration.getSyncNamedShadows(), is(Collections.singletonList("foo2")));
            } else {
                fail("Encountered unknown thing in sync configurations list: " + thingShadowSyncConfiguration.getThingName());
            }
        });
        assertThat(shadowManager.getSyncConfiguration().getMaxOutboundSyncUpdatesPerSecond(), is(500));
        assertThat(shadowManager.getSyncConfiguration().isProvideSyncStatus(), is(true));
    }

    @Test
    void GIVEN_good_sync_configuration_with_only_nucleus_thing_config_WHEN_thing_name_changes_THEN_updates_nucleus_configuration_correctly() throws UnsupportedInputTypeException {
        Topic thingNameTopic = Topic.of(context, DEVICE_PARAM_THING_NAME, KERNEL_THING);
        Topic maxDiskUtilizationMBTopic = Topic.of(context, CONFIGURATION_MAX_DISK_UTILIZATION_MB_TOPIC, DEFAULT_DISK_UTILIZATION_SIZE_B);
        Topic maxDocSizeTopic = Topic.of(context, CONFIGURATION_MAX_DOC_SIZE_LIMIT_B_TOPIC, DEFAULT_DOCUMENT_SIZE);
        Topics configTopics = Topics.of(context, CONFIGURATION_SYNCHRONIZATION_TOPIC, null);
        configTopics.createLeafChild(CONFIGURATION_MAX_OUTBOUND_UPDATES_PS_TOPIC).withValueChecked(500);
        configTopics.createLeafChild(CONFIGURATION_PROVIDE_SYNC_STATUS_TOPIC).withValueChecked(true);
        Topics systemConfigTopics = configTopics.createInteriorChild(CONFIGURATION_NUCLEUS_THING_TOPIC);
        systemConfigTopics.createLeafChild(CONFIGURATION_CLASSIC_SHADOW_TOPIC).withValue("true");
        systemConfigTopics.createLeafChild(CONFIGURATION_NAMED_SHADOWS_TOPIC).withValue(Collections.singletonList("boo2"));

        when(config.lookup(CONFIGURATION_CONFIG_KEY, CONFIGURATION_MAX_DISK_UTILIZATION_MB_TOPIC))
                .thenReturn(maxDiskUtilizationMBTopic);
        when(config.lookup(CONFIGURATION_CONFIG_KEY, CONFIGURATION_MAX_DOC_SIZE_LIMIT_B_TOPIC))
                .thenReturn(maxDocSizeTopic);
        when(config.lookupTopics(CONFIGURATION_CONFIG_KEY, CONFIGURATION_SYNCHRONIZATION_TOPIC))
                .thenReturn(configTopics);
        when(mockDeviceConfiguration.getThingName()).thenReturn(thingNameTopic);
        ShadowManager shadowManager = new ShadowManager(config, mockDatabase, mockDao, mockAuthorizationHandlerWrapper, mockPubSubClientWrapper, mockDeviceConfiguration);
        shadowManager.install();

        assertFalse(shadowManager.isErrored());
        assertThat(shadowManager.getSyncConfiguration().getSyncConfigurationList().size(), is(1));
        shadowManager.getSyncConfiguration().getSyncConfigurationList().forEach(thingShadowSyncConfiguration -> {
            if (KERNEL_THING.equals(thingShadowSyncConfiguration.getThingName())) {
                assertThat(thingShadowSyncConfiguration.getSyncNamedShadows(), is(Collections.singletonList("boo2")));
            } else {
                fail("Encountered unknown thing in sync configurations list: " + thingShadowSyncConfiguration.getThingName());
            }
        });
        assertThat(shadowManager.getSyncConfiguration().getMaxOutboundSyncUpdatesPerSecond(), is(500));
        assertThat(shadowManager.getSyncConfiguration().isProvideSyncStatus(), is(true));

        String newThingName = "newKernelThing";
        thingNameTopic = thingNameTopic.withValue(newThingName);
        shadowManager.handleDeviceThingNameChange(WhatHappened.changed, thingNameTopic);
        assertThat(shadowManager.getSyncConfiguration().getSyncConfigurationList().size(), is(1));
        shadowManager.getSyncConfiguration().getSyncConfigurationList().forEach(thingShadowSyncConfiguration -> {
            if (newThingName.equals(thingShadowSyncConfiguration.getThingName())) {
                assertThat(thingShadowSyncConfiguration.getSyncNamedShadows(), is(Collections.singletonList("boo2")));
            } else {
                fail("Encountered unknown thing in sync configurations list: " + thingShadowSyncConfiguration.getThingName());
            }
        });

    }

    @Test
    void GIVEN_bad_field_in_nucleus_sync_configuration_WHEN_initialize_THEN_service_errors(ExtensionContext extensionContext) throws UnsupportedInputTypeException {
        ignoreExceptionOfType(extensionContext, InvalidConfigurationException.class);
        Topic thingNameTopic = Topic.of(context, DEVICE_PARAM_THING_NAME, KERNEL_THING);
        Topic maxDiskUtilizationMBTopic = Topic.of(context, CONFIGURATION_MAX_DISK_UTILIZATION_MB_TOPIC, DEFAULT_DISK_UTILIZATION_SIZE_B);
        Topic maxDocSizeTopic = Topic.of(context, CONFIGURATION_MAX_DOC_SIZE_LIMIT_B_TOPIC, DEFAULT_DOCUMENT_SIZE);
        Topics configTopics = Topics.of(context, CONFIGURATION_SYNCHRONIZATION_TOPIC, null);
        configTopics.createLeafChild(CONFIGURATION_SHADOW_DOCUMENTS_TOPIC).withValueChecked(null);
        Topics systemConfigTopics = configTopics.createInteriorChild(CONFIGURATION_NUCLEUS_THING_TOPIC);
        systemConfigTopics.createLeafChild(CONFIGURATION_CLASSIC_SHADOW_TOPIC).withValue("true");
        systemConfigTopics.createLeafChild(CONFIGURATION_NAMED_SHADOWS_TOPIC).withValue(Collections.singletonList("boo2"));
        systemConfigTopics.createLeafChild("SomeBadKey").withValue("SomeBadValue");

        when(config.lookup(CONFIGURATION_CONFIG_KEY, CONFIGURATION_MAX_DISK_UTILIZATION_MB_TOPIC))
                .thenReturn(maxDiskUtilizationMBTopic);
        when(config.lookup(CONFIGURATION_CONFIG_KEY, CONFIGURATION_MAX_DOC_SIZE_LIMIT_B_TOPIC))
                .thenReturn(maxDocSizeTopic);
        when(config.lookupTopics(CONFIGURATION_CONFIG_KEY, CONFIGURATION_SYNCHRONIZATION_TOPIC))
                .thenReturn(configTopics);
        when(mockDeviceConfiguration.getThingName()).thenReturn(thingNameTopic);
        ShadowManager shadowManager = new ShadowManager(config, mockDatabase, mockDao, mockAuthorizationHandlerWrapper, mockPubSubClientWrapper, mockDeviceConfiguration);
        shadowManager.install();
        assertTrue(shadowManager.isErrored());
    }

    @Test
    void GIVEN_bad_type_of_nucleus_sync_configuration_WHEN_initialize_THEN_service_errors(ExtensionContext extensionContext) throws UnsupportedInputTypeException {
        ignoreExceptionOfType(extensionContext, InvalidConfigurationException.class);
        Topic thingNameTopic = Topic.of(context, DEVICE_PARAM_THING_NAME, KERNEL_THING);
        Topic maxDiskUtilizationMBTopic = Topic.of(context, CONFIGURATION_MAX_DISK_UTILIZATION_MB_TOPIC, DEFAULT_DISK_UTILIZATION_SIZE_B);
        Topic maxDocSizeTopic = Topic.of(context, CONFIGURATION_MAX_DOC_SIZE_LIMIT_B_TOPIC, DEFAULT_DOCUMENT_SIZE);
        Topics configTopics = Topics.of(context, CONFIGURATION_SYNCHRONIZATION_TOPIC, null);
        List<Map<String, Object>> shadowDocumentsList = new ArrayList<>();
        Map<String, Object> thingAMap = new HashMap<>();
        thingAMap.put(CONFIGURATION_CLASSIC_SHADOW_TOPIC, false);
        thingAMap.put(CONFIGURATION_NAMED_SHADOWS_TOPIC, Arrays.asList("foo", "bar"));
        shadowDocumentsList.add(thingAMap);
        configTopics.createLeafChild(CONFIGURATION_NUCLEUS_THING_TOPIC).withValueChecked(shadowDocumentsList);
        configTopics.createLeafChild(CONFIGURATION_SHADOW_DOCUMENTS_TOPIC).withValueChecked(null);

        when(config.lookup(CONFIGURATION_CONFIG_KEY, CONFIGURATION_MAX_DISK_UTILIZATION_MB_TOPIC))
                .thenReturn(maxDiskUtilizationMBTopic);
        when(config.lookup(CONFIGURATION_CONFIG_KEY, CONFIGURATION_MAX_DOC_SIZE_LIMIT_B_TOPIC))
                .thenReturn(maxDocSizeTopic);
        when(config.lookupTopics(CONFIGURATION_CONFIG_KEY, CONFIGURATION_SYNCHRONIZATION_TOPIC))
                .thenReturn(configTopics);
        when(mockDeviceConfiguration.getThingName()).thenReturn(thingNameTopic);
        ShadowManager shadowManager = new ShadowManager(config, mockDatabase, mockDao, mockAuthorizationHandlerWrapper, mockPubSubClientWrapper, mockDeviceConfiguration);
        shadowManager.install();
        assertTrue(shadowManager.isErrored());
    }

    @Test
    void GIVEN_bad_field_in_thing_sync_configuration_WHEN_initialize_THEN_service_errors(ExtensionContext extensionContext) throws UnsupportedInputTypeException {
        ignoreExceptionOfType(extensionContext, UnrecognizedPropertyException.class);
        ignoreExceptionOfType(extensionContext, InvalidConfigurationException.class);
        Topic maxDiskUtilizationMBTopic = Topic.of(context, CONFIGURATION_MAX_DISK_UTILIZATION_MB_TOPIC, DEFAULT_DISK_UTILIZATION_SIZE_B);
        Topic maxDocSizeTopic = Topic.of(context, CONFIGURATION_MAX_DOC_SIZE_LIMIT_B_TOPIC, DEFAULT_DOCUMENT_SIZE);
        Topics configTopics = Topics.of(context, CONFIGURATION_SYNCHRONIZATION_TOPIC, null);
        configTopics.createLeafChild(CONFIGURATION_NUCLEUS_THING_TOPIC).withValueChecked(null);
        List<Map<String, Object>> shadowDocumentsList = new ArrayList<>();
        Map<String, Object> thingAMap = new HashMap<>();
        thingAMap.put(CONFIGURATION_THING_NAME_TOPIC, THING_NAME_A);
        thingAMap.put(CONFIGURATION_CLASSIC_SHADOW_TOPIC, false);
        thingAMap.put(CONFIGURATION_NAMED_SHADOWS_TOPIC, Arrays.asList("foo", "bar"));
        thingAMap.put("SomeBadKey", "SomeBadValue");
        shadowDocumentsList.add(thingAMap);
        configTopics.createLeafChild(CONFIGURATION_SHADOW_DOCUMENTS_TOPIC).withValueChecked(shadowDocumentsList);

        when(config.lookup(CONFIGURATION_CONFIG_KEY, CONFIGURATION_MAX_DISK_UTILIZATION_MB_TOPIC))
                .thenReturn(maxDiskUtilizationMBTopic);
        when(config.lookup(CONFIGURATION_CONFIG_KEY, CONFIGURATION_MAX_DOC_SIZE_LIMIT_B_TOPIC))
                .thenReturn(maxDocSizeTopic);
        when(config.lookupTopics(CONFIGURATION_CONFIG_KEY, CONFIGURATION_SYNCHRONIZATION_TOPIC))
                .thenReturn(configTopics);
        ShadowManager shadowManager = new ShadowManager(config, mockDatabase, mockDao, mockAuthorizationHandlerWrapper, mockPubSubClientWrapper, mockDeviceConfiguration);
        shadowManager.install();
        assertTrue(shadowManager.isErrored());
    }

    @Test
    void GIVEN_bad_type_of_thing_sync_configuration_WHEN_initialize_THEN_service_errors(ExtensionContext extensionContext) throws UnsupportedInputTypeException {
        ignoreExceptionOfType(extensionContext, InvalidConfigurationException.class);
        Topic maxDiskUtilizationMBTopic = Topic.of(context, CONFIGURATION_MAX_DISK_UTILIZATION_MB_TOPIC, DEFAULT_DISK_UTILIZATION_SIZE_B);
        Topic maxDocSizeTopic = Topic.of(context, CONFIGURATION_MAX_DOC_SIZE_LIMIT_B_TOPIC, DEFAULT_DOCUMENT_SIZE);
        Topics configTopics = Topics.of(context, CONFIGURATION_SYNCHRONIZATION_TOPIC, null);
        configTopics.createLeafChild(CONFIGURATION_NUCLEUS_THING_TOPIC).withValueChecked(null);
        Topics shadowDocumentsTopics = configTopics.createInteriorChild(CONFIGURATION_SHADOW_DOCUMENTS_TOPIC);
        shadowDocumentsTopics.createLeafChild(CONFIGURATION_CLASSIC_SHADOW_TOPIC).withValue("true");
        shadowDocumentsTopics.createLeafChild(CONFIGURATION_NAMED_SHADOWS_TOPIC).withValue(Collections.singletonList("boo2"));
        shadowDocumentsTopics.createLeafChild(CONFIGURATION_THING_NAME_TOPIC).withValue(THING_NAME_A);

        when(config.lookup(CONFIGURATION_CONFIG_KEY, CONFIGURATION_MAX_DISK_UTILIZATION_MB_TOPIC))
                .thenReturn(maxDiskUtilizationMBTopic);
        when(config.lookup(CONFIGURATION_CONFIG_KEY, CONFIGURATION_MAX_DOC_SIZE_LIMIT_B_TOPIC))
                .thenReturn(maxDocSizeTopic);
        when(config.lookupTopics(CONFIGURATION_CONFIG_KEY, CONFIGURATION_SYNCHRONIZATION_TOPIC))
                .thenReturn(configTopics);
        ShadowManager shadowManager = new ShadowManager(config, mockDatabase, mockDao, mockAuthorizationHandlerWrapper, mockPubSubClientWrapper, mockDeviceConfiguration);
        shadowManager.install();
        assertTrue(shadowManager.isErrored());
    }

    @Test
    void GIVEN_bad_max_outbound_updates_ps_WHEN_initialize_THEN_service_errors(ExtensionContext extensionContext) throws UnsupportedInputTypeException {
        ignoreExceptionOfType(extensionContext, InvalidConfigurationException.class);
        Topic maxDiskUtilizationMBTopic = Topic.of(context, CONFIGURATION_MAX_DISK_UTILIZATION_MB_TOPIC, DEFAULT_DISK_UTILIZATION_SIZE_B);
        Topic maxDocSizeTopic = Topic.of(context, CONFIGURATION_MAX_DOC_SIZE_LIMIT_B_TOPIC, DEFAULT_DOCUMENT_SIZE);
        Topics configTopics = Topics.of(context, CONFIGURATION_SYNCHRONIZATION_TOPIC, null);
        configTopics.createLeafChild(CONFIGURATION_NUCLEUS_THING_TOPIC).withValueChecked(null);
        configTopics.createLeafChild(CONFIGURATION_MAX_OUTBOUND_UPDATES_PS_TOPIC).withValueChecked(-1);


        when(config.lookup(CONFIGURATION_CONFIG_KEY, CONFIGURATION_MAX_DISK_UTILIZATION_MB_TOPIC))
                .thenReturn(maxDiskUtilizationMBTopic);
        when(config.lookup(CONFIGURATION_CONFIG_KEY, CONFIGURATION_MAX_DOC_SIZE_LIMIT_B_TOPIC))
                .thenReturn(maxDocSizeTopic);
        when(config.lookupTopics(CONFIGURATION_CONFIG_KEY, CONFIGURATION_SYNCHRONIZATION_TOPIC))
                .thenReturn(configTopics);
        ShadowManager shadowManager = new ShadowManager(config, mockDatabase, mockDao, mockAuthorizationHandlerWrapper, mockPubSubClientWrapper, mockDeviceConfiguration);
        shadowManager.install();
        assertTrue(shadowManager.isErrored());
    }
}
