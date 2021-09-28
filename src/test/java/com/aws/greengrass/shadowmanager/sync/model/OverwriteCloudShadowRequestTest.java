/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.shadowmanager.sync.model;

import com.aws.greengrass.shadowmanager.ShadowManagerDAO;
import com.aws.greengrass.shadowmanager.exception.RetryableException;
import com.aws.greengrass.shadowmanager.exception.SkipSyncRequestException;
import com.aws.greengrass.shadowmanager.ipc.DeleteThingShadowRequestHandler;
import com.aws.greengrass.shadowmanager.ipc.UpdateThingShadowRequestHandler;
import com.aws.greengrass.shadowmanager.model.ShadowDocument;
import com.aws.greengrass.shadowmanager.model.dao.SyncInformation;
import com.aws.greengrass.shadowmanager.sync.IotDataPlaneClientWrapper;
import com.aws.greengrass.shadowmanager.util.JsonUtil;
import com.aws.greengrass.testcommons.testutilities.GGExtension;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.iotdataplane.model.DeleteThingShadowResponse;
import software.amazon.awssdk.services.iotdataplane.model.UpdateThingShadowResponse;

import java.io.IOException;
import java.time.Instant;
import java.util.Optional;

import static com.aws.greengrass.shadowmanager.TestUtils.SHADOW_NAME;
import static com.aws.greengrass.shadowmanager.TestUtils.THING_NAME;
import static com.aws.greengrass.shadowmanager.model.Constants.SHADOW_DOCUMENT_VERSION;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith({MockitoExtension.class, GGExtension.class})
class OverwriteCloudShadowRequestTest {
    private static final byte[] LOCAL_DOCUMENT = "{\"version\": 10, \"state\": {\"reported\": {\"name\": \"The Beach Boys\", \"NewField\": 100}, \"desired\": {\"name\": \"Pink Floyd\", \"SomethingNew\": true}}}".getBytes();
    private static final byte[] BASE_DOCUMENT = "{\"version\": 1, \"state\": {\"reported\": {\"name\": \"The Beatles\", \"OldField\": true}, \"desired\": {\"name\": \"The Beatles\"}}}".getBytes();

    @Mock
    private ShadowManagerDAO mockDao;
    @Mock
    private IotDataPlaneClientWrapper mockIotDataPlaneClientWrapper;
    @Mock
    private UpdateThingShadowRequestHandler mockUpdateThingShadowRequestHandler;
    @Mock
    private DeleteThingShadowRequestHandler mockDeleteThingShadowRequestHandler;
    @Captor
    private ArgumentCaptor<SyncInformation> syncInformationCaptor;
    @Captor
    private ArgumentCaptor<String> thingNameCaptor;
    @Captor
    private ArgumentCaptor<String> shadowNameCaptor;
    @Captor
    private ArgumentCaptor<byte[]> payloadCaptor;

    private SyncContext syncContext;

    @BeforeEach
    void setup() throws IOException {
        lenient().when(mockDao.updateSyncInformation(syncInformationCaptor.capture())).thenReturn(true);
        syncContext = new SyncContext(mockDao, mockUpdateThingShadowRequestHandler, mockDeleteThingShadowRequestHandler,
                mockIotDataPlaneClientWrapper);
        JsonUtil.loadSchema();
    }

    @Test
    void GIVEN_updated_local_shadow_WHEN_execute_THEN_updates_cloud_shadow() throws RetryableException, SkipSyncRequestException, IOException, InterruptedException {
        long epochSeconds = Instant.now().getEpochSecond();
        long epochSecondsMinus60 = Instant.now().minusSeconds(60).getEpochSecond();
        ShadowDocument shadowDocument = new ShadowDocument(LOCAL_DOCUMENT);
        when(mockDao.getShadowThing(anyString(), anyString())).thenReturn(Optional.of(shadowDocument));
        when(mockDao.getShadowSyncInformation(anyString(), anyString())).thenReturn(Optional.of(SyncInformation.builder()
                .cloudUpdateTime(epochSecondsMinus60)
                .thingName(THING_NAME)
                .shadowName(SHADOW_NAME)
                .cloudDeleted(false)
                .lastSyncedDocument(BASE_DOCUMENT)
                .cloudVersion(1L)
                .localVersion(1L)
                .lastSyncTime(epochSecondsMinus60)
                .build()));

        when(mockIotDataPlaneClientWrapper.updateThingShadow(thingNameCaptor.capture(), shadowNameCaptor.capture(), payloadCaptor.capture()))
                .thenReturn(UpdateThingShadowResponse.builder().payload(SdkBytes.fromString("{\"version\": 6, \"state\": {}}", UTF_8)).build());

        OverwriteCloudShadowRequest request = new OverwriteCloudShadowRequest(THING_NAME, SHADOW_NAME);
        request.execute(syncContext);

        verify(mockDao, times(1)).getShadowThing(anyString(), anyString());
        verify(mockDao, times(1)).updateSyncInformation(any());
        verify(mockUpdateThingShadowRequestHandler, never()).handleRequest(any(software.amazon.awssdk.aws.greengrass.model.UpdateThingShadowRequest.class), anyString());
        verify(mockIotDataPlaneClientWrapper, times(1)).updateThingShadow(anyString(), anyString(), any(byte[].class));

        assertThat(thingNameCaptor.getValue(), is(THING_NAME));
        assertThat(shadowNameCaptor.getValue(), is(SHADOW_NAME));
        JsonNode actualCloudUpdateDocument = JsonUtil.getPayloadJson(payloadCaptor.getValue()).get();
        ((ObjectNode)actualCloudUpdateDocument).remove(SHADOW_DOCUMENT_VERSION);
        assertThat(actualCloudUpdateDocument, is(shadowDocument.toJson(false)));

        assertThat(syncInformationCaptor.getValue(), is(notNullValue()));
        assertThat(JsonUtil.getPayloadJson(syncInformationCaptor.getValue().getLastSyncedDocument()).get(), is(shadowDocument.toJson(false)));
        assertThat(syncInformationCaptor.getValue().getCloudVersion(), is(6L));
        assertThat(syncInformationCaptor.getValue().getLocalVersion(), is(10L));
        assertThat(syncInformationCaptor.getValue().getCloudUpdateTime(), is(greaterThanOrEqualTo(epochSeconds)));
        assertThat(syncInformationCaptor.getValue().getLastSyncTime(), is(greaterThanOrEqualTo(epochSeconds)));
        assertThat(syncInformationCaptor.getValue().getShadowName(), is(SHADOW_NAME));
        assertThat(syncInformationCaptor.getValue().getThingName(), is(THING_NAME));
        assertThat(syncInformationCaptor.getValue().isCloudDeleted(), is(false));
    }

    @Test
    void GIVEN_deleted_local_shadow_WHEN_execute_THEN_deletes_cloud_shadow() throws RetryableException, SkipSyncRequestException, IOException, InterruptedException {
        long epochSeconds = Instant.now().getEpochSecond();
        long epochSecondsMinus60 = Instant.now().minusSeconds(60).getEpochSecond();
        when(mockDao.getShadowThing(anyString(), anyString())).thenReturn(Optional.empty());
        when(mockDao.getDeletedShadowVersion(anyString(), anyString())).thenReturn(Optional.of(5L));
        when(mockDao.getShadowSyncInformation(anyString(), anyString())).thenReturn(Optional.of(SyncInformation.builder()
                .cloudUpdateTime(epochSecondsMinus60)
                .thingName(THING_NAME)
                .shadowName(SHADOW_NAME)
                .cloudDeleted(false)
                .lastSyncedDocument(BASE_DOCUMENT)
                .cloudVersion(1L)
                .localVersion(1L)
                .lastSyncTime(epochSecondsMinus60)
                .build()));

        when(mockIotDataPlaneClientWrapper.deleteThingShadow(thingNameCaptor.capture(), shadowNameCaptor.capture()))
                .thenReturn(DeleteThingShadowResponse.builder().build());

        OverwriteCloudShadowRequest request = new OverwriteCloudShadowRequest(THING_NAME, SHADOW_NAME);
        request.execute(syncContext);

        verify(mockDao, times(1)).getShadowThing(anyString(), anyString());
        verify(mockDao, times(1)).updateSyncInformation(any());
        verify(mockUpdateThingShadowRequestHandler, never()).handleRequest(any(software.amazon.awssdk.aws.greengrass.model.UpdateThingShadowRequest.class), anyString());
        verify(mockDeleteThingShadowRequestHandler, never()).handleRequest(any(software.amazon.awssdk.aws.greengrass.model.DeleteThingShadowRequest.class), anyString());
        verify(mockIotDataPlaneClientWrapper, never()).updateThingShadow(anyString(), anyString(), any(byte[].class));
        verify(mockIotDataPlaneClientWrapper, times(1)).deleteThingShadow(anyString(), anyString());

        assertThat(thingNameCaptor.getValue(), is(THING_NAME));
        assertThat(shadowNameCaptor.getValue(), is(SHADOW_NAME));

        assertThat(syncInformationCaptor.getValue(), is(notNullValue()));
        assertThat(syncInformationCaptor.getValue().getLastSyncedDocument(), is(nullValue()));
        assertThat(syncInformationCaptor.getValue().getCloudVersion(), is(2L));
        assertThat(syncInformationCaptor.getValue().getLocalVersion(), is(5L));
        assertThat(syncInformationCaptor.getValue().getCloudUpdateTime(), is(greaterThanOrEqualTo(epochSeconds)));
        assertThat(syncInformationCaptor.getValue().getLastSyncTime(), is(greaterThanOrEqualTo(epochSeconds)));
        assertThat(syncInformationCaptor.getValue().getShadowName(), is(SHADOW_NAME));
        assertThat(syncInformationCaptor.getValue().getThingName(), is(THING_NAME));
        assertThat(syncInformationCaptor.getValue().isCloudDeleted(), is(false));
    }

    @Test
    void GIVEN_same_local_shadow_WHEN_execute_THEN_does_not_update_cloud_shadow() throws RetryableException, SkipSyncRequestException, IOException, InterruptedException {
        long epochSecondsMinus60 = Instant.now().minusSeconds(60).getEpochSecond();
        ShadowDocument shadowDocument = new ShadowDocument(LOCAL_DOCUMENT);
        when(mockDao.getShadowThing(anyString(), anyString())).thenReturn(Optional.of(shadowDocument));
        when(mockDao.getShadowSyncInformation(anyString(), anyString())).thenReturn(Optional.of(SyncInformation.builder()
                .cloudUpdateTime(epochSecondsMinus60)
                .thingName(THING_NAME)
                .shadowName(SHADOW_NAME)
                .cloudDeleted(false)
                .lastSyncedDocument(BASE_DOCUMENT)
                .cloudVersion(1L)
                .localVersion(10L)
                .lastSyncTime(epochSecondsMinus60)
                .build()));

        OverwriteCloudShadowRequest request = new OverwriteCloudShadowRequest(THING_NAME, SHADOW_NAME);
        request.execute(syncContext);

        verify(mockDao, times(1)).getShadowThing(anyString(), anyString());
        verify(mockDao, never()).updateSyncInformation(any());
        verify(mockUpdateThingShadowRequestHandler, never()).handleRequest(any(software.amazon.awssdk.aws.greengrass.model.UpdateThingShadowRequest.class), anyString());
        verify(mockDeleteThingShadowRequestHandler, never()).handleRequest(any(software.amazon.awssdk.aws.greengrass.model.DeleteThingShadowRequest.class), anyString());
        verify(mockIotDataPlaneClientWrapper, never()).updateThingShadow(anyString(), anyString(), any(byte[].class));
        verify(mockIotDataPlaneClientWrapper, never()).deleteThingShadow(anyString(), anyString());
    }
}
