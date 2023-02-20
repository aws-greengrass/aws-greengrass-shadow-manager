/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.shadowmanager.sync.model;

import com.aws.greengrass.shadowmanager.ShadowManagerDAO;
import com.aws.greengrass.shadowmanager.exception.IoTDataPlaneClientCreationException;
import com.aws.greengrass.shadowmanager.exception.RetryableException;
import com.aws.greengrass.shadowmanager.exception.SkipSyncRequestException;
import com.aws.greengrass.shadowmanager.ipc.DeleteThingShadowRequestHandler;
import com.aws.greengrass.shadowmanager.ipc.UpdateThingShadowRequestHandler;
import com.aws.greengrass.shadowmanager.model.UpdateThingShadowHandlerResponse;
import com.aws.greengrass.shadowmanager.model.dao.SyncInformation;
import com.aws.greengrass.shadowmanager.sync.IotDataPlaneClientWrapper;
import com.aws.greengrass.shadowmanager.util.JsonUtil;
import com.aws.greengrass.testcommons.testutilities.GGExtension;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.mockito.Answers;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.iotdataplane.model.GetThingShadowResponse;
import software.amazon.awssdk.services.iotdataplane.model.ResourceNotFoundException;

import java.io.IOException;
import java.time.Instant;
import java.util.Optional;

import static com.aws.greengrass.shadowmanager.TestUtils.SHADOW_NAME;
import static com.aws.greengrass.shadowmanager.TestUtils.THING_NAME;
import static com.aws.greengrass.shadowmanager.model.Constants.SHADOW_DOCUMENT_METADATA;
import static com.aws.greengrass.shadowmanager.model.Constants.SHADOW_DOCUMENT_VERSION;
import static com.aws.greengrass.testcommons.testutilities.ExceptionLogProtector.ignoreExceptionOfType;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith({MockitoExtension.class, GGExtension.class})
class OverwriteLocalShadowRequestTest {
    private static final byte[] BASE_DOCUMENT = "{\"version\": 1, \"state\": {\"reported\": {\"name\": \"The Beatles\", \"OldField\": true}, \"desired\": {\"name\": \"The Beatles\"}}}".getBytes();
    private static final byte[] CLOUD_DOCUMENT_WITH_METADATA = "{\"version\": 5, \"state\": {\"reported\": {\"name\": \"The Beatles\", \"OldField\": true}, \"desired\": {\"name\": \"Backstreet Boys\", \"SomeOtherThingNew\": 100}}, \"metadata\": {\"reported\": {\"name\": {\"timestamp\": 100}, \"OldField\": {\"timestamp\": 100}}, \"desired\": {\"name\": {\"timestamp\": 100}, \"SomeOtherThingNew\": {\"timestamp\": 100}}}}".getBytes();

    @Mock
    private ShadowManagerDAO mockDao;
    @Mock
    private IotDataPlaneClientWrapper mockIotDataPlaneClientWrapper;
    @Mock
    private UpdateThingShadowRequestHandler mockUpdateThingShadowRequestHandler;
    @Mock
    private DeleteThingShadowRequestHandler mockDeleteThingShadowRequestHandler;
    @Mock(answer = Answers.RETURNS_DEEP_STUBS)
    private UpdateThingShadowHandlerResponse mockUpdateThingShadowHandlerResponse;
    @Captor
    private ArgumentCaptor<SyncInformation> syncInformationCaptor;
    @Captor
    private ArgumentCaptor<software.amazon.awssdk.aws.greengrass.model.UpdateThingShadowRequest> localUpdateThingShadowRequestCaptor;
    @Captor
    private ArgumentCaptor<software.amazon.awssdk.aws.greengrass.model.DeleteThingShadowRequest> localDeleteThingShadowRequest;

    private SyncContext syncContext;

    @BeforeEach
    void setup() throws IOException {
        lenient().when(mockDao.updateSyncInformation(any())).thenReturn(true);
        syncContext = new SyncContext(mockDao, mockUpdateThingShadowRequestHandler, mockDeleteThingShadowRequestHandler,
                mockIotDataPlaneClientWrapper);
        JsonUtil.loadSchema();
    }

    @Test
    void GIVEN_updated_cloud_shadow_WHEN_execute_THEN_updates_local_shadow() throws RetryableException, SkipSyncRequestException, IOException, InterruptedException, IoTDataPlaneClientCreationException {
        long epochSeconds = Instant.now().getEpochSecond();
        long epochSecondsMinus60 = Instant.now().minusSeconds(60).getEpochSecond();
        GetThingShadowResponse response = GetThingShadowResponse.builder()
                .payload(SdkBytes.fromByteArray(CLOUD_DOCUMENT_WITH_METADATA))
                .build();
        when(mockIotDataPlaneClientWrapper.getThingShadow(anyString(), anyString())).thenReturn(response);
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
        when(mockUpdateThingShadowHandlerResponse.getUpdateThingShadowResponse().getPayload()).thenReturn("{\"version\": 2, \"state\": {}}".getBytes(UTF_8));
        when(mockUpdateThingShadowRequestHandler.handleRequest(localUpdateThingShadowRequestCaptor.capture(), anyString())).
                thenReturn(mockUpdateThingShadowHandlerResponse);

        OverwriteLocalShadowRequest request = new OverwriteLocalShadowRequest(THING_NAME, SHADOW_NAME);
        request.execute(syncContext);

        verify(mockIotDataPlaneClientWrapper, times(1)).getThingShadow(anyString(), anyString());
        verify(mockDao, never()).getShadowThing(anyString(), anyString());
        verify(mockDao, times(1)).updateSyncInformation(syncInformationCaptor.capture());
        verify(mockUpdateThingShadowRequestHandler, times(1)).handleRequest(any(software.amazon.awssdk.aws.greengrass.model.UpdateThingShadowRequest.class), anyString());
        verify(mockDeleteThingShadowRequestHandler, never()).handleRequest(any(software.amazon.awssdk.aws.greengrass.model.DeleteThingShadowRequest.class), anyString());
        verify(mockIotDataPlaneClientWrapper, never()).updateThingShadow(anyString(), anyString(), any(byte[].class));
        verify(mockIotDataPlaneClientWrapper, never()).deleteThingShadow(anyString(), anyString());

        software.amazon.awssdk.aws.greengrass.model.UpdateThingShadowRequest localDocumentUpdateRequest = localUpdateThingShadowRequestCaptor.getValue();
        assertThat(localDocumentUpdateRequest.getShadowName(), is(SHADOW_NAME));
        assertThat(localDocumentUpdateRequest.getThingName(), is(THING_NAME));
        JsonNode actualLocalUpdateDocument = JsonUtil.getPayloadJson(localDocumentUpdateRequest.getPayload()).get();
        ((ObjectNode)actualLocalUpdateDocument).remove(SHADOW_DOCUMENT_METADATA);
        ((ObjectNode)actualLocalUpdateDocument).remove(SHADOW_DOCUMENT_VERSION);
        JsonNode expectedMergedDocument = JsonUtil.getPayloadJson(CLOUD_DOCUMENT_WITH_METADATA).get();
        ((ObjectNode)expectedMergedDocument).remove(SHADOW_DOCUMENT_METADATA);
        ((ObjectNode)expectedMergedDocument).remove(SHADOW_DOCUMENT_VERSION);
        assertThat(actualLocalUpdateDocument, is(expectedMergedDocument));

        assertThat(syncInformationCaptor.getValue(), is(notNullValue()));
        JsonNode lastSyncedDocument = JsonUtil.getPayloadJson(syncInformationCaptor.getValue().getLastSyncedDocument()).get();
        ((ObjectNode)lastSyncedDocument).remove(SHADOW_DOCUMENT_METADATA);
        ((ObjectNode)lastSyncedDocument).remove(SHADOW_DOCUMENT_VERSION);

        assertThat(lastSyncedDocument, is(expectedMergedDocument));
        assertThat(syncInformationCaptor.getValue().getCloudVersion(), is(5L));
        assertThat(syncInformationCaptor.getValue().getLocalVersion(), is(2L));
        assertThat(syncInformationCaptor.getValue().getCloudUpdateTime(), is(100L));
        assertThat(syncInformationCaptor.getValue().getLastSyncTime(), is(greaterThanOrEqualTo(epochSeconds)));
        assertThat(syncInformationCaptor.getValue().getShadowName(), is(SHADOW_NAME));
        assertThat(syncInformationCaptor.getValue().getThingName(), is(THING_NAME));
        assertThat(syncInformationCaptor.getValue().isCloudDeleted(), is(false));
    }

    @Test
    void GIVEN_deleted_cloud_shadow_WHEN_execute_THEN_deletes_local_shadow(ExtensionContext context) throws RetryableException, SkipSyncRequestException, IOException, InterruptedException, IoTDataPlaneClientCreationException {
        ignoreExceptionOfType(context, ResourceNotFoundException.class);
        long epochSeconds = Instant.now().getEpochSecond();
        long epochSecondsMinus60 = Instant.now().minusSeconds(60).getEpochSecond();
        when(mockIotDataPlaneClientWrapper.getThingShadow(anyString(), anyString())).thenThrow(ResourceNotFoundException.class);
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
        when(mockDeleteThingShadowRequestHandler.handleRequest(localDeleteThingShadowRequest.capture(), anyString())).
                thenReturn(mock(software.amazon.awssdk.aws.greengrass.model.DeleteThingShadowResponse.class));

        OverwriteLocalShadowRequest request = new OverwriteLocalShadowRequest(THING_NAME, SHADOW_NAME);
        request.execute(syncContext);

        verify(mockIotDataPlaneClientWrapper, times(1)).getThingShadow(anyString(), anyString());
        verify(mockDao, never()).getShadowThing(anyString(), anyString());
        verify(mockDao, times(1)).updateSyncInformation(syncInformationCaptor.capture());
        verify(mockUpdateThingShadowRequestHandler, never()).handleRequest(any(software.amazon.awssdk.aws.greengrass.model.UpdateThingShadowRequest.class), anyString());
        verify(mockDeleteThingShadowRequestHandler, times(1)).handleRequest(any(software.amazon.awssdk.aws.greengrass.model.DeleteThingShadowRequest.class), anyString());
        verify(mockIotDataPlaneClientWrapper, never()).updateThingShadow(anyString(), anyString(), any(byte[].class));
        verify(mockIotDataPlaneClientWrapper, never()).deleteThingShadow(anyString(), anyString());

        software.amazon.awssdk.aws.greengrass.model.DeleteThingShadowRequest localDocumentDeleteRequest = localDeleteThingShadowRequest.getValue();
        assertThat(localDocumentDeleteRequest.getShadowName(), is(SHADOW_NAME));
        assertThat(localDocumentDeleteRequest.getThingName(), is(THING_NAME));

        assertThat(syncInformationCaptor.getValue(), is(notNullValue()));
        assertThat(syncInformationCaptor.getValue().getLastSyncedDocument(), is(nullValue()));
        assertThat(syncInformationCaptor.getValue().getCloudVersion(), is(2L));
        assertThat(syncInformationCaptor.getValue().getLocalVersion(), is(2L));
        assertThat(syncInformationCaptor.getValue().getCloudUpdateTime(), is(greaterThanOrEqualTo(epochSecondsMinus60)));
        assertThat(syncInformationCaptor.getValue().getLastSyncTime(), is(greaterThanOrEqualTo(epochSeconds)));
        assertThat(syncInformationCaptor.getValue().getShadowName(), is(SHADOW_NAME));
        assertThat(syncInformationCaptor.getValue().getThingName(), is(THING_NAME));
        assertThat(syncInformationCaptor.getValue().isCloudDeleted(), is(false));
    }

    @Test
    void GIVEN_same_cloud_shadow_WHEN_execute_THEN_does_not_update_local_shadow() throws RetryableException, SkipSyncRequestException, IOException, InterruptedException, IoTDataPlaneClientCreationException {
        long epochSecondsMinus60 = Instant.now().minusSeconds(60).getEpochSecond();
        GetThingShadowResponse response = GetThingShadowResponse.builder()
                .payload(SdkBytes.fromByteArray(CLOUD_DOCUMENT_WITH_METADATA))
                .build();
        when(mockIotDataPlaneClientWrapper.getThingShadow(anyString(), anyString())).thenReturn(response);
        when(mockDao.getShadowSyncInformation(anyString(), anyString())).thenReturn(Optional.of(SyncInformation.builder()
                .cloudUpdateTime(epochSecondsMinus60)
                .thingName(THING_NAME)
                .shadowName(SHADOW_NAME)
                .cloudDeleted(false)
                .lastSyncedDocument(BASE_DOCUMENT)
                .cloudVersion(5L)
                .localVersion(1L)
                .lastSyncTime(epochSecondsMinus60)
                .build()));

        OverwriteLocalShadowRequest request = new OverwriteLocalShadowRequest(THING_NAME, SHADOW_NAME);
        request.execute(syncContext);

        verify(mockIotDataPlaneClientWrapper, times(1)).getThingShadow(anyString(), anyString());
        verify(mockDao, never()).getShadowThing(anyString(), anyString());
        verify(mockDao, never()).updateSyncInformation(any());
        verify(mockUpdateThingShadowRequestHandler, never()).handleRequest(any(software.amazon.awssdk.aws.greengrass.model.UpdateThingShadowRequest.class), anyString());
        verify(mockDeleteThingShadowRequestHandler, never()).handleRequest(any(software.amazon.awssdk.aws.greengrass.model.DeleteThingShadowRequest.class), anyString());
        verify(mockIotDataPlaneClientWrapper, never()).updateThingShadow(anyString(), anyString(), any(byte[].class));
        verify(mockIotDataPlaneClientWrapper, never()).deleteThingShadow(anyString(), anyString());
    }
}
