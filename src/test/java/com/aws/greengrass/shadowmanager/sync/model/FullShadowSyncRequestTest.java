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
import com.aws.greengrass.shadowmanager.sync.IotDataPlaneClientFactory;
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
import software.amazon.awssdk.services.iotdataplane.IotDataPlaneClient;
import software.amazon.awssdk.services.iotdataplane.model.GetThingShadowRequest;
import software.amazon.awssdk.services.iotdataplane.model.GetThingShadowResponse;
import software.amazon.awssdk.services.iotdataplane.model.UpdateThingShadowRequest;
import software.amazon.awssdk.services.iotdataplane.model.UpdateThingShadowResponse;

import java.io.IOException;
import java.time.Instant;
import java.util.Optional;

import static com.aws.greengrass.shadowmanager.TestUtils.SHADOW_NAME;
import static com.aws.greengrass.shadowmanager.TestUtils.THING_NAME;
import static com.aws.greengrass.shadowmanager.model.Constants.SHADOW_DOCUMENT_VERSION;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith({MockitoExtension.class, GGExtension.class})
class FullShadowSyncRequestTest {
    private static final byte[] LOCAL_DOCUMENT = "{\"version\": 10, \"state\": {\"reported\": {\"name\": \"The Beach Boys\", \"NewField\": 100}, \"desired\": {\"name\": \"Pink Floyd\", \"SomethingNew\": true}}}".getBytes();
    private static final byte[] CLOUD_DOCUMENT = "{\"version\": 5, \"state\": {\"reported\": {\"name\": \"The Beatles\", \"OldField\": true}, \"desired\": {\"name\": \"Backstreet Boys\", \"SomeOtherThingNew\": 100}}}".getBytes();
    private static final byte[] BASE_DOCUMENT = "{\"version\": 1, \"state\": {\"reported\": {\"name\": \"The Beatles\", \"OldField\": true}, \"desired\": {\"name\": \"The Beatles\"}}}".getBytes();
    private static final byte[] MERGED_DOCUMENT = "{\"state\": {\"reported\": {\"name\": \"The Beach Boys\", \"NewField\": 100}, \"desired\": {\"name\": \"Backstreet Boys\", \"SomethingNew\": true, \"SomeOtherThingNew\": 100}}}".getBytes();

    @Mock
    private ShadowManagerDAO mockDao;
    @Mock
    private IotDataPlaneClientFactory mockClientFactory;
    @Mock
    private IotDataPlaneClient mockIotDataPlaneClient;
    @Mock
    private UpdateThingShadowRequestHandler mockUpdateThingShadowRequestHandler;
    @Mock
    private DeleteThingShadowRequestHandler mockDeleteThingShadowRequestHandler;
    @Captor
    private ArgumentCaptor<SyncInformation> syncInformationCaptor;
    @Captor
    private ArgumentCaptor<software.amazon.awssdk.aws.greengrass.model.UpdateThingShadowRequest> localUpdateThingShadowRequestCaptor;
    @Captor
    private ArgumentCaptor<UpdateThingShadowRequest> cloudUpdateThingShadowRequestCaptor;

    @BeforeEach
    void setup() {
        lenient().when(mockClientFactory.getIotDataPlaneClient()).thenReturn(mockIotDataPlaneClient);
        lenient().when(mockDao.updateSyncInformation(syncInformationCaptor.capture())).thenReturn(true);
    }


    @Test
    void GIVEN_updated_local_and_cloud_document_WHEN_execute_THEN_updates_local_and_cloud_document() throws RetryableException, SkipSyncRequestException, IOException {
        long epochSeconds = Instant.now().getEpochSecond();
        long epochSecondsMinus60 = Instant.now().minusSeconds(60).getEpochSecond();
        JsonNode expectedMergedDocument = JsonUtil.getPayloadJson(MERGED_DOCUMENT).get();
        ShadowDocument shadowDocument = new ShadowDocument(LOCAL_DOCUMENT);
        when(mockDao.getShadowThing(anyString(), anyString())).thenReturn(Optional.of(shadowDocument));
        GetThingShadowResponse response = GetThingShadowResponse.builder()
                .payload(SdkBytes.fromByteArray(CLOUD_DOCUMENT))
                .build();
        when(mockIotDataPlaneClient.getThingShadow(any(GetThingShadowRequest.class))).thenReturn(response);
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
        when(mockUpdateThingShadowRequestHandler.handleRequest(localUpdateThingShadowRequestCaptor.capture(), anyString())).
                thenReturn(mock(software.amazon.awssdk.aws.greengrass.model.UpdateThingShadowResponse.class));
        when(mockIotDataPlaneClient.updateThingShadow(cloudUpdateThingShadowRequestCaptor.capture())).thenReturn(mock(UpdateThingShadowResponse.class));

        FullShadowSyncRequest fullShadowSyncRequest = new FullShadowSyncRequest(THING_NAME, SHADOW_NAME, mockDao, mockUpdateThingShadowRequestHandler, mockDeleteThingShadowRequestHandler, mockClientFactory);
        fullShadowSyncRequest.execute();

        verify(mockClientFactory, times(2)).getIotDataPlaneClient();
        verify(mockDao, times(1)).getShadowThing(anyString(), anyString());
        verify(mockDao, times(1)).updateSyncInformation(any());
        verify(mockUpdateThingShadowRequestHandler, times(1)).handleRequest(any(software.amazon.awssdk.aws.greengrass.model.UpdateThingShadowRequest.class), anyString());
        verify(mockIotDataPlaneClient, times(1)).updateThingShadow(any(UpdateThingShadowRequest.class));

        software.amazon.awssdk.aws.greengrass.model.UpdateThingShadowRequest localDocumentUpdateRequest = localUpdateThingShadowRequestCaptor.getValue();
        assertThat(localDocumentUpdateRequest.getShadowName(), is(SHADOW_NAME));
        assertThat(localDocumentUpdateRequest.getThingName(), is(THING_NAME));
        JsonNode actualLocalUpdateDocument = JsonUtil.getPayloadJson(localDocumentUpdateRequest.getPayload()).get();
        assertThat(actualLocalUpdateDocument.get(SHADOW_DOCUMENT_VERSION).asLong(), is(10L));
        ((ObjectNode)actualLocalUpdateDocument).remove(SHADOW_DOCUMENT_VERSION);
        assertThat(actualLocalUpdateDocument, is(expectedMergedDocument));

        UpdateThingShadowRequest cloudDocumentUpdateRequest = cloudUpdateThingShadowRequestCaptor.getValue();
        assertThat(cloudDocumentUpdateRequest.shadowName(), is(SHADOW_NAME));
        assertThat(cloudDocumentUpdateRequest.thingName(), is(THING_NAME));
        JsonNode actualCloudUpdateDocument = JsonUtil.getPayloadJson(cloudDocumentUpdateRequest.payload().asByteArray()).get();
        assertThat(actualCloudUpdateDocument.get(SHADOW_DOCUMENT_VERSION).asLong(), is(5L));
        ((ObjectNode)actualCloudUpdateDocument).remove(SHADOW_DOCUMENT_VERSION);
        assertThat(actualCloudUpdateDocument, is(expectedMergedDocument));

        assertThat(syncInformationCaptor.getValue(), is(notNullValue()));
        assertThat(JsonUtil.getPayloadJson(syncInformationCaptor.getValue().getLastSyncedDocument()).get(), is(expectedMergedDocument));
        assertThat(syncInformationCaptor.getValue().getCloudVersion(), is(6L));
        assertThat(syncInformationCaptor.getValue().getLocalVersion(), is(11L));
        assertThat(syncInformationCaptor.getValue().getCloudUpdateTime(), is(greaterThanOrEqualTo(epochSeconds)));
        assertThat(syncInformationCaptor.getValue().getLastSyncTime(), is(greaterThanOrEqualTo(epochSeconds)));
        assertThat(syncInformationCaptor.getValue().getShadowName(), is(SHADOW_NAME));
        assertThat(syncInformationCaptor.getValue().getThingName(), is(THING_NAME));
        assertThat(syncInformationCaptor.getValue().isCloudDeleted(), is(false));

    }
}
