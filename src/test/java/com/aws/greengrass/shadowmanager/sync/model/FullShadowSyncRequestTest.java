/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.shadowmanager.sync.model;

import com.aws.greengrass.shadowmanager.ShadowManagerDAO;
import com.aws.greengrass.shadowmanager.exception.RetryableException;
import com.aws.greengrass.shadowmanager.exception.ShadowManagerDataException;
import com.aws.greengrass.shadowmanager.exception.SkipSyncRequestException;
import com.aws.greengrass.shadowmanager.ipc.DeleteThingShadowRequestHandler;
import com.aws.greengrass.shadowmanager.ipc.UpdateThingShadowRequestHandler;
import com.aws.greengrass.shadowmanager.model.ShadowDocument;
import com.aws.greengrass.shadowmanager.model.UpdateThingShadowHandlerResponse;
import com.aws.greengrass.shadowmanager.model.dao.SyncInformation;
import com.aws.greengrass.shadowmanager.sync.IotDataPlaneClientFactory;
import com.aws.greengrass.shadowmanager.util.JsonUtil;
import com.aws.greengrass.testcommons.testutilities.GGExtension;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
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
import software.amazon.awssdk.aws.greengrass.model.ConflictError;
import software.amazon.awssdk.aws.greengrass.model.InvalidArgumentsError;
import software.amazon.awssdk.aws.greengrass.model.ServiceError;
import software.amazon.awssdk.aws.greengrass.model.UnauthorizedError;
import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.services.iotdataplane.IotDataPlaneClient;
import software.amazon.awssdk.services.iotdataplane.model.ConflictException;
import software.amazon.awssdk.services.iotdataplane.model.DeleteThingShadowRequest;
import software.amazon.awssdk.services.iotdataplane.model.DeleteThingShadowResponse;
import software.amazon.awssdk.services.iotdataplane.model.GetThingShadowRequest;
import software.amazon.awssdk.services.iotdataplane.model.GetThingShadowResponse;
import software.amazon.awssdk.services.iotdataplane.model.InternalFailureException;
import software.amazon.awssdk.services.iotdataplane.model.InvalidRequestException;
import software.amazon.awssdk.services.iotdataplane.model.MethodNotAllowedException;
import software.amazon.awssdk.services.iotdataplane.model.RequestEntityTooLargeException;
import software.amazon.awssdk.services.iotdataplane.model.ResourceNotFoundException;
import software.amazon.awssdk.services.iotdataplane.model.ServiceUnavailableException;
import software.amazon.awssdk.services.iotdataplane.model.ThrottlingException;
import software.amazon.awssdk.services.iotdataplane.model.UnauthorizedException;
import software.amazon.awssdk.services.iotdataplane.model.UnsupportedDocumentEncodingException;
import software.amazon.awssdk.services.iotdataplane.model.UpdateThingShadowRequest;
import software.amazon.awssdk.services.iotdataplane.model.UpdateThingShadowResponse;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.time.Instant;
import java.util.Optional;

import static com.aws.greengrass.shadowmanager.TestUtils.SHADOW_NAME;
import static com.aws.greengrass.shadowmanager.TestUtils.THING_NAME;
import static com.aws.greengrass.shadowmanager.model.Constants.SHADOW_DOCUMENT_METADATA;
import static com.aws.greengrass.shadowmanager.model.Constants.SHADOW_DOCUMENT_VERSION;
import static com.aws.greengrass.testcommons.testutilities.ExceptionLogProtector.ignoreExceptionOfType;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@SuppressWarnings("PMD.CouplingBetweenObjects")
@ExtendWith({MockitoExtension.class, GGExtension.class})
class FullShadowSyncRequestTest {
    private static final byte[] LOCAL_DOCUMENT = "{\"version\": 10, \"state\": {\"reported\": {\"name\": \"The Beach Boys\", \"NewField\": 100}, \"desired\": {\"name\": \"Pink Floyd\", \"SomethingNew\": true}}}".getBytes();
    private static final byte[] CLOUD_DOCUMENT = "{\"version\": 5, \"state\": {\"reported\": {\"name\": \"The Beatles\", \"OldField\": true}, \"desired\": {\"name\": \"Backstreet Boys\", \"SomeOtherThingNew\": 100}}}".getBytes();
    private static final byte[] BASE_DOCUMENT = "{\"version\": 1, \"state\": {\"reported\": {\"name\": \"The Beatles\", \"OldField\": true}, \"desired\": {\"name\": \"The Beatles\"}}}".getBytes();
    private static final byte[] MERGED_DOCUMENT = "{\"state\": {\"reported\": {\"name\": \"The Beach Boys\", \"NewField\": 100}, \"desired\": {\"name\": \"Backstreet Boys\", \"SomethingNew\": true, \"SomeOtherThingNew\": 100}}}".getBytes();
    private static final byte[] BAD_DOCUMENT = "{\"version\": true}".getBytes();

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
    @Captor
    private ArgumentCaptor<software.amazon.awssdk.aws.greengrass.model.DeleteThingShadowRequest> localDeleteThingShadowRequest;
    @Captor
    private ArgumentCaptor<DeleteThingShadowRequest> cloudDeleteThingShadowRequest;

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
                thenReturn(mock(UpdateThingShadowHandlerResponse.class));
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

    @Test
    void GIVEN_updated_cloud_document_and_no_local_document_WHEN_execute_THEN_deletes_cloud_document() throws RetryableException, SkipSyncRequestException {
        long epochSeconds = Instant.now().getEpochSecond();
        long epochSecondsMinus60 = Instant.now().minusSeconds(60).getEpochSecond();
        GetThingShadowResponse response = GetThingShadowResponse.builder()
                .payload(SdkBytes.fromByteArray(CLOUD_DOCUMENT))
                .build();
        when(mockIotDataPlaneClient.getThingShadow(any(GetThingShadowRequest.class))).thenReturn(response);
        when(mockDao.getShadowThing(anyString(), anyString())).thenReturn(Optional.empty());
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
        when(mockIotDataPlaneClient.deleteThingShadow(cloudDeleteThingShadowRequest.capture())).thenReturn(mock(DeleteThingShadowResponse.class));

        FullShadowSyncRequest fullShadowSyncRequest = new FullShadowSyncRequest(THING_NAME, SHADOW_NAME, mockDao, mockUpdateThingShadowRequestHandler, mockDeleteThingShadowRequestHandler, mockClientFactory);
        fullShadowSyncRequest.execute();

        verify(mockClientFactory, times(2)).getIotDataPlaneClient();
        verify(mockDao, times(1)).getShadowThing(anyString(), anyString());
        verify(mockDao, times(1)).updateSyncInformation(any());
        verify(mockUpdateThingShadowRequestHandler, times(0)).handleRequest(any(software.amazon.awssdk.aws.greengrass.model.UpdateThingShadowRequest.class), anyString());
        verify(mockIotDataPlaneClient, times(0)).updateThingShadow(any(UpdateThingShadowRequest.class));
        verify(mockIotDataPlaneClient, times(1)).deleteThingShadow(any(DeleteThingShadowRequest.class));

        DeleteThingShadowRequest cloudDocumentDeleteRequest = cloudDeleteThingShadowRequest.getValue();
        assertThat(cloudDocumentDeleteRequest.shadowName(), is(SHADOW_NAME));
        assertThat(cloudDocumentDeleteRequest.thingName(), is(THING_NAME));

        assertThat(syncInformationCaptor.getValue(), is(notNullValue()));
        assertThat(syncInformationCaptor.getValue().getLastSyncedDocument(), is(nullValue()));
        assertThat(syncInformationCaptor.getValue().getCloudVersion(), is(5L));
        assertThat(syncInformationCaptor.getValue().getLocalVersion(), is(1L));
        assertThat(syncInformationCaptor.getValue().getCloudUpdateTime(), is(greaterThanOrEqualTo(epochSeconds)));
        assertThat(syncInformationCaptor.getValue().getLastSyncTime(), is(greaterThanOrEqualTo(epochSeconds)));
        assertThat(syncInformationCaptor.getValue().getShadowName(), is(SHADOW_NAME));
        assertThat(syncInformationCaptor.getValue().getThingName(), is(THING_NAME));
        assertThat(syncInformationCaptor.getValue().isCloudDeleted(), is(false));
    }

    @Test
    void GIVEN_same_cloud_document_and_local_document_WHEN_execute_THEN_does_not_update_local_and_cloud_document() throws RetryableException, SkipSyncRequestException, IOException {
        long epochSecondsMinus60 = Instant.now().minusSeconds(60).getEpochSecond();
        GetThingShadowResponse response = GetThingShadowResponse.builder()
                .payload(SdkBytes.fromByteArray(CLOUD_DOCUMENT))
                .build();
        ShadowDocument shadowDocument = new ShadowDocument(LOCAL_DOCUMENT);
        when(mockIotDataPlaneClient.getThingShadow(any(GetThingShadowRequest.class))).thenReturn(response);
        when(mockDao.getShadowThing(anyString(), anyString())).thenReturn(Optional.of(shadowDocument));
        when(mockDao.getShadowSyncInformation(anyString(), anyString())).thenReturn(Optional.of(SyncInformation.builder()
                .cloudUpdateTime(epochSecondsMinus60)
                .thingName(THING_NAME)
                .shadowName(SHADOW_NAME)
                .cloudDeleted(false)
                .lastSyncedDocument(BASE_DOCUMENT)
                .cloudVersion(5L)
                .localVersion(10L)
                .lastSyncTime(epochSecondsMinus60)
                .build()));

        FullShadowSyncRequest fullShadowSyncRequest = new FullShadowSyncRequest(THING_NAME, SHADOW_NAME, mockDao, mockUpdateThingShadowRequestHandler, mockDeleteThingShadowRequestHandler, mockClientFactory);
        fullShadowSyncRequest.execute();

        verify(mockClientFactory, times(1)).getIotDataPlaneClient();
        verify(mockDao, times(1)).getShadowThing(anyString(), anyString());
        verify(mockDao, times(0)).updateSyncInformation(any());
        verify(mockUpdateThingShadowRequestHandler, times(0)).handleRequest(any(software.amazon.awssdk.aws.greengrass.model.UpdateThingShadowRequest.class), anyString());
        verify(mockIotDataPlaneClient, times(0)).updateThingShadow(any(UpdateThingShadowRequest.class));
        verify(mockIotDataPlaneClient, times(0)).deleteThingShadow(any(DeleteThingShadowRequest.class));
    }

    @Test
    void GIVEN_nonexistent_cloud_document_and_existent_local_document_WHEN_execute_THEN_deletes_local_document(ExtensionContext context) throws RetryableException, SkipSyncRequestException, IOException {
        ignoreExceptionOfType(context, ResourceNotFoundException.class);
        long epochSeconds = Instant.now().getEpochSecond();
        long epochSecondsMinus60 = Instant.now().minusSeconds(60).getEpochSecond();
        ShadowDocument shadowDocument = new ShadowDocument(LOCAL_DOCUMENT);
        when(mockIotDataPlaneClient.getThingShadow(any(GetThingShadowRequest.class))).thenThrow(ResourceNotFoundException.class);
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
        when(mockDeleteThingShadowRequestHandler.handleRequest(localDeleteThingShadowRequest.capture(), anyString())).
                thenReturn(mock(software.amazon.awssdk.aws.greengrass.model.DeleteThingShadowResponse.class));

        FullShadowSyncRequest fullShadowSyncRequest = new FullShadowSyncRequest(THING_NAME, SHADOW_NAME, mockDao, mockUpdateThingShadowRequestHandler, mockDeleteThingShadowRequestHandler, mockClientFactory);
        fullShadowSyncRequest.execute();

        verify(mockClientFactory, times(1)).getIotDataPlaneClient();
        verify(mockDao, times(1)).getShadowThing(anyString(), anyString());
        verify(mockDao, times(1)).updateSyncInformation(any());
        verify(mockUpdateThingShadowRequestHandler, times(0)).handleRequest(any(software.amazon.awssdk.aws.greengrass.model.UpdateThingShadowRequest.class), anyString());
        verify(mockDeleteThingShadowRequestHandler, times(1)).handleRequest(any(software.amazon.awssdk.aws.greengrass.model.DeleteThingShadowRequest.class), anyString());
        verify(mockIotDataPlaneClient, times(0)).updateThingShadow(any(UpdateThingShadowRequest.class));
        verify(mockIotDataPlaneClient, times(0)).deleteThingShadow(any(DeleteThingShadowRequest.class));

        software.amazon.awssdk.aws.greengrass.model.DeleteThingShadowRequest localDocumentDeleteRequest = localDeleteThingShadowRequest.getValue();
        assertThat(localDocumentDeleteRequest.getShadowName(), is(SHADOW_NAME));
        assertThat(localDocumentDeleteRequest.getThingName(), is(THING_NAME));

        assertThat(syncInformationCaptor.getValue(), is(notNullValue()));
        assertThat(syncInformationCaptor.getValue().getLastSyncedDocument(), is(nullValue()));
        assertThat(syncInformationCaptor.getValue().getCloudVersion(), is(1L));
        assertThat(syncInformationCaptor.getValue().getLocalVersion(), is(10L));
        assertThat(syncInformationCaptor.getValue().getCloudUpdateTime(), is(greaterThanOrEqualTo(epochSeconds)));
        assertThat(syncInformationCaptor.getValue().getLastSyncTime(), is(greaterThanOrEqualTo(epochSeconds)));
        assertThat(syncInformationCaptor.getValue().getShadowName(), is(SHADOW_NAME));
        assertThat(syncInformationCaptor.getValue().getThingName(), is(THING_NAME));
        assertThat(syncInformationCaptor.getValue().isCloudDeleted(), is(false));
    }

    @Test
    void GIVEN_local_document_first_sync_and_existent_cloud_document_WHEN_execute_THEN_updates_local_document() throws RetryableException, SkipSyncRequestException, IOException {
        long epochSeconds = Instant.now().getEpochSecond();
        JsonNode expectedMergedDocument = JsonUtil.getPayloadJson(CLOUD_DOCUMENT).get();
        ((ObjectNode)expectedMergedDocument).remove(SHADOW_DOCUMENT_VERSION);
        when(mockDao.getShadowThing(anyString(), anyString())).thenReturn(Optional.empty());
        GetThingShadowResponse response = GetThingShadowResponse.builder()
                .payload(SdkBytes.fromByteArray(CLOUD_DOCUMENT))
                .build();
        when(mockIotDataPlaneClient.getThingShadow(any(GetThingShadowRequest.class))).thenReturn(response);
        when(mockDao.getShadowSyncInformation(anyString(), anyString())).thenReturn(Optional.of(SyncInformation.builder()
                .cloudUpdateTime(Instant.EPOCH.getEpochSecond())
                .thingName(THING_NAME)
                .shadowName(SHADOW_NAME)
                .cloudDeleted(false)
                .lastSyncedDocument(null)
                .cloudVersion(0L)
                .localVersion(0L)
                .lastSyncTime(Instant.EPOCH.getEpochSecond())
                .build()));
        when(mockUpdateThingShadowRequestHandler.handleRequest(localUpdateThingShadowRequestCaptor.capture(), anyString())).
                thenReturn(mock(UpdateThingShadowHandlerResponse.class));

        FullShadowSyncRequest fullShadowSyncRequest = new FullShadowSyncRequest(THING_NAME, SHADOW_NAME, mockDao, mockUpdateThingShadowRequestHandler, mockDeleteThingShadowRequestHandler, mockClientFactory);
        fullShadowSyncRequest.execute();

        verify(mockClientFactory, times(1)).getIotDataPlaneClient();
        verify(mockDao, times(1)).getShadowThing(anyString(), anyString());
        verify(mockDao, times(1)).updateSyncInformation(any());
        verify(mockUpdateThingShadowRequestHandler, times(1)).handleRequest(any(software.amazon.awssdk.aws.greengrass.model.UpdateThingShadowRequest.class), anyString());
        verify(mockIotDataPlaneClient, times(0)).updateThingShadow(any(UpdateThingShadowRequest.class));

        software.amazon.awssdk.aws.greengrass.model.UpdateThingShadowRequest localDocumentUpdateRequest = localUpdateThingShadowRequestCaptor.getValue();
        assertThat(localDocumentUpdateRequest.getShadowName(), is(SHADOW_NAME));
        assertThat(localDocumentUpdateRequest.getThingName(), is(THING_NAME));
        JsonNode actualLocalUpdateDocument = JsonUtil.getPayloadJson(localDocumentUpdateRequest.getPayload()).get();
        ((ObjectNode)actualLocalUpdateDocument).remove(SHADOW_DOCUMENT_METADATA);
        assertThat(actualLocalUpdateDocument, is(expectedMergedDocument));

        assertThat(syncInformationCaptor.getValue(), is(notNullValue()));
        JsonNode lastSyncedDocument = JsonUtil.getPayloadJson(syncInformationCaptor.getValue().getLastSyncedDocument()).get();
        ((ObjectNode)lastSyncedDocument).remove(SHADOW_DOCUMENT_METADATA);

        assertThat(lastSyncedDocument, is(expectedMergedDocument));
        assertThat(syncInformationCaptor.getValue().getCloudVersion(), is(5L));
        assertThat(syncInformationCaptor.getValue().getLocalVersion(), is(1L));
        assertThat(syncInformationCaptor.getValue().getCloudUpdateTime(), is(greaterThanOrEqualTo(epochSeconds)));
        assertThat(syncInformationCaptor.getValue().getLastSyncTime(), is(greaterThanOrEqualTo(epochSeconds)));
        assertThat(syncInformationCaptor.getValue().getShadowName(), is(SHADOW_NAME));
        assertThat(syncInformationCaptor.getValue().getThingName(), is(THING_NAME));
        assertThat(syncInformationCaptor.getValue().isCloudDeleted(), is(false));

    }

    @Test
    void GIVEN_cloud_document_first_sync_and_existent_local_document_WHEN_execute_THEN_updates_cloud_document(ExtensionContext context) throws RetryableException, SkipSyncRequestException, IOException {
        ignoreExceptionOfType(context, ResourceNotFoundException.class);
        long epochSeconds = Instant.now().getEpochSecond();
        JsonNode expectedMergedDocument = JsonUtil.getPayloadJson(LOCAL_DOCUMENT).get();
        ((ObjectNode)expectedMergedDocument).remove(SHADOW_DOCUMENT_VERSION);

        ShadowDocument shadowDocument = new ShadowDocument(LOCAL_DOCUMENT);
        when(mockIotDataPlaneClient.getThingShadow(any(GetThingShadowRequest.class))).thenThrow(ResourceNotFoundException.class);
        when(mockDao.getShadowThing(anyString(), anyString())).thenReturn(Optional.of(shadowDocument));
        when(mockDao.getShadowSyncInformation(anyString(), anyString())).thenReturn(Optional.of(SyncInformation.builder()
                .cloudUpdateTime(Instant.EPOCH.getEpochSecond())
                .thingName(THING_NAME)
                .shadowName(SHADOW_NAME)
                .cloudDeleted(false)
                .lastSyncedDocument(null)
                .cloudVersion(0L)
                .localVersion(0L)
                .lastSyncTime(Instant.EPOCH.getEpochSecond())
                .build()));
        when(mockIotDataPlaneClient.updateThingShadow(cloudUpdateThingShadowRequestCaptor.capture())).thenReturn(mock(UpdateThingShadowResponse.class));

        FullShadowSyncRequest fullShadowSyncRequest = new FullShadowSyncRequest(THING_NAME, SHADOW_NAME, mockDao, mockUpdateThingShadowRequestHandler, mockDeleteThingShadowRequestHandler, mockClientFactory);
        fullShadowSyncRequest.execute();

        verify(mockClientFactory, times(2)).getIotDataPlaneClient();
        verify(mockDao, times(1)).getShadowThing(anyString(), anyString());
        verify(mockDao, times(1)).updateSyncInformation(any());
        verify(mockUpdateThingShadowRequestHandler, times(0)).handleRequest(any(software.amazon.awssdk.aws.greengrass.model.UpdateThingShadowRequest.class), anyString());
        verify(mockDeleteThingShadowRequestHandler, times(0)).handleRequest(any(software.amazon.awssdk.aws.greengrass.model.DeleteThingShadowRequest.class), anyString());
        verify(mockIotDataPlaneClient, times(1)).getThingShadow(any(GetThingShadowRequest.class));
        verify(mockIotDataPlaneClient, times(1)).updateThingShadow(any(UpdateThingShadowRequest.class));
        verify(mockIotDataPlaneClient, times(0)).deleteThingShadow(any(DeleteThingShadowRequest.class));

        UpdateThingShadowRequest cloudDocumentUpdateRequest = cloudUpdateThingShadowRequestCaptor.getValue();
        assertThat(cloudDocumentUpdateRequest.shadowName(), is(SHADOW_NAME));
        assertThat(cloudDocumentUpdateRequest.thingName(), is(THING_NAME));
        JsonNode actualCloudUpdateDocument = JsonUtil.getPayloadJson(cloudDocumentUpdateRequest.payload().asByteArray()).get();
        ((ObjectNode)actualCloudUpdateDocument).remove(SHADOW_DOCUMENT_METADATA);
        assertThat(actualCloudUpdateDocument, is(expectedMergedDocument));

        assertThat(syncInformationCaptor.getValue(), is(notNullValue()));
        JsonNode lastSyncedDocument = JsonUtil.getPayloadJson(syncInformationCaptor.getValue().getLastSyncedDocument()).get();
        ((ObjectNode)lastSyncedDocument).remove(SHADOW_DOCUMENT_METADATA);

        assertThat(lastSyncedDocument, is(expectedMergedDocument));
        assertThat(syncInformationCaptor.getValue().getCloudVersion(), is(1L));
        assertThat(syncInformationCaptor.getValue().getLocalVersion(), is(10L));
        assertThat(syncInformationCaptor.getValue().getCloudUpdateTime(), is(greaterThanOrEqualTo(epochSeconds)));
        assertThat(syncInformationCaptor.getValue().getLastSyncTime(), is(greaterThanOrEqualTo(epochSeconds)));
        assertThat(syncInformationCaptor.getValue().getShadowName(), is(SHADOW_NAME));
        assertThat(syncInformationCaptor.getValue().getThingName(), is(THING_NAME));
        assertThat(syncInformationCaptor.getValue().isCloudDeleted(), is(false));
    }

    @Test
    void GIVEN_non_existent_cloud_document_and_existent_local_document_WHEN_execute_THEN_updates_sync_info_only(ExtensionContext context) throws RetryableException, SkipSyncRequestException {
        ignoreExceptionOfType(context, ResourceNotFoundException.class);
        long epochSeconds = Instant.now().getEpochSecond();

        when(mockIotDataPlaneClient.getThingShadow(any(GetThingShadowRequest.class))).thenThrow(ResourceNotFoundException.class);
        when(mockDao.getShadowThing(anyString(), anyString())).thenReturn(Optional.empty());
        when(mockDao.getShadowSyncInformation(anyString(), anyString())).thenReturn(Optional.of(SyncInformation.builder()
                .cloudUpdateTime(Instant.EPOCH.getEpochSecond())
                .thingName(THING_NAME)
                .shadowName(SHADOW_NAME)
                .cloudDeleted(false)
                .lastSyncedDocument(null)
                .cloudVersion(5L)
                .localVersion(10L)
                .lastSyncTime(Instant.EPOCH.getEpochSecond())
                .build()));

        FullShadowSyncRequest fullShadowSyncRequest = new FullShadowSyncRequest(THING_NAME, SHADOW_NAME, mockDao, mockUpdateThingShadowRequestHandler, mockDeleteThingShadowRequestHandler, mockClientFactory);
        fullShadowSyncRequest.execute();

        verify(mockClientFactory, times(1)).getIotDataPlaneClient();
        verify(mockDao, times(1)).getShadowThing(anyString(), anyString());
        verify(mockDao, times(1)).updateSyncInformation(any());
        verify(mockUpdateThingShadowRequestHandler, times(0)).handleRequest(any(software.amazon.awssdk.aws.greengrass.model.UpdateThingShadowRequest.class), anyString());
        verify(mockDeleteThingShadowRequestHandler, times(0)).handleRequest(any(software.amazon.awssdk.aws.greengrass.model.DeleteThingShadowRequest.class), anyString());
        verify(mockIotDataPlaneClient, times(1)).getThingShadow(any(GetThingShadowRequest.class));
        verify(mockIotDataPlaneClient, times(0)).updateThingShadow(any(UpdateThingShadowRequest.class));
        verify(mockIotDataPlaneClient, times(0)).deleteThingShadow(any(DeleteThingShadowRequest.class));

        assertThat(syncInformationCaptor.getValue(), is(notNullValue()));
        assertThat(syncInformationCaptor.getValue().getLastSyncedDocument(), is(nullValue()));
        assertThat(syncInformationCaptor.getValue().getCloudVersion(), is(5L));
        assertThat(syncInformationCaptor.getValue().getLocalVersion(), is(10L));
        assertThat(syncInformationCaptor.getValue().getCloudUpdateTime(), is(greaterThanOrEqualTo(epochSeconds)));
        assertThat(syncInformationCaptor.getValue().getLastSyncTime(), is(greaterThanOrEqualTo(epochSeconds)));
        assertThat(syncInformationCaptor.getValue().getShadowName(), is(SHADOW_NAME));
        assertThat(syncInformationCaptor.getValue().getThingName(), is(THING_NAME));
        assertThat(syncInformationCaptor.getValue().isCloudDeleted(), is(false));
    }

    @Test
    void GIVEN_nonexistent_sync_info_WHEN_execute_THEN_throws_skip_exception(ExtensionContext context) {
        ignoreExceptionOfType(context, SkipSyncRequestException.class);
        when(mockDao.getShadowSyncInformation(anyString(), anyString())).thenReturn(Optional.empty());

        FullShadowSyncRequest fullShadowSyncRequest = new FullShadowSyncRequest(THING_NAME, SHADOW_NAME, mockDao, mockUpdateThingShadowRequestHandler, mockDeleteThingShadowRequestHandler, mockClientFactory);
        SkipSyncRequestException thrown = assertThrows(SkipSyncRequestException.class, fullShadowSyncRequest::execute);
        assertThat(thrown.getMessage(), is("Unable to find sync information"));

        verify(mockClientFactory, times(0)).getIotDataPlaneClient();
        verify(mockDao, times(0)).getShadowThing(anyString(), anyString());
        verify(mockDao, times(0)).updateSyncInformation(any());
        verify(mockUpdateThingShadowRequestHandler, times(0)).handleRequest(any(software.amazon.awssdk.aws.greengrass.model.UpdateThingShadowRequest.class), anyString());
        verify(mockDeleteThingShadowRequestHandler, times(0)).handleRequest(any(software.amazon.awssdk.aws.greengrass.model.DeleteThingShadowRequest.class), anyString());
        verify(mockIotDataPlaneClient, times(0)).updateThingShadow(any(UpdateThingShadowRequest.class));
        verify(mockIotDataPlaneClient, times(0)).deleteThingShadow(any(DeleteThingShadowRequest.class));
    }

    @ParameterizedTest
    @ValueSource(classes = {ThrottlingException.class, ServiceUnavailableException.class, InternalFailureException.class})
    void GIVEN_updated_cloud_document_and_no_local_document_WHEN_execute_and_deleteThingShadow_throws_retryable_error_THEN_does_not_update_cloud_shadow_and_sync_information(Class clazz, ExtensionContext context) throws RetryableException, SkipSyncRequestException {
        ignoreExceptionOfType(context, clazz);
        long epochSecondsMinus60 = Instant.now().minusSeconds(60).getEpochSecond();
        GetThingShadowResponse response = GetThingShadowResponse.builder()
                .payload(SdkBytes.fromByteArray(CLOUD_DOCUMENT))
                .build();
        when(mockIotDataPlaneClient.getThingShadow(any(GetThingShadowRequest.class))).thenReturn(response);
        when(mockDao.getShadowThing(anyString(), anyString())).thenReturn(Optional.empty());
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
        when(mockIotDataPlaneClient.deleteThingShadow(cloudDeleteThingShadowRequest.capture())).thenThrow(clazz);

        FullShadowSyncRequest fullShadowSyncRequest = new FullShadowSyncRequest(THING_NAME, SHADOW_NAME, mockDao, mockUpdateThingShadowRequestHandler, mockDeleteThingShadowRequestHandler, mockClientFactory);
        RetryableException thrown = assertThrows(RetryableException.class, fullShadowSyncRequest::execute);
        assertThat(thrown.getCause(), is(instanceOf(clazz)));


        verify(mockClientFactory, times(2)).getIotDataPlaneClient();
        verify(mockDao, times(1)).getShadowThing(anyString(), anyString());
        verify(mockDao, times(0)).updateSyncInformation(any());
        verify(mockUpdateThingShadowRequestHandler, times(0)).handleRequest(any(software.amazon.awssdk.aws.greengrass.model.UpdateThingShadowRequest.class), anyString());
        verify(mockIotDataPlaneClient, times(0)).updateThingShadow(any(UpdateThingShadowRequest.class));
        verify(mockIotDataPlaneClient, times(1)).deleteThingShadow(any(DeleteThingShadowRequest.class));

        DeleteThingShadowRequest cloudDocumentDeleteRequest = cloudDeleteThingShadowRequest.getValue();
        assertThat(cloudDocumentDeleteRequest.shadowName(), is(SHADOW_NAME));
        assertThat(cloudDocumentDeleteRequest.thingName(), is(THING_NAME));
    }

    @ParameterizedTest
    @ValueSource(classes = {RequestEntityTooLargeException.class, InvalidRequestException.class, UnauthorizedException.class,
            MethodNotAllowedException.class, UnsupportedDocumentEncodingException.class, AwsServiceException.class, SdkClientException.class})
    void GIVEN_updated_cloud_document_and_no_local_document_WHEN_execute_and_deleteThingShadow_throws_skipable_error_THEN_does_not_update_cloud_shadow_and_sync_information(Class clazz, ExtensionContext context) throws IOException {
        ignoreExceptionOfType(context, clazz);
        long epochSecondsMinus60 = Instant.now().minusSeconds(60).getEpochSecond();
        GetThingShadowResponse response = GetThingShadowResponse.builder()
                .payload(SdkBytes.fromByteArray(CLOUD_DOCUMENT))
                .build();
        when(mockIotDataPlaneClient.getThingShadow(any(GetThingShadowRequest.class))).thenReturn(response);
        when(mockDao.getShadowThing(anyString(), anyString())).thenReturn(Optional.empty());
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
        when(mockIotDataPlaneClient.deleteThingShadow(cloudDeleteThingShadowRequest.capture())).thenThrow(clazz);

        FullShadowSyncRequest fullShadowSyncRequest = new FullShadowSyncRequest(THING_NAME, SHADOW_NAME, mockDao, mockUpdateThingShadowRequestHandler, mockDeleteThingShadowRequestHandler, mockClientFactory);
        SkipSyncRequestException thrown = assertThrows(SkipSyncRequestException.class, fullShadowSyncRequest::execute);
        assertThat(thrown.getCause(), is(instanceOf(clazz)));


        verify(mockClientFactory, times(2)).getIotDataPlaneClient();
        verify(mockDao, times(1)).getShadowThing(anyString(), anyString());
        verify(mockDao, times(0)).updateSyncInformation(any());
        verify(mockUpdateThingShadowRequestHandler, times(0)).handleRequest(any(software.amazon.awssdk.aws.greengrass.model.UpdateThingShadowRequest.class), anyString());
        verify(mockIotDataPlaneClient, times(0)).updateThingShadow(any(UpdateThingShadowRequest.class));
        verify(mockIotDataPlaneClient, times(1)).deleteThingShadow(any(DeleteThingShadowRequest.class));

        DeleteThingShadowRequest cloudDocumentDeleteRequest = cloudDeleteThingShadowRequest.getValue();
        assertThat(cloudDocumentDeleteRequest.shadowName(), is(SHADOW_NAME));
        assertThat(cloudDocumentDeleteRequest.thingName(), is(THING_NAME));
    }

    @ParameterizedTest
    @ValueSource(classes = {ShadowManagerDataException.class, UnauthorizedError.class, InvalidArgumentsError.class, ServiceError.class})
    void GIVEN_updated_local_document_and_no_cloud_document_WHEN_execute_and_deleteThingShadow_throws_skipable_error_THEN_does_not_local_cloud_shadow_and_sync_information(Class<Throwable> clazz, ExtensionContext context) throws IOException {
        ignoreExceptionOfType(context, ResourceNotFoundException.class);
        ignoreExceptionOfType(context, clazz);
        long epochSecondsMinus60 = Instant.now().minusSeconds(60).getEpochSecond();
        ShadowDocument shadowDocument = new ShadowDocument(LOCAL_DOCUMENT);
        when(mockIotDataPlaneClient.getThingShadow(any(GetThingShadowRequest.class))).thenThrow(ResourceNotFoundException.class);
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
        when(mockDeleteThingShadowRequestHandler.handleRequest(localDeleteThingShadowRequest.capture(), anyString())).thenThrow(mock(clazz));

        FullShadowSyncRequest fullShadowSyncRequest = new FullShadowSyncRequest(THING_NAME, SHADOW_NAME, mockDao, mockUpdateThingShadowRequestHandler, mockDeleteThingShadowRequestHandler, mockClientFactory);
        SkipSyncRequestException thrown = assertThrows(SkipSyncRequestException.class, fullShadowSyncRequest::execute);
        assertThat(thrown.getCause(), is(instanceOf(clazz)));

        verify(mockClientFactory, times(1)).getIotDataPlaneClient();
        verify(mockDao, times(1)).getShadowThing(anyString(), anyString());
        verify(mockDao, times(0)).updateSyncInformation(any());
        verify(mockUpdateThingShadowRequestHandler, times(0)).handleRequest(any(software.amazon.awssdk.aws.greengrass.model.UpdateThingShadowRequest.class), anyString());
        verify(mockDeleteThingShadowRequestHandler, times(1)).handleRequest(any(software.amazon.awssdk.aws.greengrass.model.DeleteThingShadowRequest.class), anyString());
        verify(mockIotDataPlaneClient, times(0)).updateThingShadow(any(UpdateThingShadowRequest.class));
        verify(mockIotDataPlaneClient, times(0)).deleteThingShadow(any(DeleteThingShadowRequest.class));

        software.amazon.awssdk.aws.greengrass.model.DeleteThingShadowRequest localDocumentDeleteRequest = localDeleteThingShadowRequest.getValue();
        assertThat(localDocumentDeleteRequest.getShadowName(), is(SHADOW_NAME));
        assertThat(localDocumentDeleteRequest.getThingName(), is(THING_NAME));
    }

    @ParameterizedTest
    @ValueSource(classes = {ThrottlingException.class, ServiceUnavailableException.class, InternalFailureException.class})
    void GIVEN_updated_cloud_document_and_no_local_document_WHEN_execute_and_getThingShadow_throws_retryable_error_THEN_does_not_update_cloud_shadow_and_sync_information(Class clazz, ExtensionContext context) throws RetryableException, SkipSyncRequestException {
        ignoreExceptionOfType(context, clazz);
        long epochSecondsMinus60 = Instant.now().minusSeconds(60).getEpochSecond();
        when(mockIotDataPlaneClient.getThingShadow(any(GetThingShadowRequest.class))).thenThrow(clazz);
        when(mockDao.getShadowThing(anyString(), anyString())).thenReturn(Optional.empty());
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

        FullShadowSyncRequest fullShadowSyncRequest = new FullShadowSyncRequest(THING_NAME, SHADOW_NAME, mockDao, mockUpdateThingShadowRequestHandler, mockDeleteThingShadowRequestHandler, mockClientFactory);
        RetryableException thrown = assertThrows(RetryableException.class, fullShadowSyncRequest::execute);
        assertThat(thrown.getCause(), is(instanceOf(clazz)));


        verify(mockClientFactory, times(1)).getIotDataPlaneClient();
        verify(mockDao, times(1)).getShadowThing(anyString(), anyString());
        verify(mockDao, times(0)).updateSyncInformation(any());
        verify(mockUpdateThingShadowRequestHandler, times(0)).handleRequest(any(software.amazon.awssdk.aws.greengrass.model.UpdateThingShadowRequest.class), anyString());
        verify(mockIotDataPlaneClient, times(0)).updateThingShadow(any(UpdateThingShadowRequest.class));
        verify(mockIotDataPlaneClient, times(0)).deleteThingShadow(any(DeleteThingShadowRequest.class));
    }

    @ParameterizedTest
    @ValueSource(classes = {RequestEntityTooLargeException.class, InvalidRequestException.class, UnauthorizedException.class,
            MethodNotAllowedException.class, UnsupportedDocumentEncodingException.class, AwsServiceException.class, SdkClientException.class})
    void GIVEN_updated_cloud_document_and_no_local_document_WHEN_execute_and_getThingShadow_throws_skipable_error_THEN_does_not_update_cloud_shadow_and_sync_information(Class clazz, ExtensionContext context) throws IOException {
        ignoreExceptionOfType(context, clazz);
        long epochSecondsMinus60 = Instant.now().minusSeconds(60).getEpochSecond();
        when(mockIotDataPlaneClient.getThingShadow(any(GetThingShadowRequest.class))).thenThrow(clazz);
        when(mockDao.getShadowThing(anyString(), anyString())).thenReturn(Optional.empty());
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

        FullShadowSyncRequest fullShadowSyncRequest = new FullShadowSyncRequest(THING_NAME, SHADOW_NAME, mockDao, mockUpdateThingShadowRequestHandler, mockDeleteThingShadowRequestHandler, mockClientFactory);
        SkipSyncRequestException thrown = assertThrows(SkipSyncRequestException.class, fullShadowSyncRequest::execute);
        assertThat(thrown.getCause(), is(instanceOf(clazz)));


        verify(mockClientFactory, times(1)).getIotDataPlaneClient();
        verify(mockDao, times(1)).getShadowThing(anyString(), anyString());
        verify(mockDao, times(0)).updateSyncInformation(any());
        verify(mockUpdateThingShadowRequestHandler, times(0)).handleRequest(any(software.amazon.awssdk.aws.greengrass.model.UpdateThingShadowRequest.class), anyString());
        verify(mockIotDataPlaneClient, times(0)).updateThingShadow(any(UpdateThingShadowRequest.class));
        verify(mockIotDataPlaneClient, times(0)).deleteThingShadow(any(DeleteThingShadowRequest.class));
    }

    @ParameterizedTest
    @ValueSource(classes = {ThrottlingException.class, ServiceUnavailableException.class, InternalFailureException.class})
    void GIVEN_updated_cloud_document_and_no_local_document_WHEN_execute_and_updateThingShadow_throws_retryable_error_THEN_does_not_update_cloud_shadow_and_sync_information(Class clazz, ExtensionContext context) throws RetryableException, SkipSyncRequestException, IOException {
        ignoreExceptionOfType(context, clazz);
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
                thenReturn(mock(UpdateThingShadowHandlerResponse.class));
        when(mockIotDataPlaneClient.updateThingShadow(cloudUpdateThingShadowRequestCaptor.capture())).thenThrow(clazz);

        FullShadowSyncRequest fullShadowSyncRequest = new FullShadowSyncRequest(THING_NAME, SHADOW_NAME, mockDao, mockUpdateThingShadowRequestHandler, mockDeleteThingShadowRequestHandler, mockClientFactory);
        RetryableException thrown = assertThrows(RetryableException.class, fullShadowSyncRequest::execute);
        assertThat(thrown.getCause(), is(instanceOf(clazz)));

        verify(mockClientFactory, times(2)).getIotDataPlaneClient();
        verify(mockDao, times(1)).getShadowThing(anyString(), anyString());
        verify(mockDao, times(0)).updateSyncInformation(any());
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
    }

    @Test
    void GIVEN_updated_cloud_document_and_local_document_WHEN_execute_and_updateThingShadow_throws_ConflictException_THEN_does_not_update_cloud_shadow_and_sync_information(ExtensionContext context) throws IOException {
        ignoreExceptionOfType(context, ConflictException.class);
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
                thenReturn(mock(UpdateThingShadowHandlerResponse.class));
        when(mockIotDataPlaneClient.updateThingShadow(cloudUpdateThingShadowRequestCaptor.capture())).thenThrow(ConflictException.class);

        FullShadowSyncRequest fullShadowSyncRequest = new FullShadowSyncRequest(THING_NAME, SHADOW_NAME, mockDao, mockUpdateThingShadowRequestHandler, mockDeleteThingShadowRequestHandler, mockClientFactory);
        assertThrows(ConflictException.class, fullShadowSyncRequest::execute);

        verify(mockClientFactory, times(2)).getIotDataPlaneClient();
        verify(mockDao, times(1)).getShadowThing(anyString(), anyString());
        verify(mockDao, times(0)).updateSyncInformation(any());
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
    }

    @Test
    void GIVEN_updated_local_document_and_cloud_document_WHEN_execute_and_updateThingShadow_throws_ConflictError_THEN_does_not_update_local_shadow_and_sync_information(ExtensionContext context) throws IOException {
        ignoreExceptionOfType(context, ConflictError.class);
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
                thenThrow(ConflictError.class);

        FullShadowSyncRequest fullShadowSyncRequest = new FullShadowSyncRequest(THING_NAME, SHADOW_NAME, mockDao, mockUpdateThingShadowRequestHandler, mockDeleteThingShadowRequestHandler, mockClientFactory);
        assertThrows(ConflictError.class, fullShadowSyncRequest::execute);

        verify(mockClientFactory, times(1)).getIotDataPlaneClient();
        verify(mockDao, times(1)).getShadowThing(anyString(), anyString());
        verify(mockDao, times(0)).updateSyncInformation(any());
        verify(mockUpdateThingShadowRequestHandler, times(1)).handleRequest(any(software.amazon.awssdk.aws.greengrass.model.UpdateThingShadowRequest.class), anyString());
        verify(mockIotDataPlaneClient, times(0)).updateThingShadow(any(UpdateThingShadowRequest.class));

        software.amazon.awssdk.aws.greengrass.model.UpdateThingShadowRequest localDocumentUpdateRequest = localUpdateThingShadowRequestCaptor.getValue();
        assertThat(localDocumentUpdateRequest.getShadowName(), is(SHADOW_NAME));
        assertThat(localDocumentUpdateRequest.getThingName(), is(THING_NAME));
        JsonNode actualLocalUpdateDocument = JsonUtil.getPayloadJson(localDocumentUpdateRequest.getPayload()).get();
        assertThat(actualLocalUpdateDocument.get(SHADOW_DOCUMENT_VERSION).asLong(), is(10L));
        ((ObjectNode)actualLocalUpdateDocument).remove(SHADOW_DOCUMENT_VERSION);
        assertThat(actualLocalUpdateDocument, is(expectedMergedDocument));
    }

    @ParameterizedTest
    @ValueSource(classes = {RequestEntityTooLargeException.class, InvalidRequestException.class, UnauthorizedException.class,
            MethodNotAllowedException.class, UnsupportedDocumentEncodingException.class, AwsServiceException.class, SdkClientException.class})
    void GIVEN_updated_cloud_document_and_local_document_WHEN_execute_and_updateThingShadow_throws_skipable_error_THEN_does_not_update_cloud_shadow_and_sync_information(Class clazz, ExtensionContext context) throws IOException {
        ignoreExceptionOfType(context, clazz);
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
                thenReturn(mock(UpdateThingShadowHandlerResponse.class));
        when(mockIotDataPlaneClient.updateThingShadow(cloudUpdateThingShadowRequestCaptor.capture())).thenThrow(clazz);

        FullShadowSyncRequest fullShadowSyncRequest = new FullShadowSyncRequest(THING_NAME, SHADOW_NAME, mockDao, mockUpdateThingShadowRequestHandler, mockDeleteThingShadowRequestHandler, mockClientFactory);
        SkipSyncRequestException thrown = assertThrows(SkipSyncRequestException.class, fullShadowSyncRequest::execute);
        assertThat(thrown.getCause(), is(instanceOf(clazz)));

        verify(mockClientFactory, times(2)).getIotDataPlaneClient();
        verify(mockDao, times(1)).getShadowThing(anyString(), anyString());
        verify(mockDao, times(0)).updateSyncInformation(any());
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
    }

    @ParameterizedTest
    @ValueSource(classes = {ShadowManagerDataException.class, UnauthorizedError.class, InvalidArgumentsError.class, ServiceError.class})
    void GIVEN_updated_local_document_and_cloud_document_WHEN_execute_and_updateThingShadow_throws_skipable_error_THEN_does_not_update_cloud_shadow_and_sync_information(Class<Throwable> clazz, ExtensionContext context) throws IOException, NoSuchMethodException, IllegalAccessException, InvocationTargetException, InstantiationException {
        ignoreExceptionOfType(context, clazz);
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
        when(mockUpdateThingShadowRequestHandler.handleRequest(localUpdateThingShadowRequestCaptor.capture(), anyString())).thenThrow(mock(clazz));

        FullShadowSyncRequest fullShadowSyncRequest = new FullShadowSyncRequest(THING_NAME, SHADOW_NAME, mockDao, mockUpdateThingShadowRequestHandler, mockDeleteThingShadowRequestHandler, mockClientFactory);
        SkipSyncRequestException thrown = assertThrows(SkipSyncRequestException.class, fullShadowSyncRequest::execute);
        assertThat(thrown.getCause(), is(instanceOf(clazz)));

        verify(mockClientFactory, times(1)).getIotDataPlaneClient();
        verify(mockDao, times(1)).getShadowThing(anyString(), anyString());
        verify(mockDao, times(0)).updateSyncInformation(any());
        verify(mockUpdateThingShadowRequestHandler, times(1)).handleRequest(any(software.amazon.awssdk.aws.greengrass.model.UpdateThingShadowRequest.class), anyString());
        verify(mockIotDataPlaneClient, times(0)).updateThingShadow(any(UpdateThingShadowRequest.class));

        software.amazon.awssdk.aws.greengrass.model.UpdateThingShadowRequest localDocumentUpdateRequest = localUpdateThingShadowRequestCaptor.getValue();
        assertThat(localDocumentUpdateRequest.getShadowName(), is(SHADOW_NAME));
        assertThat(localDocumentUpdateRequest.getThingName(), is(THING_NAME));
        JsonNode actualLocalUpdateDocument = JsonUtil.getPayloadJson(localDocumentUpdateRequest.getPayload()).get();
        assertThat(actualLocalUpdateDocument.get(SHADOW_DOCUMENT_VERSION).asLong(), is(10L));
        ((ObjectNode)actualLocalUpdateDocument).remove(SHADOW_DOCUMENT_VERSION);
        assertThat(actualLocalUpdateDocument, is(expectedMergedDocument));
    }

    @Test
    void GIVEN_bad_last_synced_document_WHEN_execute_THEN_throws_skip_exception(ExtensionContext context) throws RetryableException, SkipSyncRequestException, IOException {
        ignoreExceptionOfType(context, IOException.class);
        long epochSecondsMinus60 = Instant.now().minusSeconds(60).getEpochSecond();
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
                .lastSyncedDocument(BAD_DOCUMENT)
                .cloudVersion(1L)
                .localVersion(1L)
                .lastSyncTime(epochSecondsMinus60)
                .build()));

        FullShadowSyncRequest fullShadowSyncRequest = new FullShadowSyncRequest(THING_NAME, SHADOW_NAME, mockDao, mockUpdateThingShadowRequestHandler, mockDeleteThingShadowRequestHandler, mockClientFactory);
        SkipSyncRequestException thrown = assertThrows(SkipSyncRequestException.class, fullShadowSyncRequest::execute);
        assertThat(thrown.getCause(), is(instanceOf(IOException.class)));

        verify(mockClientFactory, times(1)).getIotDataPlaneClient();
        verify(mockDao, times(1)).getShadowThing(anyString(), anyString());
        verify(mockDao, times(0)).updateSyncInformation(any());
        verify(mockUpdateThingShadowRequestHandler, times(0)).handleRequest(any(software.amazon.awssdk.aws.greengrass.model.UpdateThingShadowRequest.class), anyString());
        verify(mockIotDataPlaneClient, times(0)).updateThingShadow(any(UpdateThingShadowRequest.class));
    }
 }
