/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.shadowmanager.sync.model;

import com.aws.greengrass.shadowmanager.ShadowManagerDAO;
import com.aws.greengrass.shadowmanager.exception.RetryableException;
import com.aws.greengrass.shadowmanager.exception.SkipSyncRequestException;
import com.aws.greengrass.shadowmanager.model.ShadowDocument;
import com.aws.greengrass.shadowmanager.model.dao.SyncInformation;
import com.aws.greengrass.shadowmanager.sync.IotDataPlaneClientFactory;
import com.aws.greengrass.shadowmanager.util.JsonUtil;
import com.aws.greengrass.testcommons.testutilities.GGExtension;
import com.fasterxml.jackson.databind.JsonNode;
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
import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.services.iotdataplane.IotDataPlaneClient;
import software.amazon.awssdk.services.iotdataplane.model.ConflictException;
import software.amazon.awssdk.services.iotdataplane.model.InternalFailureException;
import software.amazon.awssdk.services.iotdataplane.model.InvalidRequestException;
import software.amazon.awssdk.services.iotdataplane.model.MethodNotAllowedException;
import software.amazon.awssdk.services.iotdataplane.model.ResourceNotFoundException;
import software.amazon.awssdk.services.iotdataplane.model.ServiceUnavailableException;
import software.amazon.awssdk.services.iotdataplane.model.ThrottlingException;
import software.amazon.awssdk.services.iotdataplane.model.UnauthorizedException;
import software.amazon.awssdk.services.iotdataplane.model.UnsupportedDocumentEncodingException;
import software.amazon.awssdk.services.iotdataplane.model.UpdateThingShadowRequest;

import java.io.IOException;
import java.time.Instant;
import java.util.Optional;

import static com.aws.greengrass.shadowmanager.TestUtils.SAMPLE_EXCEPTION_MESSAGE;
import static com.aws.greengrass.shadowmanager.TestUtils.SHADOW_NAME;
import static com.aws.greengrass.shadowmanager.TestUtils.THING_NAME;
import static com.aws.greengrass.testcommons.testutilities.ExceptionLogProtector.ignoreExceptionOfType;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith({MockitoExtension.class, GGExtension.class})
class CloudUpdateSyncRequestTest {
    private static final byte[] BASE_DOCUMENT = "{\"version\": 1, \"state\": {\"reported\": {\"name\": \"The Beatles\"}}}".getBytes();
    private static final byte[] UPDATE_DOCUMENT = "{\"version\": 1, \"state\": {\"reported\": {\"name\": \"The Rolling Stones\"}}}".getBytes();

    private JsonNode baseDocumentJson;
    @Mock
    private ShadowManagerDAO mockDao;
    @Mock
    private IotDataPlaneClientFactory mockClientFactory;
    @Mock
    private IotDataPlaneClient mockIotDataPlaneClient;
    @Captor
    private ArgumentCaptor<SyncInformation> syncInformationCaptor;
    @Mock
    private SyncContext mockContext;

    @BeforeEach
    void setup() throws IOException {
        lenient().when(mockClientFactory.getIotDataPlaneClient()).thenReturn(mockIotDataPlaneClient);
        lenient().when(mockDao.updateSyncInformation(syncInformationCaptor.capture())).thenReturn(true);
        baseDocumentJson = JsonUtil.getPayloadJson(BASE_DOCUMENT).get();
        lenient().when(mockContext.getDao()).thenReturn(mockDao);
        lenient().when(mockContext.getIotDataPlaneClientFactory()).thenReturn(mockClientFactory);
        lenient().when(mockDao.getShadowSyncInformation(anyString(), anyString())).thenReturn(Optional.of(SyncInformation.builder().build()));
    }

    @Test
    void GIVEN_good_cloud_update_request_WHEN_execute_THEN_successfully_updates_cloud_shadow_and_sync_information() throws Exception {
        long epochSeconds = Instant.now().getEpochSecond();
        long epochSecondsMinus60 = Instant.now().minusSeconds(60).getEpochSecond();
        ShadowDocument shadowDocument = new ShadowDocument(BASE_DOCUMENT);
        when(mockDao.getShadowThing(anyString(), anyString())).thenReturn(Optional.of(shadowDocument));
        when(mockDao.getShadowSyncInformation(anyString(), anyString())).thenReturn(Optional.of(SyncInformation.builder()
                .cloudUpdateTime(epochSecondsMinus60)
                .thingName(THING_NAME)
                .shadowName(SHADOW_NAME)
                .cloudDeleted(false)
                .lastSyncedDocument(null)
                .cloudVersion(5L)
                .lastSyncTime(epochSecondsMinus60)
                .build()));

        CloudUpdateSyncRequest request = new CloudUpdateSyncRequest(THING_NAME, SHADOW_NAME, baseDocumentJson);

        request.execute(mockContext);

        verify(mockClientFactory, times(1)).getIotDataPlaneClient();
        verify(mockDao, times(1)).getShadowThing(anyString(), anyString());
        verify(mockDao, times(1)).updateSyncInformation(any());
        verify(mockIotDataPlaneClient, times(1)).updateThingShadow(any(UpdateThingShadowRequest.class));

        assertThat(syncInformationCaptor.getValue(), is(notNullValue()));
        assertThat(syncInformationCaptor.getValue().getLastSyncedDocument(), is(JsonUtil.getPayloadBytes(shadowDocument.toJson(false))));
        assertThat(syncInformationCaptor.getValue().getCloudVersion(), is(6L));
        assertThat(syncInformationCaptor.getValue().getCloudUpdateTime(), is(greaterThanOrEqualTo(epochSeconds)));
        assertThat(syncInformationCaptor.getValue().getLastSyncTime(), is(greaterThanOrEqualTo(epochSeconds)));
        assertThat(syncInformationCaptor.getValue().getShadowName(), is(SHADOW_NAME));
        assertThat(syncInformationCaptor.getValue().getThingName(), is(THING_NAME));
        assertThat(syncInformationCaptor.getValue().isCloudDeleted(), is(false));
    }

    @Test
    void GIVEN_cloud_update_request_for_non_existent_shadow_WHEN_execute_THEN_does_not_update_cloud_shadow_and_sync_information() throws Exception {
        when(mockDao.getShadowThing(anyString(), anyString())).thenReturn(Optional.empty());
        CloudUpdateSyncRequest request = new CloudUpdateSyncRequest(THING_NAME, SHADOW_NAME, baseDocumentJson);

        request.execute(mockContext);

        verify(mockClientFactory, times(0)).getIotDataPlaneClient();
        verify(mockDao, times(1)).getShadowThing(anyString(), anyString());
        verify(mockDao, times(0)).updateSyncInformation(any());
        verify(mockIotDataPlaneClient, times(0)).updateThingShadow(any(UpdateThingShadowRequest.class));
    }

    @ParameterizedTest
    @ValueSource(classes = {ThrottlingException.class, ServiceUnavailableException.class, InternalFailureException.class})
    void GIVEN_bad_cloud_update_request_WHEN_execute_and_updateShadow_throws_retryable_error_THEN_does_not_update_cloud_shadow_and_sync_information(Class clazz, ExtensionContext context) throws IOException {
        ignoreExceptionOfType(context, clazz);
        ShadowDocument shadowDocument = new ShadowDocument(BASE_DOCUMENT);
        when(mockDao.getShadowThing(anyString(), anyString())).thenReturn(Optional.of(shadowDocument));
        when(mockIotDataPlaneClient.updateThingShadow(any(UpdateThingShadowRequest.class))).thenThrow(clazz);
        CloudUpdateSyncRequest request = new CloudUpdateSyncRequest(THING_NAME, SHADOW_NAME, baseDocumentJson);

        RetryableException thrown = assertThrows(RetryableException.class, () -> request.execute(mockContext));
        assertThat(thrown.getCause(), is(instanceOf(clazz)));

        verify(mockClientFactory, times(1)).getIotDataPlaneClient();
        verify(mockDao, times(1)).getShadowThing(anyString(), anyString());
        verify(mockDao, times(0)).updateSyncInformation(any());
        verify(mockIotDataPlaneClient, times(1)).updateThingShadow(any(UpdateThingShadowRequest.class));
    }

    @Test
    void GIVEN_bad_cloud_update_request_WHEN_execute_and_updateShadow_throws_conflict_exception_THEN_does_not_update_cloud_shadow_and_sync_information(ExtensionContext context) throws IOException {
        ignoreExceptionOfType(context, ConflictException.class);
        ShadowDocument shadowDocument = new ShadowDocument(BASE_DOCUMENT);
        when(mockDao.getShadowThing(anyString(), anyString())).thenReturn(Optional.of(shadowDocument));
        when(mockIotDataPlaneClient.updateThingShadow(any(UpdateThingShadowRequest.class)))
                .thenThrow(ConflictException.builder().message(SAMPLE_EXCEPTION_MESSAGE).build());
        CloudUpdateSyncRequest request = new CloudUpdateSyncRequest(THING_NAME, SHADOW_NAME, baseDocumentJson);

        ConflictException thrown = assertThrows(ConflictException.class, () -> request.execute(mockContext));
        assertThat(thrown.getMessage(), is(equalTo(SAMPLE_EXCEPTION_MESSAGE)));

        verify(mockClientFactory, times(1)).getIotDataPlaneClient();
        verify(mockDao, times(1)).getShadowThing(anyString(), anyString());
        verify(mockDao, times(0)).updateSyncInformation(any());
        verify(mockIotDataPlaneClient, times(1)).updateThingShadow(any(UpdateThingShadowRequest.class));
    }

    @ParameterizedTest
    @ValueSource(classes = {ResourceNotFoundException.class, InvalidRequestException.class, UnauthorizedException.class,
            MethodNotAllowedException.class, UnsupportedDocumentEncodingException.class, AwsServiceException.class, SdkClientException.class})
    void GIVEN_bad_cloud_update_request_WHEN_execute_and_updateShadow_throws_skipable_error_THEN_does_not_update_cloud_shadow_and_sync_information(Class clazz, ExtensionContext context) throws IOException {
        ignoreExceptionOfType(context, clazz);
        ShadowDocument shadowDocument = new ShadowDocument(BASE_DOCUMENT);
        when(mockDao.getShadowThing(anyString(), anyString())).thenReturn(Optional.of(shadowDocument));
        when(mockIotDataPlaneClient.updateThingShadow(any(UpdateThingShadowRequest.class))).thenThrow(clazz);
        CloudUpdateSyncRequest request = new CloudUpdateSyncRequest(THING_NAME, SHADOW_NAME, baseDocumentJson);

        SkipSyncRequestException thrown = assertThrows(SkipSyncRequestException.class,
                () -> request.execute(mockContext));
        assertThat(thrown.getCause(), is(instanceOf(clazz)));

        verify(mockClientFactory, times(1)).getIotDataPlaneClient();
        verify(mockDao, times(1)).getShadowThing(anyString(), anyString());
        verify(mockDao, times(0)).updateSyncInformation(any());
        verify(mockIotDataPlaneClient, times(1)).updateThingShadow(any(UpdateThingShadowRequest.class));
    }

    @Test
    void GIVEN_new_values_WHEN_merge_THEN_document_merged() throws IOException {

        CloudUpdateSyncRequest request = new CloudUpdateSyncRequest(THING_NAME, SHADOW_NAME, baseDocumentJson);
        JsonNode updateDocument = JsonUtil.getPayloadJson(UPDATE_DOCUMENT).get();
        CloudUpdateSyncRequest other = new CloudUpdateSyncRequest(THING_NAME, SHADOW_NAME, updateDocument);
        request.merge(other);

        assertThat(request.updateDocument, is(updateDocument));
    }

    @Test
    void GIVEN_no_change_WHEN_execute_THEN_does_not_update_cloud_shadow_and_sync_information() throws IOException {
        ShadowDocument shadowDocument = new ShadowDocument(BASE_DOCUMENT);
        when(mockDao.getShadowThing(anyString(), anyString())).thenReturn(Optional.of(shadowDocument));

        when(mockDao.getShadowSyncInformation(anyString(), anyString())).thenReturn(Optional.of(
                SyncInformation.builder()
                        .lastSyncedDocument(BASE_DOCUMENT)
                        .build()));
        CloudUpdateSyncRequest request = new CloudUpdateSyncRequest(THING_NAME, SHADOW_NAME, baseDocumentJson);

        assertDoesNotThrow(() -> request.execute(mockContext));

        verify(mockIotDataPlaneClient, never()).updateThingShadow(any(UpdateThingShadowRequest.class));
        verify(mockDao, never()).updateSyncInformation(any());
    }

    @Test
    void GIVEN_different_cloud_update_WHEN_isUpdateNecessary_THEN_returns_false() throws IOException {
        JsonNode j1 = JsonUtil.getPayloadJson("{\"state\":{\"reported\":{\"color\":{\"r\":255,\"g\":0,\"b\":0},\"SomeKey\":\"SomeValue\"}},\"metadata\":{\"reported\":{\"color\":{\"r\":{\"timestamp\":1619722006},\"g\":{\"timestamp\":1619722006},\"b\":{\"timestamp\":1619722006}},\"SomeKey\":{\"timestamp\":1619722006}}},\"version\":1,\"timestamp\":1619722006}".getBytes()).get();
        JsonNode j2 = JsonUtil.getPayloadJson("{\"state\":{\"reported\":{\"color\":{\"r\":255,\"g\":255,\"b\":255},\"SomeKey\":\"SomeValue\"}},\"metadata\":{\"reported\":{\"color\":{\"r\":{\"timestamp\":1619722006},\"g\":{\"timestamp\":1619722006},\"b\":{\"timestamp\":1619722006}},\"SomeKey\":{\"timestamp\":1619722006}}},\"version\":1,\"timestamp\":1619722006}".getBytes()).get();
        CloudUpdateSyncRequest request = new CloudUpdateSyncRequest(THING_NAME, SHADOW_NAME, baseDocumentJson);
        assertTrue(request.isUpdateNecessary(j1, j2));
    }

    @Test
    void GIVEN_same_cloud_update_WHEN_isUpdateNecessary_THEN_returns_false() throws IOException {
        JsonNode j1 = JsonUtil.getPayloadJson("{\"state\":{\"reported\":{\"color\":{\"r\":255,\"g\":255,\"b\":255},\"SomeKey\":\"SomeValue\"}},\"metadata\":{\"reported\":{\"color\":{\"r\":{\"timestamp\":1619722006},\"g\":{\"timestamp\":1619722006},\"b\":{\"timestamp\":1619722006}},\"SomeKey\":{\"timestamp\":1619722006}}},\"version\":1,\"timestamp\":1619722006}".getBytes()).get();
        JsonNode j2 = JsonUtil.getPayloadJson("{\"state\":{\"reported\":{\"color\":{\"r\":255,\"g\":255,\"b\":255},\"SomeKey\":\"SomeValue\"}},\"metadata\":{\"reported\":{\"color\":{\"r\":{\"timestamp\":1619722006},\"g\":{\"timestamp\":1619722006},\"b\":{\"timestamp\":1619722006}},\"SomeKey\":{\"timestamp\":1619722006}}},\"version\":1,\"timestamp\":1619722006}".getBytes()).get();
        CloudUpdateSyncRequest request = new CloudUpdateSyncRequest(THING_NAME, SHADOW_NAME, baseDocumentJson);
        assertFalse(request.isUpdateNecessary(j1, j2));
    }
}
