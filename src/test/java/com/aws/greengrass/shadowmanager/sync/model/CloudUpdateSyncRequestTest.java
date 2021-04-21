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

import static com.aws.greengrass.shadowmanager.TestUtils.SHADOW_NAME;
import static com.aws.greengrass.shadowmanager.TestUtils.THING_NAME;
import static com.aws.greengrass.testcommons.testutilities.ExceptionLogProtector.ignoreExceptionOfType;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith({MockitoExtension.class, GGExtension.class})
class CloudUpdateSyncRequestTest {
    private static final byte[] BASE_DOCUMENT = "{\"version\": 1, \"state\": {\"reported\": {\"name\": \"The Beatles\"}}}".getBytes();
    private JsonNode baseDocumentJson;
    @Mock
    private ShadowManagerDAO mockDao;
    @Mock
    private IotDataPlaneClientFactory mockClientFactory;
    @Mock
    private IotDataPlaneClient mockIotDataPlaneClient;
    @Captor
    private ArgumentCaptor<SyncInformation> syncInformationCaptor;

    @BeforeEach
    void setup() throws IOException {
        lenient().when(mockClientFactory.getIotDataPlaneClient()).thenReturn(mockIotDataPlaneClient);
        lenient().when(mockDao.updateSyncInformation(syncInformationCaptor.capture())).thenReturn(true);
        baseDocumentJson = JsonUtil.getPayloadJson(BASE_DOCUMENT).get();
    }

    @Test
    void GIVEN_good_cloud_update_request_WHEN_execute_THEN_successfully_updates_cloud_shadow_and_sync_information() throws RetryableException, SkipSyncRequestException, IOException {
        long epochSeconds = Instant.now().getEpochSecond();
        long epochSecondsMinus60 = Instant.now().minusSeconds(60).getEpochSecond();
        ShadowDocument shadowDocument = new ShadowDocument(BASE_DOCUMENT);
        when(mockDao.getShadowThing(anyString(), anyString())).thenReturn(Optional.of(shadowDocument));
        when(mockDao.getShadowSyncInformation(anyString(), anyString())).thenReturn(Optional.of(SyncInformation.builder()
                .cloudUpdateTime(epochSecondsMinus60)
                .thingName(THING_NAME)
                .shadowName(SHADOW_NAME)
                .cloudDeleted(false)
                .cloudDocument(BASE_DOCUMENT)
                .cloudVersion(5L)
                .lastSyncTime(epochSecondsMinus60)
                .build()));

        CloudUpdateSyncRequest request = new CloudUpdateSyncRequest(THING_NAME, SHADOW_NAME, baseDocumentJson, mockDao, mockClientFactory);

        request.execute();

        verify(mockClientFactory, times(1)).getIotDataPlaneClient();
        verify(mockDao, times(1)).getShadowThing(anyString(), anyString());
        verify(mockDao, times(1)).updateSyncInformation(any());
        verify(mockIotDataPlaneClient, times(1)).updateThingShadow(any(UpdateThingShadowRequest.class));

        assertThat(syncInformationCaptor.getValue(), is(notNullValue()));
        assertThat(syncInformationCaptor.getValue().getCloudDocument(), is(JsonUtil.getPayloadBytes(shadowDocument.toJson(false))));
        assertThat(syncInformationCaptor.getValue().getCloudVersion(), is(6L));
        assertThat(syncInformationCaptor.getValue().getCloudUpdateTime(), is(greaterThanOrEqualTo(epochSeconds)));
        assertThat(syncInformationCaptor.getValue().getLastSyncTime(), is(greaterThanOrEqualTo(epochSeconds)));
        assertThat(syncInformationCaptor.getValue().getShadowName(), is(SHADOW_NAME));
        assertThat(syncInformationCaptor.getValue().getThingName(), is(THING_NAME));
        assertThat(syncInformationCaptor.getValue().isCloudDeleted(), is(false));
    }

    @Test
    void GIVEN_cloud_update_request_for_non_existent_shadow_WHEN_execute_THEN_does_not_update_cloud_shadow_and_sync_information() throws RetryableException, SkipSyncRequestException, IOException {
        when(mockDao.getShadowThing(anyString(), anyString())).thenReturn(Optional.empty());
        CloudUpdateSyncRequest request = new CloudUpdateSyncRequest(THING_NAME, SHADOW_NAME, baseDocumentJson, mockDao, mockClientFactory);

        request.execute();

        verify(mockClientFactory, times(0)).getIotDataPlaneClient();
        verify(mockDao, times(1)).getShadowThing(anyString(), anyString());
        verify(mockDao, times(0)).updateSyncInformation(any());
        verify(mockIotDataPlaneClient, times(0)).updateThingShadow(any(UpdateThingShadowRequest.class));
    }

    @ParameterizedTest
    @ValueSource(classes = {ConflictException.class, ThrottlingException.class, ServiceUnavailableException.class, InternalFailureException.class})
    void GIVEN_bad_cloud_update_request_WHEN_execute_and_updateShadow_throws_retryable_error_THEN_does_not_update_cloud_shadow_and_sync_information(Class clazz, ExtensionContext context) throws IOException {
        ignoreExceptionOfType(context, clazz);
        ShadowDocument shadowDocument = new ShadowDocument(BASE_DOCUMENT);
        when(mockDao.getShadowThing(anyString(), anyString())).thenReturn(Optional.of(shadowDocument));
        when(mockIotDataPlaneClient.updateThingShadow(any(UpdateThingShadowRequest.class))).thenThrow(clazz);
        CloudUpdateSyncRequest request = new CloudUpdateSyncRequest(THING_NAME, SHADOW_NAME, baseDocumentJson, mockDao, mockClientFactory);

        RetryableException thrown = assertThrows(RetryableException.class, request::execute);
        assertThat(thrown.getCause(), is(instanceOf(clazz)));

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
        CloudUpdateSyncRequest request = new CloudUpdateSyncRequest(THING_NAME, SHADOW_NAME, baseDocumentJson, mockDao, mockClientFactory);

        SkipSyncRequestException thrown = assertThrows(SkipSyncRequestException.class, request::execute);
        assertThat(thrown.getCause(), is(instanceOf(clazz)));

        verify(mockClientFactory, times(1)).getIotDataPlaneClient();
        verify(mockDao, times(1)).getShadowThing(anyString(), anyString());
        verify(mockDao, times(0)).updateSyncInformation(any());
        verify(mockIotDataPlaneClient, times(1)).updateThingShadow(any(UpdateThingShadowRequest.class));
    }
}
