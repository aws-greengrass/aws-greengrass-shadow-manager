/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.shadowmanager.sync.model;

import com.aws.greengrass.shadowmanager.ShadowManagerDAO;
import com.aws.greengrass.shadowmanager.exception.RetryableException;
import com.aws.greengrass.shadowmanager.exception.SkipSyncRequestException;
import com.aws.greengrass.shadowmanager.model.dao.SyncInformation;
import com.aws.greengrass.shadowmanager.sync.IotDataPlaneClient;
import com.aws.greengrass.testcommons.testutilities.GGExtension;
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
import software.amazon.awssdk.services.iotdataplane.model.InternalFailureException;
import software.amazon.awssdk.services.iotdataplane.model.InvalidRequestException;
import software.amazon.awssdk.services.iotdataplane.model.MethodNotAllowedException;
import software.amazon.awssdk.services.iotdataplane.model.RequestEntityTooLargeException;
import software.amazon.awssdk.services.iotdataplane.model.ServiceUnavailableException;
import software.amazon.awssdk.services.iotdataplane.model.ThrottlingException;
import software.amazon.awssdk.services.iotdataplane.model.UnauthorizedException;
import software.amazon.awssdk.services.iotdataplane.model.UnsupportedDocumentEncodingException;

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
import static org.hamcrest.Matchers.nullValue;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith({MockitoExtension.class, GGExtension.class})
class CloudDeleteSyncRequestTest {
    private static final byte[] BASE_DOCUMENT = "{\"version\": 1, \"state\": {\"reported\": {\"name\": \"The Beatles\"}}}".getBytes();

    @Mock
    private ShadowManagerDAO mockDao;
    @Mock
    private IotDataPlaneClient mockIotDataPlaneClient;
    @Captor
    private ArgumentCaptor<SyncInformation> syncInformationCaptor;
    @Mock
    private SyncContext mockContext;

    @BeforeEach
    void setup() {
        lenient().when(mockDao.updateSyncInformation(syncInformationCaptor.capture())).thenReturn(true);
        lenient().when(mockContext.getDao()).thenReturn(mockDao);
        lenient().when(mockContext.getIotDataPlaneClient()).thenReturn(mockIotDataPlaneClient);
    }

    @Test
    void GIVEN_good_cloud_delete_request_WHEN_execute_THEN_successfully_updates_cloud_shadow_and_sync_information() throws RetryableException, SkipSyncRequestException, IOException {
        long epochSeconds = Instant.now().getEpochSecond();
        long epochSecondsMinus60 = Instant.now().minusSeconds(60).getEpochSecond();
        when(mockDao.getShadowSyncInformation(anyString(), anyString())).thenReturn(Optional.of(SyncInformation.builder()
                .cloudUpdateTime(epochSecondsMinus60)
                .thingName(THING_NAME)
                .shadowName(SHADOW_NAME)
                .cloudDeleted(false)
                .lastSyncedDocument(BASE_DOCUMENT)
                .cloudVersion(1L)
                .lastSyncTime(epochSecondsMinus60)
                .build()));
        CloudDeleteSyncRequest request = new CloudDeleteSyncRequest(THING_NAME, SHADOW_NAME);

        request.execute(mockContext);

        verify(mockDao, times(1)).getShadowSyncInformation(anyString(), anyString());
        verify(mockDao, times(1)).updateSyncInformation(any());
        verify(mockIotDataPlaneClient, times(1)).deleteThingShadow(anyString(), anyString());

        assertThat(syncInformationCaptor.getValue(), is(notNullValue()));
        assertThat(syncInformationCaptor.getValue().getLastSyncedDocument(), is(nullValue()));
        assertThat(syncInformationCaptor.getValue().getCloudVersion(), is(1L));
        assertThat(syncInformationCaptor.getValue().getCloudUpdateTime(), is(greaterThanOrEqualTo(epochSeconds)));
        assertThat(syncInformationCaptor.getValue().getLastSyncTime(), is(greaterThanOrEqualTo(epochSeconds)));
        assertThat(syncInformationCaptor.getValue().getShadowName(), is(SHADOW_NAME));
        assertThat(syncInformationCaptor.getValue().getThingName(), is(THING_NAME));
        assertThat(syncInformationCaptor.getValue().isCloudDeleted(), is(true));
    }

    @Test
    void GIVEN_cloud_delete_request_for_non_existent_shadow_WHEN_execute_THEN_does_not_update_cloud_shadow_and_sync_information() throws RetryableException, SkipSyncRequestException, IOException {
        long epochSeconds = Instant.now().getEpochSecond();
        when(mockDao.getShadowSyncInformation(anyString(), anyString())).thenReturn(Optional.empty());
        CloudDeleteSyncRequest request = new CloudDeleteSyncRequest(THING_NAME, SHADOW_NAME);

        request.execute(mockContext);

        verify(mockDao, times(1)).getShadowSyncInformation(anyString(), anyString());
        verify(mockDao, times(1)).updateSyncInformation(any());
        verify(mockIotDataPlaneClient, times(1)).deleteThingShadow(anyString(), anyString());

        assertThat(syncInformationCaptor.getValue(), is(notNullValue()));
        assertThat(syncInformationCaptor.getValue().getLastSyncedDocument(), is(nullValue()));
        assertThat(syncInformationCaptor.getValue().getCloudVersion(), is(0L));
        assertThat(syncInformationCaptor.getValue().getCloudUpdateTime(), is(greaterThanOrEqualTo(epochSeconds)));
        assertThat(syncInformationCaptor.getValue().getLastSyncTime(), is(greaterThanOrEqualTo(epochSeconds)));
        assertThat(syncInformationCaptor.getValue().getShadowName(), is(SHADOW_NAME));
        assertThat(syncInformationCaptor.getValue().getThingName(), is(THING_NAME));
        assertThat(syncInformationCaptor.getValue().isCloudDeleted(), is(true));
    }

    @ParameterizedTest
    @ValueSource(classes = {ThrottlingException.class, ServiceUnavailableException.class, InternalFailureException.class})
    void GIVEN_bad_cloud_delete_request_WHEN_execute_and_updateShadow_throws_retryable_error_THEN_does_not_update_cloud_shadow_and_sync_information(Class clazz, ExtensionContext context) throws IOException {
        ignoreExceptionOfType(context, clazz);
        doThrow(clazz).when(mockIotDataPlaneClient).deleteThingShadow(anyString(), anyString());
        CloudDeleteSyncRequest request = new CloudDeleteSyncRequest(THING_NAME, SHADOW_NAME);

        RetryableException thrown = assertThrows(RetryableException.class, () -> request.execute(mockContext));
        assertThat(thrown.getCause(), is(instanceOf(clazz)));

        verify(mockDao, times(0)).getShadowSyncInformation(anyString(), anyString());
        verify(mockDao, times(0)).updateSyncInformation(any());
        verify(mockIotDataPlaneClient, times(1)).deleteThingShadow(anyString(), anyString());
    }

    @ParameterizedTest
    @ValueSource(classes = {RequestEntityTooLargeException.class, InvalidRequestException.class, UnauthorizedException.class,
            MethodNotAllowedException.class, UnsupportedDocumentEncodingException.class, AwsServiceException.class, SdkClientException.class})
    void GIVEN_bad_cloud_delete_request_WHEN_execute_and_updateShadow_throws_skipable_error_THEN_does_not_update_cloud_shadow_and_sync_information(Class clazz, ExtensionContext context) throws IOException {
        ignoreExceptionOfType(context, clazz);
        doThrow(clazz).when(mockIotDataPlaneClient).deleteThingShadow(anyString(), anyString());
        CloudDeleteSyncRequest request = new CloudDeleteSyncRequest(THING_NAME, SHADOW_NAME);

        SkipSyncRequestException thrown = assertThrows(SkipSyncRequestException.class,
                () -> request.execute(mockContext));
        assertThat(thrown.getCause(), is(instanceOf(clazz)));

        verify(mockDao, times(0)).getShadowSyncInformation(anyString(), anyString());
        verify(mockDao, times(0)).updateSyncInformation(any());
        verify(mockIotDataPlaneClient, times(1)).deleteThingShadow(anyString(), anyString());
    }
}
