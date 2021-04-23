/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.shadowmanager.sync.model;

import com.aws.greengrass.shadowmanager.ShadowManagerDAO;
import com.aws.greengrass.shadowmanager.exception.RetryableException;
import com.aws.greengrass.shadowmanager.exception.ShadowManagerDataException;
import com.aws.greengrass.shadowmanager.exception.SkipSyncRequestException;
import com.aws.greengrass.shadowmanager.ipc.UpdateThingShadowRequestHandler;
import com.aws.greengrass.shadowmanager.model.UpdateThingShadowHandlerResponse;
import com.aws.greengrass.shadowmanager.model.dao.SyncInformation;
import com.aws.greengrass.testcommons.testutilities.GGExtension;
import com.fasterxml.jackson.databind.exc.UnrecognizedPropertyException;
import org.hamcrest.Matchers;
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
import software.amazon.awssdk.aws.greengrass.model.UpdateThingShadowRequest;
import software.amazon.awssdk.aws.greengrass.model.UpdateThingShadowResponse;

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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;


@ExtendWith({MockitoExtension.class, GGExtension.class})
public class LocalUpdateSyncRequestTest {

    private static final byte[] UPDATE_DOCUMENT = "{\"version\": 6, \"state\": {\"reported\": {\"name\": \"The Beatles\"}}}".getBytes();

    @Mock
    UpdateThingShadowRequestHandler mockUpdateThingShadowRequestHandler;

    @Mock
    private ShadowManagerDAO mockDao;

    @Captor
    private ArgumentCaptor<SyncInformation> syncInformationCaptor;


    @Test
    void GIVEN_good_local_update_request_WHEN_execute_THEN_successfully_updates_local_shadow_and_sync_information() throws SkipSyncRequestException, RetryableException {
        lenient().when(mockDao.updateSyncInformation(syncInformationCaptor.capture())).thenReturn(true);

        long epochSeconds = Instant.now().getEpochSecond();
        when(mockDao.getShadowSyncInformation(anyString(), anyString())).thenReturn(Optional.of(SyncInformation.builder()
                .cloudUpdateTime(epochSeconds)
                .thingName(THING_NAME)
                .shadowName(SHADOW_NAME)
                .cloudDeleted(false)
                .lastSyncedDocument(null)
                .cloudVersion(5L)
                .lastSyncTime(epochSeconds)
                .build()));
        when(mockUpdateThingShadowRequestHandler.handleRequest(any(UpdateThingShadowRequest.class), anyString()))
                .thenReturn(new UpdateThingShadowHandlerResponse(new UpdateThingShadowResponse(), UPDATE_DOCUMENT));

        LocalUpdateSyncRequest request = new LocalUpdateSyncRequest(THING_NAME, SHADOW_NAME, UPDATE_DOCUMENT, mockDao, mockUpdateThingShadowRequestHandler);
        request.execute();

        verify(mockDao, times(1)).getShadowSyncInformation(anyString(), anyString());
        verify(mockUpdateThingShadowRequestHandler, times(1)).handleRequest(any(), any());
        verify(mockDao, times(1)).updateSyncInformation(any());

        assertThat(syncInformationCaptor.getValue(), is(notNullValue()));
        assertThat(syncInformationCaptor.getValue().getLastSyncedDocument(), is(equalTo(UPDATE_DOCUMENT)));
        assertThat(syncInformationCaptor.getValue().getCloudVersion(), is(6L));
        assertThat(syncInformationCaptor.getValue().getCloudUpdateTime(), is(greaterThanOrEqualTo(epochSeconds)));
        assertThat(syncInformationCaptor.getValue().getLastSyncTime(), is(greaterThanOrEqualTo(epochSeconds)));
        assertThat(syncInformationCaptor.getValue().getShadowName(), is(SHADOW_NAME));
        assertThat(syncInformationCaptor.getValue().getThingName(), is(THING_NAME));
        assertThat(syncInformationCaptor.getValue().isCloudDeleted(), is(false));
    }

    @Test
    void GIVEN_bad_cloud_update_payload_WHEN_execute_THEN_throw_skip_sync_request_exception(ExtensionContext context) {
        ignoreExceptionOfType(context, UnrecognizedPropertyException.class);

        final byte[] badCloudPayload = "{\"version\": 6, \"badUpdate\": {\"reported\": {\"name\": \"The Beatles\"}}}".getBytes();

        LocalUpdateSyncRequest request = new LocalUpdateSyncRequest(THING_NAME, SHADOW_NAME, badCloudPayload, mockDao, mockUpdateThingShadowRequestHandler);
        SkipSyncRequestException thrown = assertThrows(SkipSyncRequestException.class, request::execute);
        assertThat(thrown.getCause(), is(instanceOf(IOException.class)));

        verify(mockDao, times(0)).getShadowSyncInformation(anyString(), anyString());
        verify(mockUpdateThingShadowRequestHandler, times(0)).handleRequest(any(), any());
        verify(mockDao, times(0)).updateSyncInformation(any());
    }

    @Test
    void GIVEN_cloud_update_with_much_higher_version_WHEN_execute_THEN_throw_full_sync_request_exception(ExtensionContext context) {
        ignoreExceptionOfType(context, ConflictError.class);

        final byte[] cloudPayload =  "{\"version\": 10, \"state\": {\"reported\": {\"name\": \"The Beatles\"}}}".getBytes();

        long epochSeconds = Instant.now().getEpochSecond();
        when(mockDao.getShadowSyncInformation(anyString(), anyString())).thenReturn(Optional.of(SyncInformation.builder()
                .cloudUpdateTime(epochSeconds)
                .thingName(THING_NAME)
                .shadowName(SHADOW_NAME)
                .cloudDeleted(false)
                .lastSyncedDocument(null)
                .cloudVersion(5L)
                .lastSyncTime(epochSeconds)
                .build()));

        LocalUpdateSyncRequest request = new LocalUpdateSyncRequest(THING_NAME, SHADOW_NAME, cloudPayload, mockDao, mockUpdateThingShadowRequestHandler);
        ConflictError thrown = assertThrows(ConflictError.class, request::execute);
        assertThat(thrown.getMessage(), Matchers.startsWith("Missed updates"));

        verify(mockDao, times(1)).getShadowSyncInformation(anyString(), anyString());
        verify(mockUpdateThingShadowRequestHandler, times(0)).handleRequest(any(), any());
        verify(mockDao, times(0)).updateSyncInformation(any());
    }

    @ParameterizedTest
    @ValueSource(strings = {"{\"version\": 5, \"state\": {\"reported\": {\"name\": \"The Beatles\"}}}",
            "{\"version\": 2, \"state\": {\"reported\": {\"name\": \"The Beatles\"}}}"})
    void GIVEN_cloud_update_with_version_equal_or_less_WHEN_execute_THEN_do_nothing(String updateString) {
        final byte[] cloudPayload =  updateString.getBytes();

        long epochSeconds = Instant.now().getEpochSecond();
        when(mockDao.getShadowSyncInformation(anyString(), anyString())).thenReturn(Optional.of(SyncInformation.builder()
                .cloudUpdateTime(epochSeconds)
                .thingName(THING_NAME)
                .shadowName(SHADOW_NAME)
                .cloudDeleted(false)
                .lastSyncedDocument(null)
                .cloudVersion(5L)
                .lastSyncTime(epochSeconds)
                .build()));

        LocalUpdateSyncRequest request = new LocalUpdateSyncRequest(THING_NAME, SHADOW_NAME, cloudPayload, mockDao, mockUpdateThingShadowRequestHandler);
        assertDoesNotThrow(request::execute);

        verify(mockDao, times(1)).getShadowSyncInformation(anyString(), anyString());
        verify(mockUpdateThingShadowRequestHandler, times(0)).handleRequest(any(), any());
        verify(mockDao, times(0)).updateSyncInformation(any());
    }

    @Test
    void GIVEN_conflict_error_during_local_shadow_update_WHEN_execute_THEN_throw_full_sync_request_exception(ExtensionContext context) {
        ignoreExceptionOfType(context, ConflictError.class);

        long epochSeconds = Instant.now().getEpochSecond();
        when(mockDao.getShadowSyncInformation(anyString(), anyString())).thenReturn(Optional.of(SyncInformation.builder()
                .cloudUpdateTime(epochSeconds)
                .thingName(THING_NAME)
                .shadowName(SHADOW_NAME)
                .cloudDeleted(false)
                .lastSyncedDocument(null)
                .cloudVersion(5L)
                .lastSyncTime(epochSeconds)
                .build()));

        when(mockUpdateThingShadowRequestHandler.handleRequest(any(UpdateThingShadowRequest.class), anyString()))
                .thenThrow(new ConflictError(SAMPLE_EXCEPTION_MESSAGE));

        LocalUpdateSyncRequest request = new LocalUpdateSyncRequest(THING_NAME, SHADOW_NAME, UPDATE_DOCUMENT, mockDao, mockUpdateThingShadowRequestHandler);
        ConflictError thrown = assertThrows(ConflictError.class, request::execute);
        assertThat(thrown.getMessage(), is(equalTo(SAMPLE_EXCEPTION_MESSAGE)));

        verify(mockDao, times(1)).getShadowSyncInformation(anyString(), anyString());
        verify(mockUpdateThingShadowRequestHandler, times(1)).handleRequest(any(), any());
        verify(mockDao, times(0)).updateSyncInformation(any());
    }

    @Test
    void GIVEN_shadow_manager_data_exception_during_local_shadow_update_WHEN_execute_THEN_throw_skip_sync_request_exception(ExtensionContext context) {
        ignoreExceptionOfType(context, ShadowManagerDataException.class);

        long epochSeconds = Instant.now().getEpochSecond();
        when(mockDao.getShadowSyncInformation(anyString(), anyString())).thenReturn(Optional.of(SyncInformation.builder()
                .cloudUpdateTime(epochSeconds)
                .thingName(THING_NAME)
                .shadowName(SHADOW_NAME)
                .cloudDeleted(false)
                .lastSyncedDocument(null)
                .cloudVersion(5L)
                .lastSyncTime(epochSeconds)
                .build()));

        when(mockUpdateThingShadowRequestHandler.handleRequest(any(UpdateThingShadowRequest.class), anyString()))
                .thenThrow(ShadowManagerDataException.class);

        LocalUpdateSyncRequest request = new LocalUpdateSyncRequest(THING_NAME, SHADOW_NAME, UPDATE_DOCUMENT, mockDao, mockUpdateThingShadowRequestHandler);
        SkipSyncRequestException thrown = assertThrows(SkipSyncRequestException.class, request::execute);
        assertThat(thrown.getCause(), is(instanceOf(ShadowManagerDataException.class)));

        verify(mockDao, times(1)).getShadowSyncInformation(anyString(), anyString());
        verify(mockUpdateThingShadowRequestHandler, times(1)).handleRequest(any(), any());
        verify(mockDao, times(0)).updateSyncInformation(any());
    }

    @Test
    void GIVEN_unauthorized_error_during_local_shadow_update_WHEN_execute_THEN_throw_skip_sync_request_exception(ExtensionContext context) {
        ignoreExceptionOfType(context, UnauthorizedError.class);

        long epochSeconds = Instant.now().getEpochSecond();
        when(mockDao.getShadowSyncInformation(anyString(), anyString())).thenReturn(Optional.of(SyncInformation.builder()
                .cloudUpdateTime(epochSeconds)
                .thingName(THING_NAME)
                .shadowName(SHADOW_NAME)
                .cloudDeleted(false)
                .lastSyncedDocument(null)
                .cloudVersion(5L)
                .lastSyncTime(epochSeconds)
                .build()));

        when(mockUpdateThingShadowRequestHandler.handleRequest(any(UpdateThingShadowRequest.class), anyString()))
                .thenThrow(new UnauthorizedError(SAMPLE_EXCEPTION_MESSAGE));


        LocalUpdateSyncRequest request = new LocalUpdateSyncRequest(THING_NAME, SHADOW_NAME, UPDATE_DOCUMENT, mockDao, mockUpdateThingShadowRequestHandler);
        SkipSyncRequestException thrown = assertThrows(SkipSyncRequestException.class, request::execute);
        assertThat(thrown.getCause(), is(instanceOf(UnauthorizedError.class)));

        verify(mockDao, times(1)).getShadowSyncInformation(anyString(), anyString());
        verify(mockUpdateThingShadowRequestHandler, times(1)).handleRequest(any(), any());
        verify(mockDao, times(0)).updateSyncInformation(any());
    }

    @Test
    void GIVEN_invalid_arguments_error_during_local_shadow_update_WHEN_execute_THEN_throw_skip_sync_request_exception(ExtensionContext context) {
        ignoreExceptionOfType(context, InvalidArgumentsError.class);

        long epochSeconds = Instant.now().getEpochSecond();
        when(mockDao.getShadowSyncInformation(anyString(), anyString())).thenReturn(Optional.of(SyncInformation.builder()
                .cloudUpdateTime(epochSeconds)
                .thingName(THING_NAME)
                .shadowName(SHADOW_NAME)
                .cloudDeleted(false)
                .lastSyncedDocument(null)
                .cloudVersion(5L)
                .lastSyncTime(epochSeconds)
                .build()));

        when(mockUpdateThingShadowRequestHandler.handleRequest(any(UpdateThingShadowRequest.class), anyString()))
                .thenThrow(new InvalidArgumentsError(SAMPLE_EXCEPTION_MESSAGE));


        LocalUpdateSyncRequest request = new LocalUpdateSyncRequest(THING_NAME, SHADOW_NAME, UPDATE_DOCUMENT, mockDao, mockUpdateThingShadowRequestHandler);
        SkipSyncRequestException thrown = assertThrows(SkipSyncRequestException.class, request::execute);
        assertThat(thrown.getCause(), is(instanceOf(InvalidArgumentsError.class)));

        verify(mockDao, times(1)).getShadowSyncInformation(anyString(), anyString());
        verify(mockUpdateThingShadowRequestHandler, times(1)).handleRequest(any(), any());
        verify(mockDao, times(0)).updateSyncInformation(any());
    }

    @Test
    void GIVEN_service_error_during_local_shadow_update_WHEN_execute_THEN_throw_skip_sync_request_exception(ExtensionContext context) {
        ignoreExceptionOfType(context, ServiceError.class);

        long epochSeconds = Instant.now().getEpochSecond();
        when(mockDao.getShadowSyncInformation(anyString(), anyString())).thenReturn(Optional.of(SyncInformation.builder()
                .cloudUpdateTime(epochSeconds)
                .thingName(THING_NAME)
                .shadowName(SHADOW_NAME)
                .cloudDeleted(false)
                .lastSyncedDocument(null)
                .cloudVersion(5L)
                .lastSyncTime(epochSeconds)
                .build()));

        when(mockUpdateThingShadowRequestHandler.handleRequest(any(UpdateThingShadowRequest.class), anyString()))
                .thenThrow(new ServiceError(SAMPLE_EXCEPTION_MESSAGE));


        LocalUpdateSyncRequest request = new LocalUpdateSyncRequest(THING_NAME, SHADOW_NAME, UPDATE_DOCUMENT, mockDao, mockUpdateThingShadowRequestHandler);
        SkipSyncRequestException thrown = assertThrows(SkipSyncRequestException.class, request::execute);
        assertThat(thrown.getCause(), is(instanceOf(ServiceError.class)));

        verify(mockDao, times(1)).getShadowSyncInformation(anyString(), anyString());
        verify(mockUpdateThingShadowRequestHandler, times(1)).handleRequest(any(), any());
        verify(mockDao, times(0)).updateSyncInformation(any());
    }


}