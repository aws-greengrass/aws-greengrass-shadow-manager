/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.shadowmanager.sync.model;

import com.aws.greengrass.shadowmanager.ShadowManagerDAO;
import com.aws.greengrass.shadowmanager.exception.ShadowManagerDataException;
import com.aws.greengrass.shadowmanager.exception.SkipSyncRequestException;
import com.aws.greengrass.shadowmanager.exception.UnknownShadowException;
import com.aws.greengrass.shadowmanager.ipc.DeleteThingShadowRequestHandler;
import com.aws.greengrass.shadowmanager.ipc.UpdateThingShadowRequestHandler;
import com.aws.greengrass.shadowmanager.model.dao.SyncInformation;
import com.aws.greengrass.shadowmanager.sync.IotDataPlaneClientWrapper;
import com.aws.greengrass.testcommons.testutilities.GGExtension;
import com.fasterxml.jackson.core.JsonParseException;
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
import software.amazon.awssdk.aws.greengrass.model.DeleteThingShadowRequest;
import software.amazon.awssdk.aws.greengrass.model.DeleteThingShadowResponse;
import software.amazon.awssdk.aws.greengrass.model.InvalidArgumentsError;
import software.amazon.awssdk.aws.greengrass.model.ResourceNotFoundError;
import software.amazon.awssdk.aws.greengrass.model.ServiceError;
import software.amazon.awssdk.aws.greengrass.model.UnauthorizedError;

import java.time.Instant;
import java.util.Optional;

import static com.aws.greengrass.shadowmanager.TestUtils.SAMPLE_EXCEPTION_MESSAGE;
import static com.aws.greengrass.shadowmanager.TestUtils.SHADOW_NAME;
import static com.aws.greengrass.shadowmanager.TestUtils.THING_NAME;
import static com.aws.greengrass.testcommons.testutilities.ExceptionLogProtector.ignoreExceptionOfType;
import static org.hamcrest.MatcherAssert.assertThat;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.startsWith;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.verify;

@ExtendWith({MockitoExtension.class, GGExtension.class})
public class LocalDeleteSyncRequestTest {

    private static final byte[] CLOUD_DELETE_PAYLOAD = "{\"version\": 6}".getBytes();

    @Mock
    DeleteThingShadowRequestHandler mockDeleteThingShadowRequestHandler;

    @Mock
    private ShadowManagerDAO mockDao;

    @Captor
    private ArgumentCaptor<SyncInformation> syncInformationCaptor;

    private SyncContext syncContext;
    private long syncTime;

    @BeforeEach
    void setup() {
        lenient().when(mockDao.updateSyncInformation(syncInformationCaptor.capture())).thenReturn(true);
        syncTime = Instant.now().getEpochSecond();
        lenient().when(mockDao.getShadowSyncInformation(anyString(), anyString())).thenReturn(Optional.of(SyncInformation.builder()
                .thingName(THING_NAME)
                .shadowName(SHADOW_NAME)
                .cloudDeleted(false)
                .lastSyncedDocument(null)
                .cloudVersion(5L)
                .localVersion(5L)
                .lastSyncTime(syncTime)
                .cloudUpdateTime(syncTime)
                .build()));
        lenient().when(mockDeleteThingShadowRequestHandler.handleRequest(any(DeleteThingShadowRequest.class), anyString()))
                .thenReturn(new DeleteThingShadowResponse());

        syncContext = new SyncContext(mockDao, mock(UpdateThingShadowRequestHandler.class),
                mockDeleteThingShadowRequestHandler, mock(IotDataPlaneClientWrapper.class));
    }


    @ParameterizedTest
    @ValueSource(longs = {5, 6})
    void GIVEN_good_cloud_delete_payload_WHEN_execute_THEN_successfully_deletes_local_shadow_and_updates_sync_information(long deletedVersion) throws SkipSyncRequestException, UnknownShadowException {
        byte[] deletePayloadBytes = String.format("{\"version\": %d}", deletedVersion).getBytes();
        LocalDeleteSyncRequest request = new LocalDeleteSyncRequest(THING_NAME, SHADOW_NAME, deletePayloadBytes);
        request.execute(syncContext);

        verify(mockDao, times(1)).getShadowSyncInformation(anyString(), anyString());
        verify(mockDeleteThingShadowRequestHandler, times(1)).handleRequest(any(), any());
        verify(mockDao, times(1)).updateSyncInformation(any());

        assertThat(syncInformationCaptor.getValue(), is(notNullValue()));
        assertThat(syncInformationCaptor.getValue().getLastSyncedDocument(), is(nullValue()));
        assertThat(syncInformationCaptor.getValue().getCloudVersion(), is(deletedVersion));
        assertThat(syncInformationCaptor.getValue().getCloudUpdateTime(), is(greaterThanOrEqualTo(syncTime)));
        assertThat(syncInformationCaptor.getValue().getLastSyncTime(), is(greaterThanOrEqualTo(syncTime)));
        assertThat(syncInformationCaptor.getValue().getShadowName(), is(SHADOW_NAME));
        assertThat(syncInformationCaptor.getValue().getThingName(), is(THING_NAME));
        assertThat(syncInformationCaptor.getValue().isCloudDeleted(), is(true));
    }

    @Test
    void GIVEN_delete_version_less_than_synced_version_WHEN_execute_THEN_throw_conflict_error() {
        byte[] deletePayloadBytes = "{\"version\": 2}".getBytes();
        LocalDeleteSyncRequest request = new LocalDeleteSyncRequest(THING_NAME, SHADOW_NAME, deletePayloadBytes);

        ConflictError thrown = assertThrows(ConflictError.class, () -> request.execute(syncContext));
        assertThat(thrown.getMessage(), startsWith("Missed update(s)"));

        verify(mockDao, times(1)).getShadowSyncInformation(anyString(), anyString());
        verify(mockDeleteThingShadowRequestHandler, times(0)).handleRequest(any(), any());
        verify(mockDao, times(0)).updateSyncInformation(any());
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "{\"no_version_field\": 2}",
            "{\"version\": \"not_a_decimal\"}",
            "{\"version\": 1.234}"
    })
    void GIVEN_could_not_get_version_from_json_delete_payload_WHEN_execute_THEN_throw_skip_sync_request_exception(String deletePayloadString) {
        byte[] deletePayloadBytes = deletePayloadString.getBytes();
        LocalDeleteSyncRequest request = new LocalDeleteSyncRequest(THING_NAME, SHADOW_NAME, deletePayloadBytes);

        SkipSyncRequestException thrown = assertThrows(SkipSyncRequestException.class, () -> request.execute(syncContext));
        assertThat(thrown.getMessage(), startsWith("Invalid delete payload"));

        verify(mockDao, times(0)).getShadowSyncInformation(anyString(), anyString());
        verify(mockDeleteThingShadowRequestHandler, times(0)).handleRequest(any(), any());
        verify(mockDao, times(0)).updateSyncInformation(any());
    }

    @Test
    void GIVEN_could_not_parse_delete_payload_WHEN_execute_THEN_throw_skip_sync_request_exception(ExtensionContext context) {
        ignoreExceptionOfType(context, JsonParseException.class);
        byte[] deletePayloadBytes = "{NotAParsableJson}".getBytes();
        LocalDeleteSyncRequest request = new LocalDeleteSyncRequest(THING_NAME, SHADOW_NAME, deletePayloadBytes);

        assertThrows(SkipSyncRequestException.class, () -> request.execute(syncContext));

        verify(mockDao, times(0)).getShadowSyncInformation(anyString(), anyString());
        verify(mockDeleteThingShadowRequestHandler, times(0)).handleRequest(any(), any());
        verify(mockDao, times(0)).updateSyncInformation(any());
    }

    @Test
    void GIVEN_shadow_not_found_in_sync_table_WHEN_execute_THEN_throw_unknown_shadow_exception(ExtensionContext context) {
        lenient().when(mockDao.getShadowSyncInformation(anyString(), anyString())).thenReturn(Optional.empty());
        LocalDeleteSyncRequest request = new LocalDeleteSyncRequest(THING_NAME, SHADOW_NAME, CLOUD_DELETE_PAYLOAD);

        UnknownShadowException thrown = assertThrows(UnknownShadowException.class, () -> request.execute(syncContext));
        assertThat(thrown.getMessage(), startsWith("Shadow not found"));

        verify(mockDao, times(1)).getShadowSyncInformation(anyString(), anyString());
        verify(mockDeleteThingShadowRequestHandler, times(0)).handleRequest(any(), any());
        verify(mockDao, times(0)).updateSyncInformation(any());
    }

    @Test
    void GIVEN_local_shadow_already_deleted_WHEN_execute_THEN_do_nothing(ExtensionContext context) {
        ignoreExceptionOfType(context, ResourceNotFoundError.class);
        when(mockDeleteThingShadowRequestHandler.handleRequest(any(DeleteThingShadowRequest.class), anyString()))
                .thenThrow(new ResourceNotFoundError(SAMPLE_EXCEPTION_MESSAGE));

        LocalDeleteSyncRequest request = new LocalDeleteSyncRequest(THING_NAME, SHADOW_NAME, CLOUD_DELETE_PAYLOAD);
        assertDoesNotThrow(() -> request.execute(syncContext));

        verify(mockDao, times(1)).getShadowSyncInformation(anyString(), anyString());
        verify(mockDeleteThingShadowRequestHandler, times(1)).handleRequest(any(), any());
        verify(mockDao, times(0)).updateSyncInformation(any());
    }

    @Test
    void GIVEN_unauthorized_error_when_deleting_shadow_WHEN_execute_THEN_throw_skip_sync_request_exception() {
        when(mockDeleteThingShadowRequestHandler.handleRequest(any(DeleteThingShadowRequest.class), anyString()))
                .thenThrow(new UnauthorizedError(SAMPLE_EXCEPTION_MESSAGE));

        LocalDeleteSyncRequest request = new LocalDeleteSyncRequest(THING_NAME, SHADOW_NAME, CLOUD_DELETE_PAYLOAD);
        SkipSyncRequestException thrown = assertThrows(SkipSyncRequestException.class, () -> request.execute(syncContext));
        assertThat(thrown.getMessage(), containsString(SAMPLE_EXCEPTION_MESSAGE));


        verify(mockDao, times(1)).getShadowSyncInformation(anyString(), anyString());
        verify(mockDeleteThingShadowRequestHandler, times(1)).handleRequest(any(), any());
        verify(mockDao, times(0)).updateSyncInformation(any());
    }

    @Test
    void GIVEN_invalid_arguments_error_when_deleting_shadow_WHEN_execute_THEN_throw_skip_sync_request_exception() {
        when(mockDeleteThingShadowRequestHandler.handleRequest(any(DeleteThingShadowRequest.class), anyString()))
                .thenThrow(new InvalidArgumentsError(SAMPLE_EXCEPTION_MESSAGE));

        LocalDeleteSyncRequest request = new LocalDeleteSyncRequest(THING_NAME, SHADOW_NAME, CLOUD_DELETE_PAYLOAD);
        SkipSyncRequestException thrown = assertThrows(SkipSyncRequestException.class, () -> request.execute(syncContext));
        assertThat(thrown.getMessage(), containsString(SAMPLE_EXCEPTION_MESSAGE));


        verify(mockDao, times(1)).getShadowSyncInformation(anyString(), anyString());
        verify(mockDeleteThingShadowRequestHandler, times(1)).handleRequest(any(), any());
        verify(mockDao, times(0)).updateSyncInformation(any());
    }

    @Test
    void GIVEN_service_error_when_deleting_shadow_WHEN_execute_THEN_throw_skip_sync_request_exception() {
        when(mockDeleteThingShadowRequestHandler.handleRequest(any(DeleteThingShadowRequest.class), anyString()))
                .thenThrow(new ServiceError(SAMPLE_EXCEPTION_MESSAGE));

        LocalDeleteSyncRequest request = new LocalDeleteSyncRequest(THING_NAME, SHADOW_NAME, CLOUD_DELETE_PAYLOAD);
        SkipSyncRequestException thrown = assertThrows(SkipSyncRequestException.class, () -> request.execute(syncContext));
        assertThat(thrown.getMessage(), containsString(SAMPLE_EXCEPTION_MESSAGE));


        verify(mockDao, times(1)).getShadowSyncInformation(anyString(), anyString());
        verify(mockDeleteThingShadowRequestHandler, times(1)).handleRequest(any(), any());
        verify(mockDao, times(0)).updateSyncInformation(any());
    }

    @Test
    void GIVEN_shadow_manager_data_exception_when_deleting_shadow_WHEN_execute_THEN_throw_skip_sync_request_exception() {
        when(mockDeleteThingShadowRequestHandler.handleRequest(any(DeleteThingShadowRequest.class), anyString()))
                .thenThrow(new ShadowManagerDataException(new Exception(SAMPLE_EXCEPTION_MESSAGE)));

        LocalDeleteSyncRequest request = new LocalDeleteSyncRequest(THING_NAME, SHADOW_NAME, CLOUD_DELETE_PAYLOAD);
        SkipSyncRequestException thrown = assertThrows(SkipSyncRequestException.class, () -> request.execute(syncContext));
        assertThat(thrown.getMessage(), containsString(SAMPLE_EXCEPTION_MESSAGE));

        verify(mockDao, times(1)).getShadowSyncInformation(anyString(), anyString());
        verify(mockDeleteThingShadowRequestHandler, times(1)).handleRequest(any(), any());
        verify(mockDao, times(0)).updateSyncInformation(any());
    }
}
