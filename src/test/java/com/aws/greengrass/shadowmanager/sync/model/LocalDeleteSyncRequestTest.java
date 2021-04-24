/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.shadowmanager.sync.model;

import com.aws.greengrass.shadowmanager.ShadowManagerDAO;
import com.aws.greengrass.shadowmanager.exception.SkipSyncRequestException;
import com.aws.greengrass.shadowmanager.ipc.DeleteThingShadowRequestHandler;
import com.aws.greengrass.shadowmanager.model.dao.SyncInformation;
import com.aws.greengrass.testcommons.testutilities.GGExtension;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import software.amazon.awssdk.aws.greengrass.model.ConflictError;
import software.amazon.awssdk.aws.greengrass.model.DeleteThingShadowRequest;
import software.amazon.awssdk.aws.greengrass.model.DeleteThingShadowResponse;

import java.time.Instant;
import java.util.Optional;

import static com.aws.greengrass.shadowmanager.TestUtils.SHADOW_NAME;
import static com.aws.greengrass.shadowmanager.TestUtils.THING_NAME;
import static org.hamcrest.MatcherAssert.assertThat;

import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.startsWith;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.lenient;
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

    private long syncTime;

    @BeforeEach
    void setup() {
        lenient().when(mockDao.updateSyncInformation(syncInformationCaptor.capture())).thenReturn(true);
        syncTime = Instant.now().getEpochSecond();
        when(mockDao.getShadowSyncInformation(anyString(), anyString())).thenReturn(Optional.of(SyncInformation.builder()
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
    }


    @ParameterizedTest
    @ValueSource(longs = {5, 6})
    void GIVEN_good_cloud_delete_payload_WHEN_execute_THEN_successfully_deletes_local_shadow_and_updates_sync_information(long deletedVersion) throws SkipSyncRequestException {
        byte[] deletePayloadBytes = String.format("{\"version\": %d}", deletedVersion).getBytes();
        LocalDeleteSyncRequest request = new LocalDeleteSyncRequest(THING_NAME, SHADOW_NAME, deletePayloadBytes, mockDao, mockDeleteThingShadowRequestHandler);
        request.execute();

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
    void GIVEN_delete_version_less_than_synced_version_WHEN_execute_THEN_throw_conflict_error() throws SkipSyncRequestException {
        byte[] deletePayloadBytes = "{\"version\": 2}".getBytes();
        LocalDeleteSyncRequest request = new LocalDeleteSyncRequest(THING_NAME, SHADOW_NAME, deletePayloadBytes, mockDao, mockDeleteThingShadowRequestHandler);

        ConflictError thrown = assertThrows(ConflictError.class, request::execute);
        assertThat(thrown.getMessage(), startsWith("Missed update(s)"));

        verify(mockDao, times(1)).getShadowSyncInformation(anyString(), anyString());
        verify(mockDeleteThingShadowRequestHandler, times(0)).handleRequest(any(), any());
        verify(mockDao, times(0)).updateSyncInformation(any());
    }


}
