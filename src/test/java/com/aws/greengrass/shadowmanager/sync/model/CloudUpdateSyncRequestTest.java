/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.shadowmanager.sync.model;

import com.aws.greengrass.shadowmanager.ShadowManagerDAO;
import com.aws.greengrass.shadowmanager.exception.IoTDataPlaneClientCreationException;
import com.aws.greengrass.shadowmanager.exception.RetryableException;
import com.aws.greengrass.shadowmanager.exception.SkipSyncRequestException;
import com.aws.greengrass.shadowmanager.exception.UnknownShadowException;
import com.aws.greengrass.shadowmanager.model.ShadowDocument;
import com.aws.greengrass.shadowmanager.model.dao.SyncInformation;
import com.aws.greengrass.shadowmanager.sync.IotDataPlaneClientWrapper;
import com.aws.greengrass.shadowmanager.util.JsonMerger;
import com.aws.greengrass.shadowmanager.util.JsonUtil;
import com.aws.greengrass.testcommons.testutilities.GGExtension;
import com.fasterxml.jackson.databind.JsonNode;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.services.iotdataplane.model.ConflictException;
import software.amazon.awssdk.services.iotdataplane.model.InternalFailureException;
import software.amazon.awssdk.services.iotdataplane.model.InvalidRequestException;
import software.amazon.awssdk.services.iotdataplane.model.MethodNotAllowedException;
import software.amazon.awssdk.services.iotdataplane.model.ResourceNotFoundException;
import software.amazon.awssdk.services.iotdataplane.model.ServiceUnavailableException;
import software.amazon.awssdk.services.iotdataplane.model.ThrottlingException;
import software.amazon.awssdk.services.iotdataplane.model.UnauthorizedException;
import software.amazon.awssdk.services.iotdataplane.model.UnsupportedDocumentEncodingException;
import software.amazon.awssdk.services.iotdataplane.model.UpdateThingShadowResponse;

import java.io.IOException;
import java.util.stream.Stream;
import java.time.Instant;
import java.util.Optional;

import static com.aws.greengrass.shadowmanager.TestUtils.SAMPLE_EXCEPTION_MESSAGE;
import static com.aws.greengrass.shadowmanager.TestUtils.SHADOW_NAME;
import static com.aws.greengrass.shadowmanager.TestUtils.THING_NAME;
import static com.aws.greengrass.testcommons.testutilities.ExceptionLogProtector.ignoreExceptionOfType;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.atMostOnce;
import static org.mockito.Mockito.doThrow;
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
    private IotDataPlaneClientWrapper mockIotDataPlaneClientWrapper;
    @Captor
    private ArgumentCaptor<SyncInformation> syncInformationCaptor;
    @Mock
    private SyncContext mockContext;

    @BeforeAll
    static void setupSchema() throws IOException {
        JsonUtil.loadSchema();
    }

    @BeforeEach
    void setup() throws IOException {
        lenient().when(mockDao.updateSyncInformation(syncInformationCaptor.capture())).thenReturn(true);
        baseDocumentJson = JsonUtil.getPayloadJson(BASE_DOCUMENT).get();
        lenient().when(mockContext.getDao()).thenReturn(mockDao);
        lenient().when(mockContext.getIotDataPlaneClientWrapper()).thenReturn(mockIotDataPlaneClientWrapper);
        lenient().when(mockDao.getShadowSyncInformation(anyString(), anyString())).thenReturn(Optional.of(SyncInformation.builder().build()));
    }

    @Test
    void GIVEN_good_cloud_update_request_WHEN_execute_THEN_successfully_updates_cloud_shadow_and_sync_information() throws Exception {
        long epochSeconds = Instant.now().getEpochSecond();
        long epochSecondsMinus60 = Instant.now().minusSeconds(60).getEpochSecond();
        ShadowDocument shadowDocument = new ShadowDocument(BASE_DOCUMENT);
        when(mockDao.getShadowSyncInformation(anyString(), anyString())).thenReturn(Optional.of(SyncInformation.builder()
                .cloudUpdateTime(epochSecondsMinus60)
                .thingName(THING_NAME)
                .shadowName(SHADOW_NAME)
                .cloudDeleted(false)
                .lastSyncedDocument(null)
                .cloudVersion(5L)
                .lastSyncTime(epochSecondsMinus60)
                .build()));
        when(mockIotDataPlaneClientWrapper.updateThingShadow(anyString(), anyString(), any(byte[].class)))
                .thenReturn(UpdateThingShadowResponse.builder().payload(SdkBytes.fromString("{\"version\": 6}", UTF_8)).build());

        CloudUpdateSyncRequest request = new CloudUpdateSyncRequest(THING_NAME, SHADOW_NAME, baseDocumentJson,
                shadowDocument);
        request.execute(mockContext);

        verify(mockDao, times(1)).updateSyncInformation(any());
        verify(mockIotDataPlaneClientWrapper, times(1)).updateThingShadow(anyString(), anyString(), any(byte[].class));

        assertThat(syncInformationCaptor.getValue(), is(notNullValue()));
        assertThat(syncInformationCaptor.getValue().getLastSyncedDocument(), is(JsonUtil.getPayloadBytes(shadowDocument.toJson(false))));
        assertThat(syncInformationCaptor.getValue().getCloudVersion(), is(6L));
        assertThat(syncInformationCaptor.getValue().getCloudUpdateTime(), is(greaterThanOrEqualTo(epochSeconds)));
        assertThat(syncInformationCaptor.getValue().getLastSyncTime(), is(greaterThanOrEqualTo(epochSeconds)));
        assertThat(syncInformationCaptor.getValue().getShadowName(), is(SHADOW_NAME));
        assertThat(syncInformationCaptor.getValue().getThingName(), is(THING_NAME));
        assertThat(syncInformationCaptor.getValue().isCloudDeleted(), is(false));
    }

    @ParameterizedTest
    @ValueSource(classes = {ThrottlingException.class, ServiceUnavailableException.class, InternalFailureException.class})
    void GIVEN_bad_cloud_update_request_WHEN_execute_and_updateShadow_throws_retryable_error_THEN_does_not_update_cloud_shadow_and_sync_information(Class clazz, ExtensionContext context) throws IOException, IoTDataPlaneClientCreationException {
        ignoreExceptionOfType(context, clazz);
        ShadowDocument shadowDocument = new ShadowDocument(BASE_DOCUMENT);
        doThrow(clazz).when(mockIotDataPlaneClientWrapper).updateThingShadow(anyString(), anyString(), any(byte[].class));
        CloudUpdateSyncRequest request = new CloudUpdateSyncRequest(THING_NAME, SHADOW_NAME, baseDocumentJson, shadowDocument);

        RetryableException thrown = assertThrows(RetryableException.class, () -> request.execute(mockContext));
        assertThat(thrown.getCause(), is(instanceOf(clazz)));

        verify(mockDao, times(0)).updateSyncInformation(any());
        verify(mockIotDataPlaneClientWrapper, times(1)).updateThingShadow(anyString(), anyString(), any(byte[].class));
    }

    @Test
    void GIVEN_bad_cloud_update_request_WHEN_execute_and_updateShadow_throws_conflict_exception_THEN_does_not_update_cloud_shadow_and_sync_information(ExtensionContext context) throws IOException, IoTDataPlaneClientCreationException {
        ignoreExceptionOfType(context, ConflictException.class);
        ShadowDocument shadowDocument = new ShadowDocument(BASE_DOCUMENT);
        doThrow(ConflictException.builder().message(SAMPLE_EXCEPTION_MESSAGE).build())
                .when(mockIotDataPlaneClientWrapper).updateThingShadow(anyString(), anyString(), any(byte[].class));

        CloudUpdateSyncRequest request = new CloudUpdateSyncRequest(THING_NAME, SHADOW_NAME, baseDocumentJson, shadowDocument);

        ConflictException thrown = assertThrows(ConflictException.class, () -> request.execute(mockContext));
        assertThat(thrown.getMessage(), is(equalTo(SAMPLE_EXCEPTION_MESSAGE)));

        verify(mockDao, times(0)).updateSyncInformation(any());
        verify(mockIotDataPlaneClientWrapper, times(1)).updateThingShadow(anyString(), anyString(), any(byte[].class));
    }

    @ParameterizedTest
    @ValueSource(classes = {ResourceNotFoundException.class, InvalidRequestException.class, UnauthorizedException.class,
            MethodNotAllowedException.class, UnsupportedDocumentEncodingException.class, AwsServiceException.class, SdkClientException.class})
    void GIVEN_bad_cloud_update_request_WHEN_execute_and_updateShadow_throws_skipable_error_THEN_does_not_update_cloud_shadow_and_sync_information(Class clazz, ExtensionContext context) throws IOException, IoTDataPlaneClientCreationException {
        ignoreExceptionOfType(context, clazz);
        ShadowDocument shadowDocument = new ShadowDocument(BASE_DOCUMENT);
        doThrow(clazz).when(mockIotDataPlaneClientWrapper).updateThingShadow(anyString(), anyString(), any(byte[].class));
        CloudUpdateSyncRequest request = new CloudUpdateSyncRequest(THING_NAME, SHADOW_NAME, baseDocumentJson, shadowDocument);

        SkipSyncRequestException thrown = assertThrows(SkipSyncRequestException.class,
                () -> request.execute(mockContext));
        assertThat(thrown.getCause(), is(instanceOf(clazz)));

        verify(mockDao, times(0)).updateSyncInformation(any());
        verify(mockIotDataPlaneClientWrapper, times(1)).updateThingShadow(anyString(), anyString(), any(byte[].class));
    }

    @Test
    void GIVEN_new_values_WHEN_merge_THEN_document_merged() throws IOException {

        CloudUpdateSyncRequest request = new CloudUpdateSyncRequest(THING_NAME, SHADOW_NAME, baseDocumentJson, null);
        JsonNode updateDocument = JsonUtil.getPayloadJson(UPDATE_DOCUMENT).get();
        CloudUpdateSyncRequest other = new CloudUpdateSyncRequest(THING_NAME, SHADOW_NAME, updateDocument, null);
        request.merge(other);

        assertThat(request.updateDocument, is(updateDocument));
    }

    @Test
    void GIVEN_no_change_to_shadow_content_but_version_change_WHEN_isUpdateNecessary_THEN_returns_false_and_updates_sync_info() throws IOException, SkipSyncRequestException, UnknownShadowException {
        when(mockDao.updateSyncInformation(syncInformationCaptor.capture())).thenReturn(true);
        ShadowDocument shadowDocument = new ShadowDocument(BASE_DOCUMENT);

        long epochSeconds = Instant.now().getEpochSecond();
        when(mockDao.getShadowSyncInformation(anyString(), anyString())).thenReturn(Optional.of(SyncInformation.builder()
                .cloudUpdateTime(epochSeconds)
                .thingName(THING_NAME)
                .shadowName(SHADOW_NAME)
                .cloudDeleted(false)
                .lastSyncedDocument(BASE_DOCUMENT)
                .cloudVersion(0L)
                .localVersion(0L)
                .lastSyncTime(epochSeconds)
                .build()));
        CloudUpdateSyncRequest request = new CloudUpdateSyncRequest(THING_NAME, SHADOW_NAME, baseDocumentJson,
                shadowDocument);

        assertFalse(request.isUpdateNecessary(mockContext));
        verify(mockDao, atMostOnce()).updateSyncInformation(any());

        assertThat(syncInformationCaptor.getValue(), is(notNullValue()));
        assertThat(syncInformationCaptor.getValue().getLastSyncedDocument(), is(equalTo(BASE_DOCUMENT)));
        assertThat(syncInformationCaptor.getValue().getCloudVersion(), is(0L));
        assertThat(syncInformationCaptor.getValue().getLocalVersion(), is(1L));
        assertThat(syncInformationCaptor.getValue().getCloudUpdateTime(), is(greaterThanOrEqualTo(epochSeconds)));
        assertThat(syncInformationCaptor.getValue().getLastSyncTime(), is(greaterThanOrEqualTo(epochSeconds)));
        assertThat(syncInformationCaptor.getValue().getShadowName(), is(SHADOW_NAME));
        assertThat(syncInformationCaptor.getValue().getThingName(), is(THING_NAME));
        assertThat(syncInformationCaptor.getValue().isCloudDeleted(), is(false));
    }

    @Test
    void GIVEN_no_change_to_shadow_content_and_no_version_change_WHEN_isUpdateNecessary_THEN_returns_false_and_does_not_update_sync_info() throws IOException, SkipSyncRequestException, UnknownShadowException {
        ShadowDocument shadowDocument = new ShadowDocument(BASE_DOCUMENT);

        long epochSeconds = Instant.now().getEpochSecond();
        when(mockDao.getShadowSyncInformation(anyString(), anyString())).thenReturn(Optional.of(SyncInformation.builder()
                .cloudUpdateTime(epochSeconds)
                .thingName(THING_NAME)
                .shadowName(SHADOW_NAME)
                .cloudDeleted(false)
                .lastSyncedDocument(BASE_DOCUMENT)
                .cloudVersion(0L)
                .localVersion(1L)
                .lastSyncTime(epochSeconds)
                .build()));
        CloudUpdateSyncRequest request = new CloudUpdateSyncRequest(THING_NAME, SHADOW_NAME, baseDocumentJson, shadowDocument);

        assertFalse(request.isUpdateNecessary(mockContext));
        verify(mockDao, never()).updateSyncInformation(any());
    }

    @Test
    void GIVEN_new_shadow_WHEN_isUpdateNecessary_THEN_returns_true() throws IOException, UnknownShadowException, SkipSyncRequestException {
        ShadowDocument shadowDocument = new ShadowDocument(BASE_DOCUMENT);

        when(mockDao.getShadowSyncInformation(anyString(), anyString())).thenReturn(Optional.of(
                SyncInformation.builder()
                        .lastSyncedDocument(null)
                        .build()));
        CloudUpdateSyncRequest request = new CloudUpdateSyncRequest(THING_NAME, SHADOW_NAME, baseDocumentJson, shadowDocument);

        assertTrue(request.isUpdateNecessary(mockContext));
    }

    @Test
    void GIVEN_different_cloud_update_WHEN_isUpdateNecessary_THEN_returns_true() throws IOException {
        JsonNode j1 = JsonUtil.getPayloadJson("{\"state\":{\"reported\":{\"color\":{\"r\":255,\"g\":0,\"b\":0},\"SomeKey\":\"SomeValue\"}},\"metadata\":{\"reported\":{\"color\":{\"r\":{\"timestamp\":1619722006},\"g\":{\"timestamp\":1619722006},\"b\":{\"timestamp\":1619722006}},\"SomeKey\":{\"timestamp\":1619722006}}},\"version\":1,\"timestamp\":1619722006}".getBytes()).get();
        JsonNode j2 = JsonUtil.getPayloadJson("{\"state\":{\"reported\":{\"color\":{\"r\":255,\"g\":255,\"b\":255},\"SomeKey\":\"SomeValue\"}},\"metadata\":{\"reported\":{\"color\":{\"r\":{\"timestamp\":1619722006},\"g\":{\"timestamp\":1619722006},\"b\":{\"timestamp\":1619722006}},\"SomeKey\":{\"timestamp\":1619722006}}},\"version\":1,\"timestamp\":1619722006}".getBytes()).get();
        CloudUpdateSyncRequest request = new CloudUpdateSyncRequest(THING_NAME, SHADOW_NAME, baseDocumentJson,
                new ShadowDocument(BASE_DOCUMENT));
        assertTrue(request.isUpdateNecessary(j1, j2));
    }

    @Test
    void GIVEN_same_cloud_update_WHEN_isUpdateNecessary_THEN_returns_false() throws IOException {
        JsonNode j1 = JsonUtil.getPayloadJson("{\"state\":{\"reported\":{\"color\":{\"r\":255,\"g\":255,\"b\":255},\"SomeKey\":\"SomeValue\"}},\"metadata\":{\"reported\":{\"color\":{\"r\":{\"timestamp\":1619722006},\"g\":{\"timestamp\":1619722006},\"b\":{\"timestamp\":1619722006}},\"SomeKey\":{\"timestamp\":1619722006}}},\"version\":1,\"timestamp\":1619722006}".getBytes()).get();
        JsonNode j2 = JsonUtil.getPayloadJson("{\"state\":{\"reported\":{\"color\":{\"r\":255,\"g\":255,\"b\":255},\"SomeKey\":\"SomeValue\"}},\"metadata\":{\"reported\":{\"color\":{\"r\":{\"timestamp\":1619722006},\"g\":{\"timestamp\":1619722006},\"b\":{\"timestamp\":1619722006}},\"SomeKey\":{\"timestamp\":1619722006}}},\"version\":1,\"timestamp\":1619722006}".getBytes()).get();
        CloudUpdateSyncRequest request = new CloudUpdateSyncRequest(THING_NAME, SHADOW_NAME, baseDocumentJson, new ShadowDocument(BASE_DOCUMENT));
        assertFalse(request.isUpdateNecessary(j1, j2));
    }

    static Stream<Arguments> updateNecessaryInput() {
        return Stream.of(
                // base state, update state, whether the update changes the shadow
                Arguments.of("{\"desired\":{\"z\":1}}", "{\"desired\":{\"a\":{}}}", true),
                Arguments.of("{\"desired\":{\"z\":{\"k\":1}}}", "{\"desired\":{\"z\":{\"e\":{}}}}", true),
                Arguments.of("{\"desired\":{\"z\":1}}", "{\"desired\":{\"state\":{}}}", true),
                Arguments.of("{\"desired\":{\"a\":1}}", "{\"desired\":{\"a\":{}}}", true),
                Arguments.of("{\"desired\":{\"a\":{\"x\":1}}}", "{\"desired\":{\"a\":{}}}", false),
                Arguments.of("{\"desired\":{\"z\":1}}", "{\"desired\":{}}", false),
                Arguments.of("{\"desired\":{\"z\":1}}", "{\"desired\":{\"a\":null}}", false),
                Arguments.of("{\"desired\":{\"z\":1}}", "{\"desired\":{\"z\":null}}", true),
                Arguments.of("{\"desired\":{\"z\":1}}", "{\"desired\":{\"z\":1}}", false),
                Arguments.of("{\"reported\":{\"r\":1}}", "{\"desired\":{\"a\":null}}", false),
                Arguments.of("{\"desired\":{\"z\":1}}", "{\"desired\":null}", true),
                Arguments.of("{\"desired\":{\"z\":1}}", "null", true)
        );
    }

    @ParameterizedTest
    @MethodSource("updateNecessaryInput")
    void GIVEN_base_and_update_WHEN_isUpdateNecessary_THEN_matches_shadow_merge(String base, String update,
            boolean expected) throws IOException {
        JsonNode baseDocument = JsonUtil.getPayloadJson(("{\"state\":" + base + ",\"version\":1}").getBytes()).get();
        JsonNode updateDocument = JsonUtil.getPayloadJson(("{\"state\":" + update + "}").getBytes()).get();
        CloudUpdateSyncRequest request = new CloudUpdateSyncRequest(THING_NAME, SHADOW_NAME, baseDocumentJson,
                new ShadowDocument(BASE_DOCUMENT));

        assertThat(request.isUpdateNecessary(baseDocument, updateDocument), is(expected));
    }

    static Stream<Arguments> mergedUpdatesInput() {
        return Stream.of(
                // last synced state, first local update, second local update
                Arguments.of("{\"desired\":{\"a\":5}}", "{\"desired\":{\"a\":1}}",
                        "{\"desired\":{\"a\":null}}"),
                Arguments.of("{\"desired\":{\"a\":5}}", "{\"desired\":{\"b\":1}}",
                        "{\"desired\":{\"a\":null}}"),
                Arguments.of("{\"desired\":{\"a\":{\"x\":5}}}", "{\"desired\":{\"b\":1}}",
                        "{\"desired\":{\"a\":{\"x\":null}}}"),
                Arguments.of("{\"desired\":{\"a\":5}}", "{\"desired\":{\"b\":1}}",
                        "{\"desired\":null}"),
                Arguments.of("{\"desired\":{\"a\":5},\"reported\":{\"r\":1}}", "{\"desired\":{\"b\":1}}",
                        "null"),
                Arguments.of("{\"desired\":{\"a\":{\"x\":5,\"y\":6}}}", "{\"desired\":{\"a\":null}}",
                        "{\"desired\":{\"a\":{\"x\":1}}}"),
                Arguments.of("{\"reported\":{\"c\":{\"a\":1,\"b\":1}}}", "{\"reported\":{\"c\":2}}",
                        "{\"reported\":{\"c\":{\"b\":0}}}"),
                Arguments.of("{\"desired\":{\"z\":1}}", "{\"desired\":{\"a\":{\"x\":1}}}",
                        "{\"desired\":{\"a\":{}}}"),
                Arguments.of("{\"desired\":{\"z\":1}}", "{\"desired\":{\"a\":1}}",
                        "{\"desired\":{}}"),
                Arguments.of("{\"desired\":{\"z\":1}}", "{\"desired\":{\"a\":1}}",
                        "{\"desired\":{\"b\":{}}}")
        );
    }

    @ParameterizedTest
    @MethodSource("mergedUpdatesInput")
    void GIVEN_two_merged_local_updates_WHEN_execute_THEN_cloud_gets_the_change_since_last_sync(String synced,
            String first, String second) throws Exception {
        byte[] lastSynced = ("{\"state\":" + synced + ",\"version\":1}").getBytes();
        JsonNode firstUpdate = JsonUtil.getPayloadJson(("{\"state\":" + first + "}").getBytes()).get();
        JsonNode secondUpdate = JsonUtil.getPayloadJson(("{\"state\":" + second + "}").getBytes()).get();
        ShadowDocument afterFirst = new ShadowDocument(new ShadowDocument(lastSynced));
        afterFirst.update(firstUpdate);
        ShadowDocument afterSecond = new ShadowDocument(afterFirst);
        afterSecond.update(secondUpdate);
        when(mockDao.getShadowSyncInformation(anyString(), anyString())).thenReturn(Optional.of(SyncInformation.builder()
                .thingName(THING_NAME).shadowName(SHADOW_NAME).lastSyncedDocument(lastSynced)
                .cloudVersion(5L).localVersion(1L).build()));
        ArgumentCaptor<byte[]> payloadCaptor = ArgumentCaptor.forClass(byte[].class);
        lenient().when(mockIotDataPlaneClientWrapper.updateThingShadow(anyString(), anyString(), payloadCaptor.capture()))
                .thenReturn(UpdateThingShadowResponse.builder().payload(SdkBytes.fromString("{\"version\": 6}", UTF_8)).build());

        CloudUpdateSyncRequest request = new CloudUpdateSyncRequest(THING_NAME, SHADOW_NAME, firstUpdate, afterFirst);
        request.merge(new CloudUpdateSyncRequest(THING_NAME, SHADOW_NAME, secondUpdate, afterSecond));
        request.execute(mockContext);

        // the cloud applies the update like IoT Device Shadow and ends with the local state
        JsonNode cloudState = JsonUtil.getPayloadJson(lastSynced).get().get("state");
        if (!payloadCaptor.getAllValues().isEmpty()) {
            JsonNode sent = JsonUtil.getPayloadJson(payloadCaptor.getValue()).get();
            assertThat(sent.get("version").asLong(), is(5L));
            cloudState = JsonMerger.applyShadowStateUpdate(cloudState, sent.get("state"));
        }
        assertThat(JsonMerger.applyShadowStateUpdate(cloudState, JsonUtil.OBJECT_MAPPER.createObjectNode()),
                is(afterSecond.getState().toJson()));
    }
}
