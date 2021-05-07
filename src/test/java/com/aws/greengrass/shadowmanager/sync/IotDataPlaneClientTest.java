/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.shadowmanager.sync;

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
import org.mockito.invocation.InvocationOnMock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.stubbing.Answer;
import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.core.exception.SdkException;
import software.amazon.awssdk.services.iotdataplane.model.ConflictException;
import software.amazon.awssdk.services.iotdataplane.model.DeleteThingShadowRequest;
import software.amazon.awssdk.services.iotdataplane.model.DeleteThingShadowResponse;
import software.amazon.awssdk.services.iotdataplane.model.GetThingShadowRequest;
import software.amazon.awssdk.services.iotdataplane.model.GetThingShadowResponse;
import software.amazon.awssdk.services.iotdataplane.model.InternalFailureException;
import software.amazon.awssdk.services.iotdataplane.model.InvalidRequestException;
import software.amazon.awssdk.services.iotdataplane.model.IotDataPlaneException;
import software.amazon.awssdk.services.iotdataplane.model.MethodNotAllowedException;
import software.amazon.awssdk.services.iotdataplane.model.ResourceNotFoundException;
import software.amazon.awssdk.services.iotdataplane.model.RequestEntityTooLargeException;
import software.amazon.awssdk.services.iotdataplane.model.ServiceUnavailableException;
import software.amazon.awssdk.services.iotdataplane.model.ThrottlingException;
import software.amazon.awssdk.services.iotdataplane.model.UnauthorizedException;
import software.amazon.awssdk.services.iotdataplane.model.UnsupportedDocumentEncodingException;
import software.amazon.awssdk.services.iotdataplane.model.UpdateThingShadowRequest;
import software.amazon.awssdk.services.iotdataplane.model.UpdateThingShadowResponse;
import vendored.com.google.common.util.concurrent.RateLimiter;

import java.time.Instant;

import static com.aws.greengrass.shadowmanager.TestUtils.SHADOW_NAME;
import static com.aws.greengrass.shadowmanager.TestUtils.THING_NAME;
import static com.aws.greengrass.testcommons.testutilities.ExceptionLogProtector.ignoreExceptionOfType;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.verify;

@ExtendWith({MockitoExtension.class, GGExtension.class})
public class IotDataPlaneClientTest {
    private static final byte[] UPDATE_PAYLOAD = "{\"version\": 1, \"state\": {\"reported\": {\"name\": \"The Beatles\"}}}".getBytes();

    @Mock
    software.amazon.awssdk.services.iotdataplane.IotDataPlaneClient mockIotDataPlaneClient;

    @Mock
    IotDataPlaneClientFactory iotDataPlaneClientFactory;

    @Captor
    ArgumentCaptor<DeleteThingShadowRequest> deleteThingShadowRequestArgumentCaptor;

    @Captor
    ArgumentCaptor<GetThingShadowRequest> getThingShadowRequestArgumentCaptor;

    @Captor
    ArgumentCaptor<UpdateThingShadowRequest> updateThingShadowRequestArgumentCaptor;


    @BeforeEach
    void setup() {
        lenient().when(iotDataPlaneClientFactory.getIotDataPlaneClient()).thenReturn(mockIotDataPlaneClient);
    }

    @Test
    void GIVEN_valid_request_WHEN_update_thing_shadow_THEN_returns_update_thing_shadow_response() {
        // GIVEN
        when(mockIotDataPlaneClient.updateThingShadow(updateThingShadowRequestArgumentCaptor.capture())).thenReturn(UpdateThingShadowResponse.builder().build());
        IotDataPlaneClient iotDataPlaneClient = new IotDataPlaneClient(iotDataPlaneClientFactory);

        // WHEN
        UpdateThingShadowResponse updateThingShadowResponse = iotDataPlaneClient.updateThingShadow(THING_NAME, SHADOW_NAME, UPDATE_PAYLOAD);

        //THEN
        UpdateThingShadowRequest updateThingShadowRequest = updateThingShadowRequestArgumentCaptor.getValue();
        assertThat(updateThingShadowRequest.thingName(), is(THING_NAME));
        assertThat(updateThingShadowRequest.shadowName(), is(SHADOW_NAME));

        assertThat(updateThingShadowResponse, is(notNullValue()));
    }

    @Test
    void GIVEN_valid_request_WHEN_get_thing_shadow_THEN_returns_get_thing_shadow_response() {
        // GIVEN
        when(mockIotDataPlaneClient.getThingShadow(getThingShadowRequestArgumentCaptor.capture())).thenReturn(GetThingShadowResponse.builder().build());
        IotDataPlaneClient iotDataPlaneClient = new IotDataPlaneClient(iotDataPlaneClientFactory);

        // WHEN
        GetThingShadowResponse getThingShadowResponse = iotDataPlaneClient.getThingShadow(THING_NAME, SHADOW_NAME);

        //THEN
        GetThingShadowRequest getThingShadowRequest = getThingShadowRequestArgumentCaptor.getValue();
        assertThat(getThingShadowRequest.thingName(), is(THING_NAME));
        assertThat(getThingShadowRequest.shadowName(), is(SHADOW_NAME));

        assertThat(getThingShadowResponse, is(notNullValue()));
    }

    @Test
    void GIVEN_valid_request_WHEN_delete_thing_shadow_THEN_returns_delete_thing_shadow_response() {
        // GIVEN
        when(mockIotDataPlaneClient.deleteThingShadow(deleteThingShadowRequestArgumentCaptor.capture())).thenReturn(DeleteThingShadowResponse.builder().build());
        IotDataPlaneClient iotDataPlaneClient = new IotDataPlaneClient(iotDataPlaneClientFactory);

        // WHEN
        DeleteThingShadowResponse deleteThingShadowResponse = iotDataPlaneClient.deleteThingShadow(THING_NAME, SHADOW_NAME);

        //THEN
        DeleteThingShadowRequest deleteThingShadowRequest = deleteThingShadowRequestArgumentCaptor.getValue();
        assertThat(deleteThingShadowRequest.thingName(), is(THING_NAME));
        assertThat(deleteThingShadowRequest.shadowName(), is(SHADOW_NAME));

        assertThat(deleteThingShadowResponse, is(notNullValue()));
    }

    @Test
    void GIVEN_valid_request_throttled_WHEN_get_thing_shadow_THEN_request_executed_when_lock_acquired() {
        // GIVEN
        RateLimiter mockRateLimiter = mock(RateLimiter.class);
        when(mockIotDataPlaneClient.getThingShadow(getThingShadowRequestArgumentCaptor.capture())).thenReturn(GetThingShadowResponse.builder().build());
        when(mockRateLimiter.acquire()).thenAnswer(new Answer<Double>() {
            @Override
            public Double answer(InvocationOnMock invocationOnMock) throws Throwable {
                Thread.sleep(5000L);
                return 5000D;
            }
        });

        // WHEN
        long start = Instant.now().toEpochMilli();
        IotDataPlaneClient iotDataPlaneClient = new IotDataPlaneClient(iotDataPlaneClientFactory, mockRateLimiter);
        iotDataPlaneClient.getThingShadow(THING_NAME, SHADOW_NAME);

        //THEN
        long current = Instant.now().toEpochMilli();
        assertThat("Retrieved lock after 5 seconds", current - start, is(greaterThanOrEqualTo(5000L)));
        verify(mockIotDataPlaneClient, times(1)).getThingShadow(any(GetThingShadowRequest.class));
        verify(mockRateLimiter, times(1)).acquire();
    }

    @ParameterizedTest
    @ValueSource(classes = {ConflictException.class, RequestEntityTooLargeException.class, InvalidRequestException.class, ThrottlingException.class,
            UnauthorizedException.class, ServiceUnavailableException.class, InternalFailureException.class, MethodNotAllowedException.class,
            UnsupportedDocumentEncodingException.class, AwsServiceException.class, SdkClientException.class, IotDataPlaneException.class})
    void GIVEN_exception_during_update_WHEN_update_thing_shadow_THEN_throw_sdk_exception(Class clazz, ExtensionContext context) {
        // GIVEN
        when(mockIotDataPlaneClient.getThingShadow(getThingShadowRequestArgumentCaptor.capture())).thenReturn(GetThingShadowResponse.builder().build());
        IotDataPlaneClient iotDataPlaneClient = new IotDataPlaneClient(iotDataPlaneClientFactory);

        // WHEN
        GetThingShadowResponse getThingShadowResponse = iotDataPlaneClient.getThingShadow(THING_NAME, SHADOW_NAME);

        //THEN
        GetThingShadowRequest getThingShadowRequest = getThingShadowRequestArgumentCaptor.getValue();
        assertThat(getThingShadowRequest.thingName(), is(THING_NAME));
        assertThat(getThingShadowRequest.shadowName(), is(SHADOW_NAME));

        assertThat(getThingShadowResponse, is(notNullValue()));
    }

    @ParameterizedTest
    @ValueSource(classes = {ResourceNotFoundException.class, InvalidRequestException.class, ThrottlingException.class,
            UnauthorizedException.class, ServiceUnavailableException.class, InternalFailureException.class, MethodNotAllowedException.class,
            UnsupportedDocumentEncodingException.class, AwsServiceException.class, SdkClientException.class, IotDataPlaneException.class})
    void GIVEN_exception_during_delete_WHEN_delete_thing_shadow_THEN_throw_sdk_exception(Class clazz, ExtensionContext context) {
        // GIVEN
        ignoreExceptionOfType(context, clazz);
        when(mockIotDataPlaneClient.deleteThingShadow(deleteThingShadowRequestArgumentCaptor.capture())).thenThrow(clazz);
        IotDataPlaneClient iotDataPlaneClient = new IotDataPlaneClient(iotDataPlaneClientFactory);

        // WHEN
        SdkException thrown = assertThrows(SdkException.class, () -> iotDataPlaneClient.deleteThingShadow(THING_NAME, SHADOW_NAME));

        //THEN
        assertThat(thrown.getClass(), is(clazz));

        DeleteThingShadowRequest deleteThingShadowRequest = deleteThingShadowRequestArgumentCaptor.getValue();
        assertThat(deleteThingShadowRequest.thingName(), is(THING_NAME));
        assertThat(deleteThingShadowRequest.shadowName(), is(SHADOW_NAME));
    }

    @ParameterizedTest
    @ValueSource(classes = {ResourceNotFoundException.class, InvalidRequestException.class, ThrottlingException.class,
            UnauthorizedException.class, ServiceUnavailableException.class, InternalFailureException.class, MethodNotAllowedException.class,
            UnsupportedDocumentEncodingException.class, AwsServiceException.class, SdkClientException.class, IotDataPlaneException.class})
    void GIVEN_exception_during_get_WHEN_get_thing_shadow_THEN_throw_sdk_exception(Class clazz, ExtensionContext context) {
        // GIVEN
        ignoreExceptionOfType(context, clazz);
        when(mockIotDataPlaneClient.getThingShadow(getThingShadowRequestArgumentCaptor.capture())).thenThrow(clazz);
        IotDataPlaneClient iotDataPlaneClient = new IotDataPlaneClient(iotDataPlaneClientFactory);

        // WHEN
        SdkException thrown = assertThrows(SdkException.class, () -> iotDataPlaneClient.getThingShadow(THING_NAME, SHADOW_NAME));

        //THEN
        assertThat(thrown.getClass(), is(clazz));

        GetThingShadowRequest getThingShadowRequest = getThingShadowRequestArgumentCaptor.getValue();
        assertThat(getThingShadowRequest.thingName(), is(THING_NAME));
        assertThat(getThingShadowRequest.shadowName(), is(SHADOW_NAME));
    }
}
