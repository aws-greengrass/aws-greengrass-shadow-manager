/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.shadowmanager.ipc;

import com.aws.greengrass.shadowmanager.exception.ThrottledRequestException;
import com.aws.greengrass.testcommons.testutilities.GGExtension;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import software.amazon.awssdk.aws.greengrass.model.GetThingShadowRequest;
import software.amazon.awssdk.aws.greengrass.model.ServiceError;
import software.amazon.awssdk.crt.eventstream.ServerConnectionContinuation;
import software.amazon.awssdk.eventstreamrpc.AuthenticationData;
import software.amazon.awssdk.eventstreamrpc.OperationContinuationHandlerContext;
import software.amazon.awssdk.eventstreamrpc.model.EventStreamJsonMessage;

import static com.aws.greengrass.shadowmanager.TestUtils.TEST_SERVICE;
import static com.aws.greengrass.testcommons.testutilities.ExceptionLogProtector.ignoreExceptionOfType;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

@ExtendWith({MockitoExtension.class, GGExtension.class})
class GetThingShadowIPCHandlerTest {

    @Mock
    OperationContinuationHandlerContext mockContext;
    @Mock
    AuthenticationData mockAuthenticationData;
    @Mock
    InboundRateLimiter mockInboundRateLimiter;
    @Mock
    GetThingShadowRequestHandler mockGetThingShadowRequestHandler;

    @BeforeEach
    void setup() throws ThrottledRequestException {
        lenient().when(mockContext.getContinuation()).thenReturn(mock(ServerConnectionContinuation.class));
        lenient().when(mockContext.getAuthenticationData()).thenReturn(mockAuthenticationData);
        lenient().when(mockAuthenticationData.getIdentityLabel()).thenReturn(TEST_SERVICE);
        lenient().doNothing().when(mockInboundRateLimiter).acquireLockForThing(any());
    }

    @Test
    void GIVEN_get_thing_shadow_ipc_handler_WHEN_handle_request_THEN_request_handler_is_called() {
        try (GetThingShadowIPCHandler getThingShadowIPCHandler = new GetThingShadowIPCHandler(mockContext, mockInboundRateLimiter, mockGetThingShadowRequestHandler)) {
            assertDoesNotThrow(() -> getThingShadowIPCHandler.handleRequest(mock(GetThingShadowRequest.class)));

            verify(mockGetThingShadowRequestHandler, times(1)).handleRequest(any(GetThingShadowRequest.class), anyString());
        }
    }

    @Test
    void GIVEN_throttled_get_request_WHEN_handle_request_THEN_service_error_thrown(ExtensionContext context) throws ThrottledRequestException {
        ignoreExceptionOfType(context, ThrottledRequestException.class);
        doThrow(ThrottledRequestException.class).when(mockInboundRateLimiter).acquireLockForThing(any());

        try (GetThingShadowIPCHandler getThingShadowIPCHandler = new GetThingShadowIPCHandler(mockContext, mockInboundRateLimiter, mockGetThingShadowRequestHandler)) {
            ServiceError thrown = assertThrows(ServiceError.class, () -> getThingShadowIPCHandler.handleRequest(mock(GetThingShadowRequest.class)));
            assertThat(thrown.getMessage(), is(equalTo("Too Many Requests")));

            verify(mockGetThingShadowRequestHandler, times(0)).handleRequest(any(GetThingShadowRequest.class), anyString());
        }
    }

    @Test
    void GIVEN_get_thing_shadow_ipc_handler_WHEN_handle_stream_event_THEN_nothing_happens() {
        try (GetThingShadowIPCHandler getThingShadowIPCHandler = new GetThingShadowIPCHandler(mockContext, mockInboundRateLimiter, mockGetThingShadowRequestHandler)) {
            assertDoesNotThrow(() -> getThingShadowIPCHandler.handleStreamEvent(mock(EventStreamJsonMessage.class)));
        }
    }

    @Test
    void GIVEN_get_thing_shadow_ipc_handler_WHEN_stream_closes_THEN_nothing_happens() {
        try (GetThingShadowIPCHandler getThingShadowIPCHandler = new GetThingShadowIPCHandler(mockContext, mockInboundRateLimiter, mockGetThingShadowRequestHandler)) {
            assertDoesNotThrow(getThingShadowIPCHandler::onStreamClosed);
        }
    }
}
