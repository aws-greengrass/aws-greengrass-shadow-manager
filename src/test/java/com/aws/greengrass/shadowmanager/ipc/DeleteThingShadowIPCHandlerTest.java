/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.shadowmanager.ipc;

import com.aws.greengrass.testcommons.testutilities.GGExtension;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import software.amazon.awssdk.aws.greengrass.model.DeleteThingShadowRequest;
import software.amazon.awssdk.crt.eventstream.ServerConnectionContinuation;
import software.amazon.awssdk.eventstreamrpc.AuthenticationData;
import software.amazon.awssdk.eventstreamrpc.OperationContinuationHandlerContext;
import software.amazon.awssdk.eventstreamrpc.model.EventStreamJsonMessage;

import static com.aws.greengrass.shadowmanager.TestUtils.TEST_SERVICE;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

@ExtendWith({MockitoExtension.class, GGExtension.class})
class DeleteThingShadowIPCHandlerTest {
    @Mock
    OperationContinuationHandlerContext mockContext;
    @Mock
    AuthenticationData mockAuthenticationData;
    @Mock
    DeleteThingShadowRequestHandler mockDeleteThingShadowRequestHandler;

    @BeforeEach
    void setup() {
        lenient().when(mockContext.getContinuation()).thenReturn(mock(ServerConnectionContinuation.class));
        lenient().when(mockContext.getAuthenticationData()).thenReturn(mockAuthenticationData);
        lenient().when(mockAuthenticationData.getIdentityLabel()).thenReturn(TEST_SERVICE);
    }

    @Test
    void GIVEN_delete_thing_shadow_ipc_handler_WHEN_handle_request_THEN_request_handler_is_called() {
        try (DeleteThingShadowIPCHandler deleteThingShadowIPCHandler = new DeleteThingShadowIPCHandler(mockContext, mockDeleteThingShadowRequestHandler)) {
            assertDoesNotThrow(() -> deleteThingShadowIPCHandler.handleRequest(mock(DeleteThingShadowRequest.class)));

            verify(mockDeleteThingShadowRequestHandler, times(1)).handleRequest(any(DeleteThingShadowRequest.class), anyString());
        }
    }

    @Test
    void GIVEN_delete_thing_shadow_ipc_handler_WHEN_handle_stream_event_THEN_nothing_happens() {
        try (DeleteThingShadowIPCHandler deleteThingShadowIPCHandler = new DeleteThingShadowIPCHandler(mockContext, mockDeleteThingShadowRequestHandler)) {
            assertDoesNotThrow(() -> deleteThingShadowIPCHandler.handleStreamEvent(mock(EventStreamJsonMessage.class)));
        }
    }

    @Test
    void GIVEN_delete_thing_shadow_ipc_handler_WHEN_stream_closes_THEN_nothing_happens() {
        try (DeleteThingShadowIPCHandler deleteThingShadowIPCHandler = new DeleteThingShadowIPCHandler(mockContext, mockDeleteThingShadowRequestHandler)) {
            assertDoesNotThrow(deleteThingShadowIPCHandler::onStreamClosed);
        }
    }
}
