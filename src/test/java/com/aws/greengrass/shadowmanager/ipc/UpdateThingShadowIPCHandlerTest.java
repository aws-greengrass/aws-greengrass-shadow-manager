package com.aws.greengrass.shadowmanager.ipc;

import com.aws.greengrass.authorization.AuthorizationHandler;
import com.aws.greengrass.authorization.Permission;
import com.aws.greengrass.authorization.exceptions.AuthorizationException;
import com.aws.greengrass.shadowmanager.ShadowManagerDAO;
import com.aws.greengrass.testcommons.testutilities.GGExtension;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import software.amazon.awssdk.aws.greengrass.model.*;
import software.amazon.awssdk.crt.eventstream.ServerConnectionContinuation;
import software.amazon.awssdk.eventstreamrpc.AuthenticationData;
import software.amazon.awssdk.eventstreamrpc.OperationContinuationHandlerContext;

import java.util.Optional;

import static com.aws.greengrass.testcommons.testutilities.ExceptionLogProtector.ignoreExceptionOfType;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@ExtendWith({MockitoExtension.class, GGExtension.class})
public class UpdateThingShadowIPCHandlerTest {

    private static final String TEST_SERVICE = "TestService";
    private static final String THING_NAME = "testThingName";
    private static final String SHADOW_NAME = "testShadowName";
    private static final byte[] UPDATE_DOCUMENT =  "{\"id\": 1, \"name\": \"The Beatles\"}".getBytes();

    @Mock
    OperationContinuationHandlerContext mockContext;

    @Mock
    AuthenticationData mockAuthenticationData;

    @Mock
    AuthorizationHandler mockAuthorizationHandler;

    @Mock
    ShadowManagerDAO mockDao;

    @BeforeEach
    void setup () {
        when(mockContext.getContinuation()).thenReturn(mock(ServerConnectionContinuation.class));
        when(mockContext.getAuthenticationData()).thenReturn(mockAuthenticationData);
        when(mockAuthenticationData.getIdentityLabel()).thenReturn(TEST_SERVICE);
    }

    @Test
    void GIVEN_update_thing_shadow_ipc_handler_WHEN_handle_request_THEN_update_thing_shadow() {
        UpdateThingShadowRequest request = new UpdateThingShadowRequest();
        request.setThingName(THING_NAME);
        request.setShadowName(SHADOW_NAME);
        request.setPayload(UPDATE_DOCUMENT);

        UpdateThingShadowResponse expectedResponse = new UpdateThingShadowResponse();
        expectedResponse.setPayload(UPDATE_DOCUMENT);

        UpdateThingShadowIPCHandler updateThingShadowIPCHandler = new UpdateThingShadowIPCHandler(mockContext, mockDao, mockAuthorizationHandler);
        when(mockDao.updateShadowThing(any(), any(), any())).thenReturn(Optional.of(UPDATE_DOCUMENT));

        UpdateThingShadowResponse actualResponse = updateThingShadowIPCHandler.handleRequest(request);
        assertEquals(expectedResponse, actualResponse);
    }

    @Test
    void GIVEN_update_thing_shadow_ipc_handler_WHEN_missing_payload_THEN_update_thing_shadow(ExtensionContext context) {
        ignoreExceptionOfType(context, InvalidArgumentsError.class);
        UpdateThingShadowRequest request = new UpdateThingShadowRequest();
        request.setThingName(THING_NAME);
        request.setShadowName(SHADOW_NAME);
        UpdateThingShadowIPCHandler updateThingShadowIPCHandler = new UpdateThingShadowIPCHandler(mockContext, mockDao, mockAuthorizationHandler);

        assertThrows(InvalidArgumentsError.class, () -> updateThingShadowIPCHandler.handleRequest(request));
    }

    @Test
    void GIVEN_update_thing_shadow_ipc_handler_WHEN_ipc_request_unauthorized_THEN_throw_unauthorized_exception(ExtensionContext context) throws Exception {
        ignoreExceptionOfType(context, AuthorizationException.class);
        UpdateThingShadowRequest request = new UpdateThingShadowRequest();
        request.setThingName(THING_NAME);
        request.setShadowName(SHADOW_NAME);
        request.setPayload(UPDATE_DOCUMENT);
        when(mockAuthorizationHandler.isAuthorized(any(), any(Permission.class)))
                .thenThrow(AuthorizationException.class);

        UpdateThingShadowIPCHandler updateThingShadowIPCHandler = new UpdateThingShadowIPCHandler(mockContext, mockDao, mockAuthorizationHandler);
        assertThrows(UnauthorizedError.class, () -> updateThingShadowIPCHandler.handleRequest(request));
    }

    @Test
    void GIVEN_update_thing_shadow_ipc_handler_WHEN_missing_thing_name_THEN_throw_invalid_arguments_exception(ExtensionContext context) throws Exception {
        ignoreExceptionOfType(context, InvalidArgumentsError.class);
        UpdateThingShadowRequest request = new UpdateThingShadowRequest();
        request.setThingName("");
        request.setShadowName(SHADOW_NAME);
        request.setPayload(UPDATE_DOCUMENT);

        UpdateThingShadowIPCHandler updateThingShadowIPCHandler = new UpdateThingShadowIPCHandler(mockContext, mockDao, mockAuthorizationHandler);
        assertThrows(InvalidArgumentsError.class, () -> updateThingShadowIPCHandler.handleRequest(request));
    }
}
