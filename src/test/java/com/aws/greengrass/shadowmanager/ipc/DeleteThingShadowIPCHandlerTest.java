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
public class DeleteThingShadowIPCHandlerTest {

    private static final String TEST_SERVICE = "TestService";
    private static final String THING_NAME = "testThingName";
    private static final String SHADOW_NAME = "testShadowName";
    private static final byte[] BASE_DOCUMENT =  "{\"id\": 1, \"name\": \"The Beatles\"}".getBytes();

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
    void GIVEN_delete_thing_shadow_ipc_handler_WHEN_handle_request_THEN_delete_thing_shadow() {

        DeleteThingShadowRequest request = new DeleteThingShadowRequest();
        request.setThingName(THING_NAME);
        request.setShadowName(SHADOW_NAME);

        DeleteThingShadowResponse expectedResponse = new DeleteThingShadowResponse();
        expectedResponse.setPayload(BASE_DOCUMENT);

        DeleteThingShadowIPCHandler deleteThingShadowIPCHandler = new DeleteThingShadowIPCHandler(mockContext, mockDao, mockAuthorizationHandler);
        when(mockDao.deleteShadowThing(any(), any())).thenReturn(Optional.of(BASE_DOCUMENT));

        DeleteThingShadowResponse actualResponse = deleteThingShadowIPCHandler.handleRequest(request);
        assertEquals(expectedResponse, actualResponse);
    }

    @Test
    void GIVEN_delete_thing_shadow_ipc_handler_WHEN_document_not_found_THEN_throw_resource_not_found_exception(ExtensionContext context) throws Exception {
        ignoreExceptionOfType(context, ResourceNotFoundError.class);
        DeleteThingShadowRequest request = new DeleteThingShadowRequest();
        request.setThingName(THING_NAME);
        request.setShadowName(SHADOW_NAME);

        when(mockDao.deleteShadowThing(any(), any())).thenReturn(Optional.empty());
        DeleteThingShadowIPCHandler deleteThingShadowIPCHandler = new DeleteThingShadowIPCHandler(mockContext, mockDao, mockAuthorizationHandler);
        assertThrows(ResourceNotFoundError.class, () -> deleteThingShadowIPCHandler.handleRequest(request));
    }

    @Test
    void GIVEN_delete_thing_shadow_ipc_handler_WHEN_ipc_request_unauthorized_THEN_throw_unauthorized_exception(ExtensionContext context) throws Exception {
        ignoreExceptionOfType(context, AuthorizationException.class);
        DeleteThingShadowRequest request = new DeleteThingShadowRequest();
        request.setThingName(THING_NAME);
        request.setShadowName(SHADOW_NAME);
        when(mockAuthorizationHandler.isAuthorized(any(), any(Permission.class)))
                .thenThrow(AuthorizationException.class);

        DeleteThingShadowIPCHandler deleteThingShadowIPCHandler = new DeleteThingShadowIPCHandler(mockContext, mockDao, mockAuthorizationHandler);
        assertThrows(UnauthorizedError.class, () -> deleteThingShadowIPCHandler.handleRequest(request));
    }

    @Test
    void GIVEN_delete_thing_shadow_ipc_handler_WHEN_missing_thing_name_THEN_throw_invalid_arguments_exception(ExtensionContext context) throws Exception {
        ignoreExceptionOfType(context, InvalidArgumentsError.class);
        DeleteThingShadowRequest request = new DeleteThingShadowRequest();
        request.setThingName("");
        request.setShadowName(SHADOW_NAME);

        DeleteThingShadowIPCHandler deleteThingShadowIPCHandler = new DeleteThingShadowIPCHandler(mockContext, mockDao, mockAuthorizationHandler);
        assertThrows(InvalidArgumentsError.class, () -> deleteThingShadowIPCHandler.handleRequest(request));
    }
}
