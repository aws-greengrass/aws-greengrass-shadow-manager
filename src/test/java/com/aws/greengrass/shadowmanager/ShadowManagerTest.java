/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.shadowmanager;

import com.aws.greengrass.authorization.AuthorizationHandler;
import com.aws.greengrass.authorization.Permission;
import com.aws.greengrass.authorization.exceptions.AuthorizationException;
import com.aws.greengrass.dependency.State;
import com.aws.greengrass.lifecyclemanager.GreengrassService;
import com.aws.greengrass.lifecyclemanager.GlobalStateChangeListener;
import com.aws.greengrass.lifecyclemanager.Kernel;
import com.aws.greengrass.testcommons.testutilities.GGExtension;
import com.aws.greengrass.testcommons.testutilities.GGServiceTestUtil;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import software.amazon.awssdk.aws.greengrass.model.GetThingShadowRequest;
import software.amazon.awssdk.aws.greengrass.model.GetThingShadowResponse;
import software.amazon.awssdk.aws.greengrass.model.ResourceNotFoundError;
import software.amazon.awssdk.aws.greengrass.model.UnauthorizedError;

import java.nio.file.Path;
import java.sql.SQLException;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static com.aws.greengrass.testcommons.testutilities.ExceptionLogProtector.ignoreExceptionOfType;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith({MockitoExtension.class, GGExtension.class})
public class ShadowManagerTest extends GGServiceTestUtil {
    private static final long TEST_TIME_OUT_SEC = 30L;
    private static final String MOCK_SERVICE_NAME = "mockService";
    private static final String THING_NAME = "testThingName";
    private static final String SHADOW_NAME = "testShadowName";
    private static final byte[] BASE_DOCUMENT =  "{\"id\": 1, \"name\": \"The Beatles\"}".getBytes();
    private static final Map<String, Object> BASE_DOCUMENT_PAYLOAD = ShadowManager.convertBytesToPayload(BASE_DOCUMENT);

    private Kernel kernel;
    private GlobalStateChangeListener listener;

    @TempDir
    Path rootDir;

    @Mock
    AuthorizationHandler mockAuthorizationHandler;

    @Mock
    ShadowManagerDatabase mockShadowManagerDatabase;

    @Mock
    ShadowManagerDAOImpl mockShadowManagerDAOImpl;

    @BeforeEach
    void setup() {
        kernel = new Kernel();
    }

    @AfterEach
    void cleanup() {
        kernel.shutdown();
    }

    private void startNucleusWithConfig(String configFile, State expectedState) throws InterruptedException {
        CountDownLatch shadowManagerRunning = new CountDownLatch(1);
        kernel.parseArgs("-r", rootDir.toAbsolutePath().toString(), "-i",
                getClass().getResource(configFile).toString());
        listener = (GreengrassService service, State was, State newState) -> {
            if (service.getName().equals(ShadowManager.SERVICE_NAME) && service.getState().equals(expectedState)) {
                shadowManagerRunning.countDown();
            }
        };
        kernel.getContext().addGlobalStateChangeListener(listener);
        kernel.getContext().put(ShadowManagerDatabase.class, mockShadowManagerDatabase);
        kernel.getContext().put(ShadowManagerDAOImpl.class, mockShadowManagerDAOImpl);
        kernel.getContext().put(AuthorizationHandler.class, mockAuthorizationHandler);
        kernel.launch();

        assertTrue(shadowManagerRunning.await(TEST_TIME_OUT_SEC, TimeUnit.SECONDS));
    }

    @Test
    void GIVEN_Greengrass_with_shadow_manager_WHEN_start_nucleus_THEN_shadow_manager_starts_successfully() throws Exception {
        startNucleusWithConfig("config.yaml", State.RUNNING);
    }

    @Test
    void GIVEN_Greengrass_with_shadow_manager_WHEN_database_install_fails_THEN_service_errors(ExtensionContext context) throws Exception {
        ignoreExceptionOfType(context, SQLException.class);

        doThrow(SQLException.class).when(mockShadowManagerDatabase).install();
        startNucleusWithConfig("config.yaml", State.ERRORED);
    }

    @Test
    void GIVEN_Greengrass_with_shadow_manager_WHEN_nucleus_shutdown_THEN_shadow_manager_database_closes() throws Exception {
        startNucleusWithConfig("config.yaml", State.RUNNING);
        kernel.shutdown();
        verify(mockShadowManagerDatabase, atLeastOnce()).close();
    }

    @Test
    void GIVEN_shadow_manager_When_log_event_occurs_THEN_code_returned() {
        for(ShadowManager.LogEvents logEvent : ShadowManager.LogEvents.values()) {
            assertFalse(logEvent.code.isEmpty());
        }
    }

    @Test
    void GIVEN_shadow_manager_WHEN_ipc_request_unauthorized_THEN_throws_unauthorized_exception(ExtensionContext context) throws Exception {
        ignoreExceptionOfType(context, AuthorizationException.class);
        startNucleusWithConfig("config.yaml", State.RUNNING);
        GetThingShadowRequest request = new GetThingShadowRequest();
        request.setThingName(THING_NAME);
        request.setShadowName(SHADOW_NAME);
        when(mockAuthorizationHandler.isAuthorized(any(), any(Permission.class)))
                .thenThrow(AuthorizationException.class);

        assertThrows(UnauthorizedError.class,
                () -> kernel.getContext().get(ShadowManager.class).handleGetThingShadowIPCRequest(request, MOCK_SERVICE_NAME));
    }

    @Test
    void GIVEN_shadow_manager_WHEN_document_not_found_THEN_throws_resource_not_found_exception(ExtensionContext context) throws Exception {
        ignoreExceptionOfType(context, ResourceNotFoundError.class);
        startNucleusWithConfig("config.yaml", State.RUNNING);
        GetThingShadowRequest request = new GetThingShadowRequest();
        request.setThingName(THING_NAME);
        request.setShadowName(SHADOW_NAME);
        when(mockShadowManagerDAOImpl.getShadowThing(any(), any())).thenReturn(Optional.empty());

        assertThrows(ResourceNotFoundError.class,
                () -> kernel.getContext().get(ShadowManager.class).handleGetThingShadowIPCRequest(request, MOCK_SERVICE_NAME));
    }

    @Test
    void GIVEN_shadow_manager_When_get_thing_shadow_handler_called_Then_correct_response_returned() throws Exception {
        startNucleusWithConfig("config.yaml", State.RUNNING);
        GetThingShadowRequest request = new GetThingShadowRequest();
        request.setThingName(THING_NAME);
        request.setShadowName(SHADOW_NAME);

        GetThingShadowResponse expectedResponse = new GetThingShadowResponse();
        expectedResponse.setPayload(BASE_DOCUMENT_PAYLOAD);
        when(mockShadowManagerDAOImpl.getShadowThing(any(), any())).thenReturn(Optional.of(BASE_DOCUMENT));
        GetThingShadowResponse actualResponse = kernel.getContext().get(ShadowManager.class).handleGetThingShadowIPCRequest(request, MOCK_SERVICE_NAME);

        assertEquals(expectedResponse, actualResponse);
    }
}
