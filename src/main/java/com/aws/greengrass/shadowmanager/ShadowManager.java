/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.shadowmanager;

import com.aws.greengrass.authorization.AuthorizationHandler;
import com.aws.greengrass.authorization.Permission;
import com.aws.greengrass.authorization.exceptions.AuthorizationException;
import com.aws.greengrass.certificatemanager.DCMService;
import com.aws.greengrass.config.Topics;
import com.aws.greengrass.dependency.ImplementsService;
import com.aws.greengrass.dependency.State;
import com.aws.greengrass.lifecyclemanager.Kernel;
import com.aws.greengrass.lifecyclemanager.PluginService;
import com.aws.greengrass.lifecyclemanager.exceptions.ServiceLoadException;
import com.aws.greengrass.util.Coerce;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.flywaydb.core.api.FlywayException;
import software.amazon.awssdk.aws.greengrass.GreengrassCoreIPCService;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import javax.inject.Inject;

//import static software.amazon.awssdk.aws.greengrass.GreengrassCoreIPCService.GET_THING_SHADOW;

@ImplementsService(name = ShadowManager.SERVICE_NAME)
public class ShadowManager extends PluginService {
    enum LogEvents {
        AUTHORIZATION_ERROR("shadow-authorization-error"),
        IPC_REGISTRATION("shadow-ipc-registration"),
        IPC_ERROR("shadow-ipc-error"),
        DCM_ERROR("shadow-dcm-error"),
        DATABASE_OPERATION_ERROR("shadow-database-operation-error"),
        DATABASE_CLOSE_ERROR("shadow-database-close-error");

        String code;
        LogEvents(String code) {
            this.code = code;
        }

        public String code() {
            return code;
        }
    }

    public static final String SERVICE_NAME = "aws.greengrass.ShadowManager";
    public static final List<String> SHADOW_AUTHORIZATION_OPCODES = Arrays.asList("GetThingShadow",
             "UpdateThingShadow", "DeleteThingShadow", "*");
    // Should make this injected?
    private static final ObjectMapper OBJECT_MAPPER =
            new ObjectMapper().configure(MapperFeature.ACCEPT_CASE_INSENSITIVE_PROPERTIES, true);
    private final ShadowManagerDAO dao;
    private final ShadowManagerDatabase database;
    private final AuthorizationHandler authorizationHandler;
    private final Kernel kernel;
    private ConcurrentHashMap<String, String> connectedDevices;

    @Inject
    private GreengrassCoreIPCService greengrassCoreIPCService;

    /**
     * Ctr for ShadowManager.
     * @param topics topics passed by the kernel
     * @param dao Local shadow repository for managing documents
     * @param database Local shadow database management
     * @param authorizationHandler The authorization handler
     * @param kernel greengrass kernel
     */
    @Inject
    public ShadowManager(
            Topics topics,
            ShadowManagerDAOImpl dao,
            ShadowManagerDatabase database,
            AuthorizationHandler authorizationHandler,
            Kernel kernel) {
        super(topics);
        this.database = database;
        this.dao = dao;
        this.authorizationHandler = authorizationHandler;
        this.kernel = kernel;
        connectedDevices = new ConcurrentHashMap<>();
    }

    private void registerHandlers() {
        try {
            authorizationHandler.registerComponent(this.getName(), new HashSet<>(SHADOW_AUTHORIZATION_OPCODES));
        } catch (AuthorizationException e) {
            logger.atError()
                    .setEventType(LogEvents.AUTHORIZATION_ERROR.code)
                    .setCause(e)
                    .log("Failed to initialize the ShadowManager service with the Authorization module.");
        }

        /*greengrassCoreIPCService.setGetThingShadowHandler(
                context -> shadowManagerIPCAgent.getThingShadowOperationHandler(context));
        logger.atInfo().setEventType(LogEvents.IPC_REGISTRATION.code())
                                        .addKeyValue("handler", "getThingShadowOperationHandler").log();*/
    }

    private void setupDCMService() throws ServiceLoadException {
        kernel.locate(DCMService.DCM_SERVICE_NAME).getConfig()
                .lookup(RUNTIME_STORE_NAMESPACE_TOPIC, DCMService.CERTIFICATES_KEY, DCMService.DEVICES_TOPIC)
                .subscribe((why, newv) -> {
                    try {
                        Map<String, String> clientCerts = (Map<String, String>) newv.toPOJO();
                        if (clientCerts.isEmpty()) {
                            logger.atDebug()
                                    .setEventType(LogEvents.DCM_ERROR.code())
                                    .log("Client Certificates map is null or empty");
                            return;
                        }
                        updateConnectedDevices(clientCerts);
                    } catch (ClassCastException e) {
                        logger.atError()
                                .setEventType(LogEvents.DCM_ERROR.code())
                                .addKeyValue("clientCerts", Coerce.toString(newv))
                                .log();
                        serviceErrored(String.format("Invalid Client Certificates map. %s", e.getMessage()));
                    }
                });
    }

    private void updateConnectedDevices(Map<String, String> clientCerts) {
        connectedDevices.clear(); // Clear out existing devices?
        connectedDevices.putAll(clientCerts);
    }

    @Override
    protected void install() throws InterruptedException {
        try {
            database.install();
        } catch (SQLException | FlywayException e) {
            serviceErrored(e);
        }
    }

    @Override
    public void startup() {
        try {
            // Register IPC and Authorization
            registerHandlers();
            // Listen for connected devices
            //setupDCMService();

            reportState(State.RUNNING);
        } catch (Exception e) { //ServiceLoadException e) {
            serviceErrored(e);
        }
    }

    @Override
    protected void shutdown() throws InterruptedException {
        super.shutdown();
        try {
            database.close();
        } catch (IOException e) {
            logger.atError()
                    .setEventType(LogEvents.DATABASE_CLOSE_ERROR.code())
                    .setCause(e)
                    .log("Failed to close out shadow database");
        }
    }

    /**
     * Handles local shadow service IPC calls.
     * /@param request GetThingShadow request received from a client.
     * /@param serviceName component name of the request.
     * /@return GetThingShadowResponse
     */
    private void handleGetThingShadow() {
        /*
        try {
            doAuthorization(opCode.toString(), context.getServiceName(), getThingShadowRequest.getThingName());
            GetThingShadowResponse response = new GetThingShadowResponse();

            thingName = getThingShadowRequest.getThingName();
            logger.atInfo().log("Getting Thing Shadow for Thing Name: {}", thingName);
            result = dao.getShadowThing(thingName);
            response = new GetThingShadowResult();
            if (result.isPresent()) {
                ((GetThingShadowResult) response).setPayload(result.get());
                response.setStatus(ShadowResponseStatus.Success);
            } else {
                response.setStatus(ShadowResponseStatus.ResourceNotFoundError);
                response.setErrorMessage("Shadow for " + thingName + " could not be found.");
            }
        } catch (IOException e) {
            response.setStatus(ShadowResponseStatus.InternalError);
            response.setErrorMessage(e.getMessage());
            logger.atError()
                    .setEventType(LogEvents.IPC_ERROR.code()).setCause(e)
                    .log("Failed to parse IPC message from {}", context.getClientId());
        }  catch (AuthorizationException e) {
            response.setStatus(ShadowResponseStatus.Unauthorized);
            response.setErrorMessage(e.getMessage());
            logger.atError()
                    .setEventType(LogEvents.AUTHORIZATION_ERROR.code()).setCause(e)
                    .log("An authorization error occurred");
        }  catch (ShadowManagerDataException e) {
            response.setStatus(ShadowResponseStatus.InternalError);
            response.setErrorMessage(e.getMessage());
            logger.atError()
                    .setEventType(LogEvents.DATABASE_OPERATION_ERROR.code()).setCause(e)
                    .log("A database error occurred");
        }
        return response;

         */
    }

    private void handleDeleteThingShadow() {
    }

    private void handleUpdateThingShadow() {
    }

    private void doAuthorization(String opCode, String serviceName, String thingName) throws AuthorizationException {
        authorizationHandler.isAuthorized(
                this.getName(),
                Permission.builder()
                        .principal(serviceName)
                        .operation(opCode.toLowerCase())
                        .resource(thingName)
                        .build());
    }
}
