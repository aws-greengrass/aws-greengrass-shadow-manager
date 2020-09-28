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
import com.aws.greengrass.ipc.ConnectionContext;
import com.aws.greengrass.ipc.IPCRouter;
import com.aws.greengrass.ipc.common.BuiltInServiceDestinationCode;
import com.aws.greengrass.ipc.common.FrameReader;
import com.aws.greengrass.ipc.exceptions.IPCException;
import com.aws.greengrass.ipc.services.common.ApplicationMessage;
import com.aws.greengrass.ipc.services.shadow.ShadowClientOpCodes;
import com.aws.greengrass.ipc.services.shadow.models.DeleteThingShadowRequest;
import com.aws.greengrass.ipc.services.shadow.models.DeleteThingShadowResult;
import com.aws.greengrass.ipc.services.shadow.models.GetThingShadowRequest;
import com.aws.greengrass.ipc.services.shadow.models.GetThingShadowResult;
import com.aws.greengrass.ipc.services.shadow.models.ShadowGenericResponse;
import com.aws.greengrass.ipc.services.shadow.models.ShadowResponseStatus;
import com.aws.greengrass.ipc.services.shadow.models.UpdateThingShadowRequest;
import com.aws.greengrass.ipc.services.shadow.models.UpdateThingShadowResult;
import com.aws.greengrass.lifecyclemanager.Kernel;
import com.aws.greengrass.lifecyclemanager.PluginService;
import com.aws.greengrass.lifecyclemanager.exceptions.ServiceLoadException;
import com.aws.greengrass.shadowmanager.exception.ShadowManagerDataException;
import com.aws.greengrass.util.Coerce;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.cbor.databind.CBORMapper;
import org.flywaydb.core.api.FlywayException;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;
import javax.inject.Inject;

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
    private static final ObjectMapper CBOR_MAPPER = new CBORMapper();
    private final IPCRouter ipcRouter;
    private final ShadowManagerDAO dao;
    private final ShadowManagerDatabase database;
    private final AuthorizationHandler authorizationHandler;
    private final Kernel kernel;
    private ConcurrentHashMap<String, String> connectedDevices;

    /**
     * Ctr for ShadowManager.
     * @param topics topics passed by the kernel
     * @param ipcRouter IPC router for handling local shadow Request / Reply
     * @param dao Local shadow repository for managing documents
     * @param database Local shadow database management
     * @param authorizationHandler The authorization handler
     * @param kernel greengrass kernel
     */
    @Inject
    public ShadowManager(
            Topics topics,
            IPCRouter ipcRouter,
            ShadowManagerDAOImpl dao,
            ShadowManagerDatabase database,
            AuthorizationHandler authorizationHandler,
            Kernel kernel) {
        super(topics);
        this.ipcRouter = ipcRouter;
        this.database = database;
        this.dao = dao;
        this.authorizationHandler = authorizationHandler;
        this.kernel = kernel;
        connectedDevices = new ConcurrentHashMap<>();
    }

    private void registerHandlers() throws IPCException {
        BuiltInServiceDestinationCode destinationCode = BuiltInServiceDestinationCode.SHADOW;

        try {
            authorizationHandler.registerComponent(this.getName(), new HashSet<>(SHADOW_AUTHORIZATION_OPCODES));
        } catch (AuthorizationException e) {
            logger.atError()
                    .setEventType(LogEvents.AUTHORIZATION_ERROR.code)
                    .setCause(e)
                    .kv(IPCRouter.DESTINATION_STRING, destinationCode.name())
                    .log("Failed to initialize the ShadowManager service with the Authorization module.");
        }

        ipcRouter.registerServiceCallback(destinationCode.getValue(), this::handleMessage);
        logger.atInfo()
                .setEventType(LogEvents.IPC_REGISTRATION.code())
                .addKeyValue("destination", destinationCode.name())
                .log();
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
            setupDCMService();

            reportState(State.RUNNING);
        } catch (IPCException | ServiceLoadException e) {
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
     * @param message API message received from a client.
     * @param context connection context received from a client.
     * @return
     */
    private Future<FrameReader.Message> handleMessage(FrameReader.Message message, ConnectionContext context) {
        CompletableFuture<FrameReader.Message> future = new CompletableFuture<>();
        ShadowGenericResponse response = new ShadowGenericResponse();
        Optional<byte[]> result;
        final String thingName;
        ApplicationMessage applicationMessage = ApplicationMessage.fromBytes(message.getPayload());
        try {
            ShadowClientOpCodes opCode = ShadowClientOpCodes.values()[applicationMessage.getOpCode()];
            logger.atTrace().log("Received message with OpCode: {}", opCode);
            switch (opCode) { //NOPMD
                // Let's break this up into a map->router
                case GET_THING_SHADOW:
                    GetThingShadowRequest getThingShadowRequest = CBOR_MAPPER.readValue(
                            applicationMessage.getPayload(),
                            GetThingShadowRequest.class);
                    doAuthorization(opCode.toString(), context.getServiceName(), getThingShadowRequest.getThingName());
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
                    break;
                case DELETE_THING_SHADOW:
                    DeleteThingShadowRequest deleteThingShadowRequest = CBOR_MAPPER.readValue(
                            applicationMessage.getPayload(),
                            DeleteThingShadowRequest.class);
                    doAuthorization(opCode.toString(), context.getServiceName(),
                            deleteThingShadowRequest.getThingName());
                    thingName = deleteThingShadowRequest.getThingName();
                    logger.atInfo().log("Deleting Thing Shadow for Thing Name: {}", thingName);
                    result = dao.deleteShadowThing(thingName);
                    response = new DeleteThingShadowResult();
                    if (result.isPresent()) {
                        ((DeleteThingShadowResult) response).setPayload(result.get());
                        response.setStatus(ShadowResponseStatus.Success);
                    } else {
                        response.setStatus(ShadowResponseStatus.ResourceNotFoundError);
                        response.setErrorMessage("Shadow for " + thingName + " could not be found.");
                    }
                    break;
                case UPDATE_THING_SHADOW:
                    UpdateThingShadowRequest updateThingShadowRequest = CBOR_MAPPER.readValue(
                            applicationMessage.getPayload(),
                            UpdateThingShadowRequest.class);
                    doAuthorization(opCode.toString(), context.getServiceName(),
                            updateThingShadowRequest.getThingName());
                    thingName = updateThingShadowRequest.getThingName();
                    logger.atInfo().log("Updating Thing Shadow for Thing Name: {}", thingName);
                    result = dao.updateShadowThing(thingName, updateThingShadowRequest.getPayload());
                    response = new UpdateThingShadowResult();
                    if (result.isPresent()) {
                        ((UpdateThingShadowResult) response).setPayload(result.get());
                        response.setStatus(ShadowResponseStatus.Success);
                    } else {
                        response.setStatus(ShadowResponseStatus.ResourceNotFoundError);
                        response.setErrorMessage("Shadow for " + thingName + " could not be found.");
                    }
                    break;
                default:
                    response.setStatus(ShadowResponseStatus.InvalidRequest);
                    response.setErrorMessage("Unknown request type " + opCode);
                    break;
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
        } finally {
            try {
                ApplicationMessage responseMessage = ApplicationMessage.builder()
                        .version(applicationMessage.getVersion())
                        .payload(CBOR_MAPPER.writeValueAsBytes(response))
                        .build();
                future.complete(new FrameReader.Message(responseMessage.toByteArray()));
            } catch (IOException e) {
                logger.atError()
                        .setEventType(LogEvents.IPC_ERROR.code()).setCause(e)
                        .log("Failed to send application message response");
            }
        }
        if (!future.isDone()) {
            future.completeExceptionally(new IPCException("Unable to serialize any response"));
        }
        return future;
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
