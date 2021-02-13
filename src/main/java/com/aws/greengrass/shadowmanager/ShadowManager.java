/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.shadowmanager;

import com.aws.greengrass.authorization.AuthorizationHandler;
import com.aws.greengrass.authorization.Permission;
import com.aws.greengrass.authorization.exceptions.AuthorizationException;
import com.aws.greengrass.config.Topics;
import com.aws.greengrass.dependency.ImplementsService;
import com.aws.greengrass.dependency.State;
import com.aws.greengrass.lifecyclemanager.Kernel;
import com.aws.greengrass.lifecyclemanager.PluginService;
import com.aws.greengrass.shadowmanager.exception.ShadowManagerDataException;
import com.aws.greengrass.shadowmanager.exception.ShadowManagerException;
import com.aws.greengrass.util.Utils;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.flywaydb.core.api.FlywayException;
import software.amazon.awssdk.aws.greengrass.GreengrassCoreIPCService;
import software.amazon.awssdk.aws.greengrass.model.DeleteThingShadowRequest;
import software.amazon.awssdk.aws.greengrass.model.DeleteThingShadowResponse;
import software.amazon.awssdk.aws.greengrass.model.GetThingShadowRequest;
import software.amazon.awssdk.aws.greengrass.model.GetThingShadowResponse;
import software.amazon.awssdk.aws.greengrass.model.InvalidArgumentsError;
import software.amazon.awssdk.aws.greengrass.model.ResourceNotFoundError;
import software.amazon.awssdk.aws.greengrass.model.ServiceError;
import software.amazon.awssdk.aws.greengrass.model.UnauthorizedError;
import software.amazon.awssdk.aws.greengrass.model.UpdateThingShadowRequest;
import software.amazon.awssdk.aws.greengrass.model.UpdateThingShadowResponse;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import javax.inject.Inject;

import static software.amazon.awssdk.aws.greengrass.GreengrassCoreIPCService.DELETE_THING_SHADOW;
import static software.amazon.awssdk.aws.greengrass.GreengrassCoreIPCService.GET_THING_SHADOW;
import static software.amazon.awssdk.aws.greengrass.GreengrassCoreIPCService.UPDATE_THING_SHADOW;

@ImplementsService(name = ShadowManager.SERVICE_NAME)
public class ShadowManager extends PluginService {
    enum LogEvents {
        AUTHORIZATION_ERROR("shadow-authorization-error"),
        IPC_REGISTRATION("shadow-ipc-registration"),
        IPC_ERROR("shadow-ipc-error"),
        DCM_ERROR("shadow-dcm-error"),
        DATABASE_OPERATION_ERROR("shadow-database-operation-error"),
        DATABASE_CLOSE_ERROR("shadow-database-close-error"),
        INVALID_THING_NAME("shadow-invalid-thing-name-error"),
        DOCUMENT_NOT_FOUND("shadow-document-not-found"),
        MISSING_THING_ERROR("shadow-missing-thing-name-error");

        String code;
        LogEvents(String code) {
            this.code = code;
        }

        public String code() {
            return code;
        }
    }

    public static final String SERVICE_NAME = "aws.greengrass.ShadowManager";
    public static final List<String> SHADOW_AUTHORIZATION_OPCODES = Arrays.asList(GET_THING_SHADOW,
            UPDATE_THING_SHADOW, DELETE_THING_SHADOW);
    private static final ObjectMapper OBJECT_MAPPER =
            new ObjectMapper().configure(MapperFeature.ACCEPT_CASE_INSENSITIVE_PROPERTIES, true);

    private final ShadowManagerDAO dao;
    private final ShadowManagerDatabase database;
    private final AuthorizationHandler authorizationHandler;
    private final Kernel kernel;

    @Inject
    ShadowManagerIPCAgent shadowManagerIPCAgent;

    @Inject
    private GreengrassCoreIPCService greengrassCoreIPCService;

    /**
     * Ctr for ShadowManager.
     *
     * @param topics               topics passed by the Nucleus
     * @param database             Local shadow database management
     * @param dao                  Local shadow database management
     * @param authorizationHandler The authorization handler
     * @param kernel               greengrass kernel
     */
    @Inject
    public ShadowManager(
            Topics topics,
            ShadowManagerDatabase database,
            ShadowManagerDAOImpl dao,
            AuthorizationHandler authorizationHandler,
            Kernel kernel) {
        super(topics);
        this.database = database;
        this.authorizationHandler = authorizationHandler;
        this.kernel = kernel;
        this.dao = dao;
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

        greengrassCoreIPCService.setGetThingShadowHandler(
                context -> shadowManagerIPCAgent.getGetThingShadowOperationHandler(context));
        greengrassCoreIPCService.setDeleteThingShadowHandler(
                context -> shadowManagerIPCAgent.getDeleteThingShadowOperationHandler(context));
        greengrassCoreIPCService.setUpdateThingShadowHandler(
                context -> shadowManagerIPCAgent.getUpdateThingShadowOperationHandler(context));
    }

    /**
     * Handles GetThingShadow API call from IPC.
     *
     * @param request     GetThingShadow request from IPC API
     * @param serviceName component name of the request
     * @return GetThingShadow response
     * @throws ResourceNotFoundError if requested document is not found locally
     * @throws UnauthorizedError if GetThingShadow call not authorized
     * @throws InvalidArgumentsError if validation error occurred with supplied request fields
     * @throws ServiceError if database error occurs
     */
    public GetThingShadowResponse handleGetThingShadowIPCRequest(GetThingShadowRequest request, String serviceName) {
        try {
            String thingName = request.getThingName();
            String shadowName = request.getShadowName();
            validateThingNameAndDoAuthorization(GET_THING_SHADOW, serviceName, thingName, shadowName);

            Optional<byte[]> result = dao.getShadowThing(thingName, shadowName);
            Map<String, Object> payload = result.map(ShadowManager::convertBytesToPayload)
                    .orElseThrow(() -> {
                        ResourceNotFoundError rnf = new ResourceNotFoundError(
                                String.format("no shadow found for thingName: %s, shadowName: %s",
                                        thingName, shadowName));
                        rnf.setResourceType("shadow");
                        logger.atWarn()
                                .setEventType(LogEvents.DOCUMENT_NOT_FOUND.code()).setCause(rnf)
                                .log(String.format("Could not process GetThingShadow Request: %s", rnf.getMessage()));
                        return rnf;
                    });

            GetThingShadowResponse response = new GetThingShadowResponse();
            response.setPayload(payload);
            return response;

        } catch (AuthorizationException e) {
            logger.atError()
                    .setEventType(LogEvents.AUTHORIZATION_ERROR.code())
                    .log("Could not process GetThingShadow Request: %s", e.getMessage());
            throw new UnauthorizedError(e.getMessage());
        } catch (ShadowManagerException e) {
            logger.atWarn()
                    .setEventType(LogEvents.INVALID_THING_NAME.code()).setCause(e)
                    .log("Could not process GetThingShadow Request: %s", e.getMessage());
            throw new InvalidArgumentsError(e.getMessage());
        } catch (ShadowManagerDataException e) {
            logger.atError()
                    .setEventType(LogEvents.DATABASE_OPERATION_ERROR.code()).setCause(e)
                    .log("Could not process GetThingShadow Request: %s", e.getMessage());
            throw new ServiceError(e.getMessage());
        }
    }

    /**
     * Handles UpdateThingShadow API calls from IPC.
     *
     * @param request     UpdateThingShadow request from IPC API
     * @param serviceName component name of the request
     * @return UpdateThingShadow
     */
    public UpdateThingShadowResponse handleUpdateThingShadowIPCRequest(UpdateThingShadowRequest request,
                                                                       String serviceName) {
        return new UpdateThingShadowResponse();
    }


    /**
     * Handles DeleteThingShadow API calls from IPC.
     *
     * @param request     DeleteThingShadow request from IPC API
     * @param serviceName component name of the request
     * @return DeleteThingShadow
     */
    public DeleteThingShadowResponse handleDeleteThingShadowIPCRequest(DeleteThingShadowRequest request,
                                                                       String serviceName) {
        return new DeleteThingShadowResponse();
    }

    @Override
    protected void install() {
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

            reportState(State.RUNNING);
        } catch (Exception e) {
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

    private void validateThingNameAndDoAuthorization(String opCode, String serviceName, String thingName,
                                                     String shadowName)
            throws AuthorizationException, ShadowManagerException {

        if (Utils.isEmpty(thingName)) {
            throw new ShadowManagerException("thingName absent in request");
        }

        String combinedThingName = thingName + shadowName;
        authorizationHandler.isAuthorized(
                this.getName(),
                Permission.builder()
                        .principal(serviceName)
                        .operation(opCode.toLowerCase())
                        .resource(combinedThingName)
                        .build());
    }

    /**
     * Helper Function to convert bytes to payload format.
     *
     * @param bytes     Bytes to be converted
     * @return payload
     * @throws ShadowManagerException if an error occurs in converting to payload format
     */
    public static Map<String, Object> convertBytesToPayload(byte[] bytes) throws ShadowManagerException {
        try {
            return OBJECT_MAPPER.readValue(bytes, new TypeReference<Map<String, Object>>() {
            });
        } catch (IOException e) {
            throw new ShadowManagerException(e);
        }
    }
}
