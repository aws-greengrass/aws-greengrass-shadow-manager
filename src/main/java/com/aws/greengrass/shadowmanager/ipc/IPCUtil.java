package com.aws.greengrass.shadowmanager.ipc;

import com.aws.greengrass.authorization.AuthorizationHandler;
import com.aws.greengrass.authorization.Permission;
import com.aws.greengrass.authorization.exceptions.AuthorizationException;
import com.aws.greengrass.util.Utils;
import software.amazon.awssdk.aws.greengrass.model.InvalidArgumentsError;

import java.util.StringJoiner;

public final class IPCUtil {

    static final String SHADOW_RESOURCE_TYPE = "shadow";
    static final String SHADOW_RESOURCE_JOINER = "shadow";
    static final String SHADOW_MANAGER_NAME = "aws.greengrass.ShadowManager";

    enum LogEvents {
        AUTHORIZATION_ERROR("shadow-authorization-error"),
        DATABASE_OPERATION_ERROR("shadow-database-operation-error"),
        INVALID_THING_NAME("shadow-invalid-thing-name-error"),
        DOCUMENT_NOT_FOUND("shadow-document-not-found");

        String code;

        LogEvents(String code) {
            this.code = code;
        }

        public String code() {
            return code;
        }
    }

    private IPCUtil() {

    }

    static void validateThingNameAndDoAuthorization(AuthorizationHandler authorizationHandler, String opCode,
                                                    String serviceName, String thingName, String shadowName)
            throws AuthorizationException, InvalidArgumentsError {

        if (Utils.isEmpty(thingName)) {
            throw new InvalidArgumentsError("ThingName absent in request");
        }

        StringJoiner shadowResource = new StringJoiner("/");
        shadowResource.add(thingName);
        shadowResource.add(SHADOW_RESOURCE_JOINER);

        if (Utils.isNotEmpty(shadowName)) {
            shadowResource.add(shadowName);
        }

        authorizationHandler.isAuthorized(
                SHADOW_MANAGER_NAME,
                Permission.builder()
                        .principal(serviceName)
                        .operation(opCode.toLowerCase())
                        .resource(shadowResource.toString())
                        .build());
    }
}
