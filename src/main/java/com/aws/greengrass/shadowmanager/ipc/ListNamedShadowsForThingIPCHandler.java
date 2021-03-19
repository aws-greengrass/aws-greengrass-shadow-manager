/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.shadowmanager.ipc;

import com.aws.greengrass.authorization.AuthorizationHandler;
import com.aws.greengrass.authorization.exceptions.AuthorizationException;
import com.aws.greengrass.logging.api.Logger;
import com.aws.greengrass.logging.impl.LogManager;
import com.aws.greengrass.shadowmanager.ShadowManagerDAO;
import com.aws.greengrass.shadowmanager.exception.ShadowManagerDataException;
import software.amazon.awssdk.aws.greengrass.GeneratedAbstractListNamedShadowsForThingOperationHandler;
import software.amazon.awssdk.aws.greengrass.model.InvalidArgumentsError;
import software.amazon.awssdk.aws.greengrass.model.ListNamedShadowsForThingRequest;
import software.amazon.awssdk.aws.greengrass.model.ListNamedShadowsForThingResponse;
import software.amazon.awssdk.aws.greengrass.model.ResourceNotFoundError;
import software.amazon.awssdk.aws.greengrass.model.ServiceError;
import software.amazon.awssdk.aws.greengrass.model.UnauthorizedError;
import software.amazon.awssdk.eventstreamrpc.OperationContinuationHandlerContext;
import software.amazon.awssdk.eventstreamrpc.model.EventStreamJsonMessage;

import java.security.InvalidAlgorithmParameterException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.KeySpec;
import java.time.Instant;
import java.util.Base64;
import java.util.List;
import java.util.Optional;
import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.SecretKey;
import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.PBEKeySpec;
import javax.crypto.spec.SecretKeySpec;

import static com.aws.greengrass.ipc.common.ExceptionUtil.translateExceptions;
import static software.amazon.awssdk.aws.greengrass.GreengrassCoreIPCService.LIST_NAMED_SHADOWS_FOR_THING;

/**
 * Handler class with business logic for all ListNamedShadowsForThing requests over IPC.
 */
public class ListNamedShadowsForThingIPCHandler extends GeneratedAbstractListNamedShadowsForThingOperationHandler {
    private static final Logger logger = LogManager.getLogger(ListNamedShadowsForThingIPCHandler.class);
    private static final int DEFAULT_PAGE_SIZE = 25;
    private static final int DEFAULT_OFFSET = 0;

    private static final String cipherTransformation = "AES/CBC/PKCS5Padding";
    private static final String encryptionAlgorithm = "AES";
    private static final String secretKeyAlgorithm = "PBKDF2WithHmacSHA256";
    private static final int PBEKeyIterationCount = 65536;
    private static final int PBEKeyLength = 256;

    private final String serviceName;

    private final ShadowManagerDAO dao;
    private final AuthorizationHandler authorizationHandler;

    /**
     * IPC Handler class for responding to ListNamedShadowsForThing requests.
     *
     * @param context              topics passed by the Nucleus
     * @param dao                  Local shadow database management
     * @param authorizationHandler The authorization handler
     */
    public ListNamedShadowsForThingIPCHandler(
            OperationContinuationHandlerContext context,
            ShadowManagerDAO dao,
            AuthorizationHandler authorizationHandler) {
        super(context);
        this.authorizationHandler = authorizationHandler;
        this.dao = dao;
        this.serviceName = context.getAuthenticationData().getIdentityLabel();
    }

    @Override
    protected void onStreamClosed() {

    }

    /**
     * Handles ListNamedShadowsForThing Requests from IPC.
     *
     * @param request ListNamedShadowsForThing request from IPC API
     * @return ListNamedShadowsForThing response
     * @throws ResourceNotFoundError if requested document is not found locally
     * @throws UnauthorizedError     if ListNamedShadowsForThing call not authorized
     * @throws InvalidArgumentsError if validation error occurred with supplied request fields
     * @throws ServiceError          if database error occurs
     */
    @Override
    public ListNamedShadowsForThingResponse handleRequest(ListNamedShadowsForThingRequest request) {
        return translateExceptions(() -> {
            String thingName = request.getThingName();

            try {
                logger.atTrace("ipc-list-named-shadow-for-thing-request").log();

                IPCUtil.validateThingNameAndDoAuthorization(authorizationHandler, LIST_NAMED_SHADOWS_FOR_THING,
                        serviceName, thingName);

                int pageSize = Optional.ofNullable(request.getPageSize())
                        .orElse(DEFAULT_PAGE_SIZE);

                if (pageSize < 1 || pageSize > 100) {
                    throw new InvalidArgumentsError("pageSize argument must be between 1 and 100");
                }

                int offset = Optional.ofNullable(request.getNextToken())
                        .filter(s -> !s.isEmpty())
                        .map(token -> decodeOffsetFromToken(token, serviceName, thingName))
                        .orElse(DEFAULT_OFFSET);

                List<String> results = dao.listNamedShadowsForThing(thingName, offset, pageSize)
                        .orElseThrow(() -> {
                            ServiceError error = new ServiceError("Unexpected error occurred in trying to "
                                    + "list named shadows for thing.");
                            logger.atError()
                                    .setEventType(IPCUtil.LogEvents.LIST_NAMED_SHADOWS.code())
                                    .kv(IPCUtil.LOG_THING_NAME_KEY, thingName)
                                    .setCause(error)
                                    .log();
                            return error;
                        });

                ListNamedShadowsForThingResponse response = new ListNamedShadowsForThingResponse();
                response.setResults(results);
                response.setTimestamp(Instant.now());

                if (results.size() == pageSize) {
                    String nextToken = generateToken(offset + pageSize, serviceName, thingName);
                    response.setNextToken(nextToken);
                } else if (results.size() > pageSize) {
                    ServiceError error = new ServiceError("Could not process ListNamedShadowsForThing "
                            + "Request due to internal service error");
                    logger.atError()
                            .setEventType(IPCUtil.LogEvents.LIST_NAMED_SHADOWS.code())
                            .kv(IPCUtil.LOG_THING_NAME_KEY, thingName)
                            .setCause(error)
                            .log();
                    throw error;
                }
                return response;

            } catch (AuthorizationException e) {
                logger.atWarn()
                        .setEventType(IPCUtil.LogEvents.LIST_NAMED_SHADOWS.code())
                        .setCause(e)
                        .kv(IPCUtil.LOG_THING_NAME_KEY, thingName)
                        .log("Not authorized to list named shadows for thing");
                throw new UnauthorizedError(e.getMessage());
            } catch (InvalidArgumentsError e) {
                logger.atWarn()
                        .setEventType(IPCUtil.LogEvents.LIST_NAMED_SHADOWS.code())
                        .setCause(e)
                        .kv(IPCUtil.LOG_THING_NAME_KEY, thingName)
                        .log();
                throw e;
            } catch (ShadowManagerDataException e) {
                logger.atError()
                    .setEventType(IPCUtil.LogEvents.LIST_NAMED_SHADOWS.code())
                    .setCause(e)
                    .kv(IPCUtil.LOG_THING_NAME_KEY, thingName)
                    .log("Could not process ListNamedShadowsForThing Request due to internal service error");
                throw new ServiceError(e.getMessage());
            }
        });
    }

    @Override
    public void handleStreamEvent(EventStreamJsonMessage streamRequestEvent) {

    }

    /**
     * Calculates the offset by decrypting the token with a secret formed from clientId and thingName.
     *
     * @param nextToken Generated token encrypted by ShadowManager
     * @param clientId  client which requested the listNameShadowsForThing Request
     * @param thingName thingName for the listNameShadowsForThing Request
     * @return The offset used in the list named shadows query
     */
    private static int decodeOffsetFromToken(String nextToken, String clientId, String thingName) {
        try {
            String secret = clientId + thingName;
            Cipher cipher = createCipher(secret, thingName, Cipher.DECRYPT_MODE);
            String offsetString = new String(cipher.doFinal(Base64.getDecoder().decode(nextToken)));
            return Integer.parseInt(offsetString);

        } catch (BadPaddingException | IllegalBlockSizeException e) {
            logger.atWarn()
                    .kv(IPCUtil.LOG_NEXT_TOKEN_KEY, nextToken)
                    .log("Invalid nextToken passed in");
            throw new InvalidArgumentsError("Invalid nextToken passed in");
        }
    }

    /**
     * Generates a token by encrypting the offset with a secret key formed from clientId and thingName.
     *
     * @param offset    current place of the ListNamedShadowsForThing query to be encrypted
     * @param clientId  client which requested the ListNamedShadowsForThing Request
     * @param thingName thingName for the ListNamedShadowsForThing Request
     * @return A generated string token to be returned as the nextToken in the ListNamedShadowsForThing response
     */
    private static String generateToken(int offset, String clientId, String thingName) {
        try {
            String secret = clientId + thingName;
            Cipher cipher = createCipher(secret, thingName, Cipher.ENCRYPT_MODE);
            return Base64.getEncoder()
                    .encodeToString(cipher.doFinal(String.valueOf(offset).getBytes()));

        } catch (IllegalBlockSizeException | BadPaddingException e) {
            ServiceError serviceError = new ServiceError("Failed to generate token");
            logger.atError()
                    .setEventType(IPCUtil.LogEvents.LIST_NAMED_SHADOWS.code())
                    .kv(IPCUtil.LOG_THING_NAME_KEY, thingName)
                    .setCause(e)
                    .log("Failed to generate token");
            throw serviceError;
        }
    }

    /**
     * Creates a Cipher used to encrypt/decrypt the offset using the clientId and thingName as the secretKey.
     * We use the thingName as the salt value in generating a derived secret key.
     *
     * @param secret               secret key used in the encryption process
     * @param salt                 salt value used to randomize the encrypted password
     * @param cipherEncryptionMode mode to determine whether Cipher object will encrypt or decrypt
     * @return A Cipher object used to encrypt/decrypt a token
     */
    private static Cipher createCipher(String secret, String salt, int cipherEncryptionMode) {
        try {
            KeySpec keySpec = new PBEKeySpec(secret.toCharArray(), salt.getBytes(), PBEKeyIterationCount, PBEKeyLength);
            SecretKeyFactory secretKeyFactory = SecretKeyFactory.getInstance(secretKeyAlgorithm);
            SecretKey secretKey = secretKeyFactory.generateSecret(keySpec);
            SecretKeySpec secretKeySpec = new SecretKeySpec(secretKey.getEncoded(), encryptionAlgorithm);

            Cipher cipher = Cipher.getInstance(cipherTransformation);
            byte[] iv = new byte[cipher.getBlockSize()];
            IvParameterSpec ivParams = new IvParameterSpec(iv);
            cipher.init(cipherEncryptionMode, secretKeySpec, ivParams);

            return cipher;

        } catch (InvalidAlgorithmParameterException | InvalidKeyException | InvalidKeySpecException
                    | NoSuchAlgorithmException | NoSuchPaddingException e) {
            ServiceError serviceError = new ServiceError("Failed to generate cipher");
            logger.atError()
                    .setEventType(IPCUtil.LogEvents.LIST_NAMED_SHADOWS.code())
                    .kv(IPCUtil.LOG_THING_NAME_KEY, salt)
                    .setCause(e)
                    .log("Failed to generate cipher");
            throw serviceError;
        }
    }

}
