/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.shadowmanager.ipc;

import com.aws.greengrass.authorization.exceptions.AuthorizationException;
import com.aws.greengrass.logging.api.Logger;
import com.aws.greengrass.logging.impl.LogManager;
import com.aws.greengrass.shadowmanager.AuthorizationHandlerWrapper;
import com.aws.greengrass.shadowmanager.ShadowManagerDAO;
import com.aws.greengrass.shadowmanager.exception.InvalidRequestParametersException;
import com.aws.greengrass.shadowmanager.exception.ShadowManagerDataException;
import com.aws.greengrass.shadowmanager.model.LogEvents;
import com.aws.greengrass.shadowmanager.model.ShadowRequest;
import software.amazon.awssdk.aws.greengrass.GeneratedAbstractListNamedShadowsForThingOperationHandler;
import software.amazon.awssdk.aws.greengrass.model.InvalidArgumentsError;
import software.amazon.awssdk.aws.greengrass.model.ListNamedShadowsForThingRequest;
import software.amazon.awssdk.aws.greengrass.model.ListNamedShadowsForThingResponse;
import software.amazon.awssdk.aws.greengrass.model.ResourceNotFoundError;
import software.amazon.awssdk.aws.greengrass.model.ServiceError;
import software.amazon.awssdk.aws.greengrass.model.UnauthorizedError;
import software.amazon.awssdk.eventstreamrpc.OperationContinuationHandlerContext;
import software.amazon.awssdk.eventstreamrpc.model.EventStreamJsonMessage;

import java.security.GeneralSecurityException;
import java.security.spec.KeySpec;
import java.time.Instant;
import java.util.Base64;
import java.util.List;
import java.util.Optional;
import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.SecretKey;
import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.PBEKeySpec;
import javax.crypto.spec.SecretKeySpec;

import static com.aws.greengrass.ipc.common.ExceptionUtil.translateExceptions;
import static com.aws.greengrass.shadowmanager.model.Constants.CIPHER_TRANSFORMATION;
import static com.aws.greengrass.shadowmanager.model.Constants.DEFAULT_OFFSET;
import static com.aws.greengrass.shadowmanager.model.Constants.DEFAULT_PAGE_SIZE;
import static com.aws.greengrass.shadowmanager.model.Constants.ENCRYPTION_ALGORITHM;
import static com.aws.greengrass.shadowmanager.model.Constants.LOG_NEXT_TOKEN_KEY;
import static com.aws.greengrass.shadowmanager.model.Constants.LOG_PAGE_SIZE_KEY;
import static com.aws.greengrass.shadowmanager.model.Constants.LOG_THING_NAME_KEY;
import static com.aws.greengrass.shadowmanager.model.Constants.MAX_PAGE_SIZE;
import static com.aws.greengrass.shadowmanager.model.Constants.MIN_PAGE_SIZE;
import static com.aws.greengrass.shadowmanager.model.Constants.PBE_KEY_ITERATION_COUNT;
import static com.aws.greengrass.shadowmanager.model.Constants.PBE_KEY_LENGTH;
import static com.aws.greengrass.shadowmanager.model.Constants.SECRET_KEY_ALGORITHM;
import static software.amazon.awssdk.aws.greengrass.GreengrassCoreIPCService.LIST_NAMED_SHADOWS_FOR_THING;

/**
 * Handler class with business logic for all ListNamedShadowsForThing requests over IPC.
 */
public class ListNamedShadowsForThingIPCHandler extends GeneratedAbstractListNamedShadowsForThingOperationHandler {
    private static final Logger logger = LogManager.getLogger(ListNamedShadowsForThingIPCHandler.class);

    private final String serviceName;
    private final ShadowManagerDAO dao;
    private final AuthorizationHandlerWrapper authorizationHandlerWrapper;

    /**
     * IPC Handler class for responding to ListNamedShadowsForThing requests.
     *
     * @param context                     topics passed by the Nucleus
     * @param dao                         Local shadow database management
     * @param authorizationHandlerWrapper The authorization handler wrapper
     */
    public ListNamedShadowsForThingIPCHandler(
            OperationContinuationHandlerContext context,
            ShadowManagerDAO dao,
            AuthorizationHandlerWrapper authorizationHandlerWrapper) {
        super(context);
        this.authorizationHandlerWrapper = authorizationHandlerWrapper;
        this.dao = dao;
        this.serviceName = context.getAuthenticationData().getIdentityLabel();
    }

    @Override
    protected void onStreamClosed() {
        //NA
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
    @SuppressWarnings("PMD.PreserveStackTrace")
    public ListNamedShadowsForThingResponse handleRequest(ListNamedShadowsForThingRequest request) {
        return translateExceptions(() -> {
            String thingName = request.getThingName();

            try {
                logger.atTrace("ipc-list-named-shadow-for-thing-request").log();

                ShadowRequest shadowRequest = new ShadowRequest(thingName);
                Validator.validateShadowRequest(shadowRequest);
                authorizationHandlerWrapper.doAuthorization(LIST_NAMED_SHADOWS_FOR_THING, serviceName, shadowRequest);

                int pageSize = Optional.ofNullable(request.getPageSize())
                        .orElse(DEFAULT_PAGE_SIZE);

                if (pageSize < MIN_PAGE_SIZE || pageSize > MAX_PAGE_SIZE) {
                    throw new IllegalArgumentException("pageSize argument must be between 1 and 100");
                }

                int offset = DEFAULT_OFFSET;
                Optional<String> userToken = Optional.ofNullable(request.getNextToken())
                        .filter(s -> !s.isEmpty());

                if (userToken.isPresent()) {
                    offset = decodeOffsetFromToken(userToken.get(), serviceName, thingName);
                }

                List<String> results = dao.listNamedShadowsForThing(thingName, offset, pageSize);

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
                            .setEventType(LogEvents.LIST_NAMED_SHADOWS.code())
                            .kv(LOG_THING_NAME_KEY, thingName)
                            .setCause(error)
                            .log();
                    throw error;
                }
                return response;

            } catch (AuthorizationException e) {
                logger.atWarn()
                        .setEventType(LogEvents.LIST_NAMED_SHADOWS.code())
                        .setCause(e)
                        .kv(LOG_THING_NAME_KEY, thingName)
                        .log("Not authorized to list named shadows for thing");
                throw new UnauthorizedError(e.getMessage());
            } catch (InvalidRequestParametersException | IllegalArgumentException e) {
                logger.atWarn()
                        .setEventType(LogEvents.LIST_NAMED_SHADOWS.code())
                        .setCause(e)
                        .kv(LOG_THING_NAME_KEY, thingName)
                        .kv(LOG_PAGE_SIZE_KEY, request.getPageSize())
                        .kv(LOG_NEXT_TOKEN_KEY, request.getNextToken())
                        .log();
                throw new InvalidArgumentsError(e.getMessage());
            } catch (ShadowManagerDataException | GeneralSecurityException e) {
                logger.atError()
                    .setEventType(LogEvents.LIST_NAMED_SHADOWS.code())
                    .setCause(e)
                    .kv(LOG_THING_NAME_KEY, thingName)
                    .log("Could not process ListNamedShadowsForThing Request due to internal service error");
                throw new ServiceError(e.getMessage());
            }
        });
    }

    @Override
    public void handleStreamEvent(EventStreamJsonMessage streamRequestEvent) {
        //NA
    }

    /**
     * Calculates the offset by decrypting the token with a secret formed from clientId and thingName.
     *
     * @param nextToken Generated token encrypted by ShadowManager
     * @param clientId  client which requested the listNameShadowsForThing Request
     * @param thingName thingName for the listNameShadowsForThing Request
     * @return The offset used in the list named shadows query
     */
    @SuppressWarnings("PMD.AvoidUncheckedExceptionsInSignatures")
    private static int decodeOffsetFromToken(String nextToken, String clientId, String thingName)
            throws GeneralSecurityException, IllegalArgumentException {
        try {
            String secret = clientId + thingName;
            Cipher cipher = createCipher(secret, thingName, Cipher.DECRYPT_MODE);
            String offsetString = new String(cipher.doFinal(Base64.getDecoder().decode(nextToken)));
            return Integer.parseInt(offsetString);
        } catch (BadPaddingException | IllegalBlockSizeException e) {
            throw new IllegalArgumentException("Invalid nextToken argument", e);
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
    private static String generateToken(int offset, String clientId, String thingName) throws GeneralSecurityException {
        String secret = clientId + thingName;
        Cipher cipher = createCipher(secret, thingName, Cipher.ENCRYPT_MODE);
        return Base64.getEncoder()
                .encodeToString(cipher.doFinal(String.valueOf(offset).getBytes()));
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
    private static Cipher createCipher(String secret, String salt, int cipherEncryptionMode)
            throws GeneralSecurityException {
        KeySpec keySpec = new PBEKeySpec(secret.toCharArray(), salt.getBytes(), PBE_KEY_ITERATION_COUNT,
                PBE_KEY_LENGTH);
        SecretKeyFactory secretKeyFactory = SecretKeyFactory.getInstance(SECRET_KEY_ALGORITHM);
        SecretKey secretKey = secretKeyFactory.generateSecret(keySpec);
        SecretKeySpec secretKeySpec = new SecretKeySpec(secretKey.getEncoded(), ENCRYPTION_ALGORITHM);

        Cipher cipher = Cipher.getInstance(CIPHER_TRANSFORMATION);
        byte[] iv = new byte[cipher.getBlockSize()];
        IvParameterSpec ivParams = new IvParameterSpec(iv);
        cipher.init(cipherEncryptionMode, secretKeySpec, ivParams);

        return cipher;
    }

}
