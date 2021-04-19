/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.shadowmanager.sync;

import com.aws.greengrass.deployment.DeviceConfiguration;
import com.aws.greengrass.shadowmanager.exception.RetryableException;
import com.aws.greengrass.shadowmanager.exception.SkipSyncRequestException;
import com.aws.greengrass.shadowmanager.model.ShadowDocument;
import com.aws.greengrass.shadowmanager.util.JsonUtil;
import com.aws.greengrass.util.Coerce;
import com.aws.greengrass.util.ProxyUtils;
import com.aws.greengrass.util.Utils;
import software.amazon.awssdk.http.HttpExecuteRequest;
import software.amazon.awssdk.http.HttpExecuteResponse;
import software.amazon.awssdk.http.SdkHttpClient;
import software.amazon.awssdk.http.SdkHttpFullRequest;
import software.amazon.awssdk.http.SdkHttpMethod;
import software.amazon.awssdk.http.apache.ApacheHttpClient;
import software.amazon.awssdk.utils.IoUtils;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URI;

import static com.aws.greengrass.shadowmanager.model.Constants.HEADER_CONTENT_TYPE_KEY;
import static com.aws.greengrass.shadowmanager.model.Constants.HEADER_CONTENT_TYPE_VALUE;
import static com.aws.greengrass.shadowmanager.model.Constants.THROTTLED_REQUEST_WAIT_TIME_SECONDS;
import static com.aws.greengrass.shadowmanager.model.Constants.TOO_MANY_REQUESTS_ERROR_CODE;

public class ShadowHttpClient {
    private final DeviceConfiguration deviceConfiguration;

    ShadowHttpClient(DeviceConfiguration deviceConfiguration) {
        this.deviceConfiguration = deviceConfiguration;
    }

    /**
     * Creates the correct endpoint URL to perform the shadow operations on the cloud.
     *
     * @param thingName  the name of the thing
     * @param shadowName the name of the shadow
     * @return the correctly formatted IoT shadow endpoint.
     */
    private String getCloudEndpoint(String thingName, String shadowName) {
        String endpoint = Coerce.toString(this.deviceConfiguration.getIotDataEndpoint());
        //TODO: is this the correct port?
        int port = Coerce.toInt(this.deviceConfiguration.getGreengrassDataPlanePort());
        if (Utils.isEmpty(shadowName)) {
            return String.format("https://%s:%d/things/%s/shadow", endpoint, port, thingName);
        }
        return String.format("https://%s:%d/things/%s/shadow?name=%s", endpoint, port, thingName, shadowName);
    }

    /**
     * Gets an HTTP client with proxy setting if available.
     *
     * @return an HTTP client.
     */
    private SdkHttpClient getSdkHttpClient() {
        SdkHttpClient proxyClient = ProxyUtils.getSdkHttpClient();
        return proxyClient == null ? ApacheHttpClient.builder().build() : proxyClient;
    }


    private void handleBadHttpResponse(HttpExecuteResponse executeResponse)
            throws RetryableException, SkipSyncRequestException {
        if (executeResponse.httpResponse().statusCode() == HttpURLConnection.HTTP_CONFLICT
                || executeResponse.httpResponse().statusCode() == HttpURLConnection.HTTP_INTERNAL_ERROR) {
            throw new RetryableException("", HttpURLConnection.HTTP_CONFLICT);
        } else if (executeResponse.httpResponse().statusCode() == TOO_MANY_REQUESTS_ERROR_CODE) {
            throw new RetryableException("", TOO_MANY_REQUESTS_ERROR_CODE);
        }
        // Skip this request if error code is Bad Request(400), Unauthorized(401), Forbidden(403),
        // Thing/Shadow not found(404), payload too large(413) and Unsupported Media Type(415)
        throw new SkipSyncRequestException("");
    }

    /**
     * Gets the shadow from the cloud.
     *
     * @param thingName  the name of the thing
     * @param shadowName the name of the shadow
     * @return the shadow document if the shadow exists in the cloud and the request was successful; Else null.
     * @throws IOException if there was an error during the HTTP request.
     */
    public ShadowDocument getShadow(String thingName, String shadowName)
            throws IOException, RetryableException, SkipSyncRequestException {
        String url = getCloudEndpoint(thingName, shadowName);
        try (SdkHttpClient client = getSdkHttpClient()) {
            HttpExecuteRequest executeRequest = HttpExecuteRequest.builder()
                    .request(SdkHttpFullRequest.builder().uri(URI.create(url)).method(SdkHttpMethod.GET).build())
                    .build();
            HttpExecuteResponse executeResponse = client.prepareRequest(executeRequest).call();
            int responseCode = executeResponse.httpResponse().statusCode();
            if (responseCode == HttpURLConnection.HTTP_OK && executeResponse.responseBody().isPresent()) {
                return new ShadowDocument(IoUtils.toByteArray(executeResponse.responseBody().get()));
            } else {
                handleBadHttpResponse(executeResponse);
            }
        }
        return null;
    }

    /**
     * Updates the shadow on the cloud.
     *
     * @param thingName      the name of the thing
     * @param shadowName     the name of the shadow
     * @param shadowDocument the shadow document to update.
     * @return true if the shadow was updated in the cloud and the request was successful; Else false.
     * @throws IOException if there was an error during the HTTP request.
     */
    public void updateShadow(String thingName, String shadowName, ShadowDocument shadowDocument)
            throws IOException, RetryableException, SkipSyncRequestException {
        String url = getCloudEndpoint(thingName, shadowName);
        byte[] payloadBytes = JsonUtil.getPayloadBytes(shadowDocument.toJson(true));
        try (SdkHttpClient client = getSdkHttpClient()) {
            HttpExecuteRequest executeRequest = HttpExecuteRequest.builder()
                    .request(SdkHttpFullRequest.builder().uri(URI.create(url))
                            .method(SdkHttpMethod.POST)
                            .putHeader(HEADER_CONTENT_TYPE_KEY, HEADER_CONTENT_TYPE_VALUE)
                            .contentStreamProvider(() -> new ByteArrayInputStream(payloadBytes))
                            .build())
                    .build();
            HttpExecuteResponse executeResponse = client.prepareRequest(executeRequest).call();
            int responseCode = executeResponse.httpResponse().statusCode();
            if (responseCode != HttpURLConnection.HTTP_OK) {
                handleBadHttpResponse(executeResponse);
            }
        }
    }

    /**
     * Deletes the shadow on the cloud.
     *
     * @param thingName  the name of the thing
     * @param shadowName the name of the shadow
     * @return true if the shadow was deleted in the cloud and the request was successful; Else false.
     * @throws IOException if there was an error during the HTTP request.
     */
    public void deleteShadow(String thingName, String shadowName)
            throws IOException, RetryableException, SkipSyncRequestException {
        String url = getCloudEndpoint(thingName, shadowName);
        try (SdkHttpClient client = getSdkHttpClient()) {
            HttpExecuteRequest executeRequest = HttpExecuteRequest.builder()
                    .request(SdkHttpFullRequest.builder().uri(URI.create(url)).method(SdkHttpMethod.DELETE).build())
                    .build();
            HttpExecuteResponse executeResponse = client.prepareRequest(executeRequest).call();
            int responseCode = executeResponse.httpResponse().statusCode();
            if (responseCode != HttpURLConnection.HTTP_OK) {
                handleBadHttpResponse(executeResponse);
            }
        }
    }
}
