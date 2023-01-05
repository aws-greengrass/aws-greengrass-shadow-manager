/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.utils;

import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.entity.ContentType;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.DefaultHttpRequestRetryHandler;
import org.apache.http.impl.client.HttpClients;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

public class Client implements AutoCloseable {
    private final CloseableHttpClient httpClient;

    public Client() {
        httpClient = HttpClients.custom()
                .setRetryHandler(new DefaultHttpRequestRetryHandler(5, false)).build();
    }

    @Override
    public void close() throws Exception {
        if (httpClient != null) {
            httpClient.close();
        }
    }

    public void sendAssertion(boolean success, String context, String message) throws IOException {
        int defaultPort = (int) Double.parseDouble(System.getProperty("serverPort"));
        sendAssertionWithCustomizedPort(success, context, message, defaultPort);
    }

    public void sendAssertionWithCustomizedPort(boolean success, String context, String message, int port)
            throws IOException {
        HttpPost httpPost = new HttpPost(
                "http://localhost:" + port + "/assert");
        httpPost.setEntity(new ByteArrayEntity(
                (String.format("{\"success\": %s, \"context\": \"%s\", \"message\": \"%s\"}", success, context,
                        message)).getBytes(StandardCharsets.UTF_8), ContentType.APPLICATION_JSON));
        try (CloseableHttpResponse response = httpClient.execute(httpPost)) {
            response.getStatusLine().getStatusCode();
        }
    }
}
