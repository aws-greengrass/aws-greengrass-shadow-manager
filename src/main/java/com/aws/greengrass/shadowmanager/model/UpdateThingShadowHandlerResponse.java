/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.shadowmanager.model;

import lombok.AllArgsConstructor;
import lombok.Getter;
import software.amazon.awssdk.aws.greengrass.model.UpdateThingShadowResponse;

@AllArgsConstructor
@Getter
public class UpdateThingShadowHandlerResponse {
    private final UpdateThingShadowResponse updateThingShadowResponse;
    private final byte[] currentDocument;
}
