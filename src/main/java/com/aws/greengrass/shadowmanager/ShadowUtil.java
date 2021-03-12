/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.shadowmanager;

import java.util.Optional;

public final class ShadowUtil {
    static final String CLASSIC_SHADOW_IDENTIFIER = "";

    /**
     * Checks and returns the CLASSIC_SHADOW_IDENTIFIER if shadowName input was null.
     *
     * @param shadowName The name of the shadow
     * @return The shadowName input or CLASSIC_SHADOW_IDENTIFIER
     */
    public static String getClassicShadowIfMissingShadowName(String shadowName) {
        return Optional.ofNullable(shadowName)
                .filter(s -> !s.isEmpty())
                .orElse(CLASSIC_SHADOW_IDENTIFIER);
    }

    private ShadowUtil() {

    }

}
