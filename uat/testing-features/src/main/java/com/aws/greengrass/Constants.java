/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass;

import java.util.concurrent.TimeUnit;

public final class Constants {
    public static final String TEST_RUN_PATH = "testRunDir";
    /**
     * Property name for the max duration for a long running test.
     * Being used for waiting before exiting
     */
    public static final String TEST_MAX_DURATION_PROP_NAME = "testMaxDurationSeconds";

    public static final long DEFAULT_GENERIC_POLLING_TIMEOUT_SECONDS =
        (long) Math.ceil(30.0 * TestUtils.getTimeOutMultiplier());
    public static final long DEFAULT_GENERIC_POLLING_TIMEOUT_MILLIS =
            TimeUnit.SECONDS.toMillis(DEFAULT_GENERIC_POLLING_TIMEOUT_SECONDS);

    private Constants() {
    }
}
