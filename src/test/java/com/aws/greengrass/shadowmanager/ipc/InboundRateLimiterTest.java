/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.shadowmanager.ipc;

import com.aws.greengrass.shadowmanager.exception.ThrottledRequestException;
import com.aws.greengrass.testcommons.testutilities.GGExtension;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

import static com.aws.greengrass.shadowmanager.TestUtils.THING_NAME;
import static com.aws.greengrass.shadowmanager.model.Constants.DEFAULT_LOCAL_SHADOW_REQUESTS_PER_THING_PS;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

@ExtendWith({MockitoExtension.class, GGExtension.class})
public class InboundRateLimiterTest {
    private static final int TEST_RATE_LIMIT = 1;
    private final InboundRateLimiter inboundRateLimiter = new InboundRateLimiter();

    @BeforeEach
    void setup() {
        inboundRateLimiter.clear();
        inboundRateLimiter.setRate(DEFAULT_LOCAL_SHADOW_REQUESTS_PER_THING_PS);
    }

    @Test
    void GIVEN_new_thing_WHEN_acquire_lock_for_thing_THEN_lock_acquired() {
        assertDoesNotThrow(() -> inboundRateLimiter.acquireLockForThing(THING_NAME));
    }

    @Test
    void GIVEN_rate_throttled_for_thing_WHEN_acquire_lock_for_thing_THEN_throw_throttled_request_exception() throws ThrottledRequestException, InterruptedException {
        inboundRateLimiter.setRate(TEST_RATE_LIMIT);

        // use up permit
        assertDoesNotThrow(() -> inboundRateLimiter.acquireLockForThing(THING_NAME));

        // next call should throw exception
        ThrottledRequestException thrown = assertThrows(ThrottledRequestException.class, () -> inboundRateLimiter.acquireLockForThing(THING_NAME));
        assertThat(thrown.getMessage(), is(equalTo("Local shadow request throttled for thing")));
    }

    @Test
    void GIVEN_acquiring_lock_for_unthrottled_thing_WHEN_acquire_lock_for_thing_THEN_lock_acquired() throws ThrottledRequestException, InterruptedException {
        inboundRateLimiter.setRate(TEST_RATE_LIMIT);

        // use up permit
        assertDoesNotThrow(() -> inboundRateLimiter.acquireLockForThing(THING_NAME));

        // verify acquiring lock for separate thing does not throttle
        assertDoesNotThrow(() -> inboundRateLimiter.acquireLockForThing("separateThing"));

        // verify original thing throttles
        assertThrows(ThrottledRequestException.class, () -> inboundRateLimiter.acquireLockForThing(THING_NAME));
    }

    @Test
    void GIVEN_new_rate_WHEN_set_rate_THEN_rate_set_for_existing_limiters() throws ThrottledRequestException, InterruptedException {
        final String existingThing1 = "existingThing1";
        final String existingThing2 = "existingThing2";

        // create rate limiters for two things
        inboundRateLimiter.acquireLockForThing(existingThing1);
        inboundRateLimiter.acquireLockForThing(existingThing2);

        // refresh throttle time period
        Thread.sleep(1000);

        // set rate and use allowed permit
        inboundRateLimiter.setRate(TEST_RATE_LIMIT);
        inboundRateLimiter.acquireLockForThing(existingThing1);
        inboundRateLimiter.acquireLockForThing(existingThing2);

        assertThrows(ThrottledRequestException.class, () -> inboundRateLimiter.acquireLockForThing(existingThing1));
        assertThrows(ThrottledRequestException.class, () -> inboundRateLimiter.acquireLockForThing(existingThing2));
    }

    @Test
    void GIVEN_existing_rate_limiters_for_things_WHEN_clear_THEN_rate_limiters_cleared() throws ThrottledRequestException, InterruptedException {
        final String existingThing1 = "existingThing1";
        final String existingThing2 = "existingThing2";
        inboundRateLimiter.clear();
        inboundRateLimiter.setRate(TEST_RATE_LIMIT);

        // create rate limiters for two things
        inboundRateLimiter.acquireLockForThing(existingThing1);
        inboundRateLimiter.acquireLockForThing(existingThing2);

        // existing rate limiters should be cleared and acquiring lock for things should use fresh rate limiter
        inboundRateLimiter.clear();
        assertDoesNotThrow(() -> inboundRateLimiter.acquireLockForThing(existingThing1));
        assertDoesNotThrow(() -> inboundRateLimiter.acquireLockForThing(existingThing2));
    }
}
