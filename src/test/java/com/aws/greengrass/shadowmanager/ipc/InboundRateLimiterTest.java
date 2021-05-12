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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.NullAndEmptySource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import vendored.com.google.common.util.concurrent.RateLimiter;

import java.util.concurrent.ConcurrentHashMap;

import static com.aws.greengrass.shadowmanager.TestUtils.THING_NAME;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyDouble;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.verify;

@ExtendWith({MockitoExtension.class, GGExtension.class})
public class InboundRateLimiterTest {
    private static final int TEST_RATE_LIMIT = 1;
    private InboundRateLimiter inboundRateLimiter;

    @Mock
    RateLimiter mockRateLimiter;

    @Mock
    ConcurrentHashMap<String, RateLimiter> mockRateLimiterMap;

    @BeforeEach
    void setup() {
        lenient().when(mockRateLimiterMap.computeIfAbsent(anyString(), any())).thenReturn(mockRateLimiter);
        lenient().when(mockRateLimiter.tryAcquire()).thenReturn(true);
        inboundRateLimiter = new InboundRateLimiter();
        inboundRateLimiter.setRateLimiterMap(mockRateLimiterMap);
    }

    @Test
    void GIVEN_new_thing_WHEN_acquire_lock_for_thing_THEN_lock_acquired() {
        assertDoesNotThrow(() -> inboundRateLimiter.acquireLockForThing(THING_NAME));
    }

    @Test
    void GIVEN_failure_to_get_lock_WHEN_acquire_lock_for_thing_THEN_throw_throttled_request_exception() {
        when(mockRateLimiter.tryAcquire()).thenReturn(false);

        ThrottledRequestException thrown = assertThrows(ThrottledRequestException.class, () -> inboundRateLimiter.acquireLockForThing(THING_NAME));
        assertThat(thrown.getMessage(), is(equalTo("Local shadow request throttled for thing")));
    }

    @ParameterizedTest
    @NullAndEmptySource
    void GIVEN_acquiring_lock_for_missing_thing_WHEN_acquire_lock_for_thing_THEN_do_nothing(String thingName) {
        assertDoesNotThrow(() -> inboundRateLimiter.acquireLockForThing(thingName));
        verify(mockRateLimiterMap, times(0)).computeIfAbsent(anyString(), any());
        verify(mockRateLimiter, times(0)).tryAcquire();
    }

    @Test
    void GIVEN_new_rate_WHEN_set_rate_THEN_rate_set_for_existing_limiters() {
        RateLimiter mockLimiter1 = mock(RateLimiter.class);
        RateLimiter mockLimiter2 = mock(RateLimiter.class);

        ConcurrentHashMap<String, RateLimiter> testMap = new ConcurrentHashMap<>();
        testMap.put("existingThing1", mockLimiter1);
        testMap.put("existingThing2", mockLimiter2);
        inboundRateLimiter.setRateLimiterMap(testMap);

        inboundRateLimiter.setRate(TEST_RATE_LIMIT);

        verify(mockLimiter1, times(1)).setRate(anyDouble());
        verify(mockLimiter2, times(1)).setRate(anyDouble());
    }

    @Test
    void GIVEN_existing_rate_limiters_for_things_WHEN_clear_THEN_rate_limiters_cleared() {
        inboundRateLimiter.clear();
        verify(mockRateLimiterMap, times(1)).clear();
    }
}
