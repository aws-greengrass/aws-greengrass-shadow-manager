package com.aws.greengrass;

//import com.aws.greengrass.platforms.Platform;
//import com.aws.iot.evergreen.common.Constants;
//
//import io.netty.util.internal.StringUtil;
import com.github.jknack.handlebars.internal.lang3.StringUtils;
import lombok.extern.log4j.Log4j2;

import java.util.function.Predicate;


@Log4j2
public final class TestUtils {

    private TestUtils() {
    }

    /**
     * Different devices are slower than others. It's good to multiply
     * hard-coded timeout values by a rough factor to accommodate these
     * slower devices.
     * @return timeout multiplier for this platform.
     */
    public static double getTimeOutMultiplier() {
        final double timeoutMultiplier = 1; //TODO: need to find a good replacement for Platform.getTimeoutMultiplier();
        log.debug(String.format(
            "the timeout multiplier for this platform is '%f'",
            timeoutMultiplier
        ));
        return timeoutMultiplier;
    }

    public static int getTestMaxDurationSeconds() {
        final String waitTime = System.getProperty(Constants.TEST_MAX_DURATION_PROP_NAME);
        if (StringUtils.isBlank(waitTime)) {
            throw new NullPointerException(String.format("Property %s is not set",
                    Constants.TEST_MAX_DURATION_PROP_NAME));
        } else {
            return Integer.parseInt(waitTime);
        }
    }
}
