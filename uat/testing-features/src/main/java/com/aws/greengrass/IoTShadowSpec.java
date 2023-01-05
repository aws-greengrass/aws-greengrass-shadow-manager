package com.aws.greengrass;

import com.aws.greengrass.IoTShadow;
import com.aws.greengrass.testing.resources.ResourceSpec;
import lombok.Builder;
import lombok.Data;
import lombok.EqualsAndHashCode;

@Builder
@Data
@EqualsAndHashCode(callSuper = false)
public class IoTShadowSpec// extends IotShadowResourceSpec
 {
    String thingName;
    String shadowName;
    @Builder.Default
    byte[] initialPayload = "{\"state\":{\"reported\":\"SomeKey\":\"SomeValue\"}}}".getBytes();

    IoTShadow resultingIotShadow;
}
