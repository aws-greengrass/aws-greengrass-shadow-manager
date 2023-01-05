package com.aws.greengrass;

import com.aws.greengrass.testing.resources.AWSResource;
import lombok.Data;
import lombok.extern.log4j.Log4j2;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.iotdataplane.IotDataPlaneClient;
import software.amazon.awssdk.services.iotdataplane.model.DeleteThingShadowRequest;
import software.amazon.awssdk.services.iotdataplane.model.ResourceNotFoundException;
import software.amazon.awssdk.services.iotdataplane.model.UpdateThingShadowRequest;

@Data
@Log4j2
public class IoTShadow implements AWSResource<IotDataPlaneClient> {
    boolean deleted = false;
    String thingName;
    String shadowName;

    public IoTShadow(String thingName, String shadowName) {
        this.thingName = thingName;
        this.shadowName = shadowName;
    }

//    public static IoTShadow create(IotDataPlaneClient iotDataPlaneClient, IoTShadowSpec spec) {
//        log.atDebug().log("Creating IoT Shadow for thing {} with name {}", spec.getThingName(), spec.getShadowName());
//        iotDataPlaneClient.updateThingShadow(UpdateThingShadowRequest.builder().thingName(spec.getThingName())
//                .shadowName(spec.getShadowName()).payload(SdkBytes.fromByteArray(spec.getInitialPayload())).build());
//        return new IoTShadow(spec.getThingName(), spec.getShadowName());
//    }
    @Override
    public void remove(IotDataPlaneClient client) {
        if (deleted) {
            return;
        }
        log.debug("Removing shadow for thing {} with name {}", thingName, shadowName);
        try {
            client.deleteThingShadow(DeleteThingShadowRequest.builder().thingName(thingName).shadowName(shadowName).build());
            log.debug("Removed shadow for thing {} with name {}", thingName, shadowName);
        } catch (ResourceNotFoundException e) {
            log.error(e);
        } finally {
            deleted = true;
        }
    }
}
