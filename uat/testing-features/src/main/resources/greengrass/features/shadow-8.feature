@Shadow @Shadow-8 @Greengate @beta
Feature: Shadow-8

  As a developer, I can set the sync directionality.

  Background:
    Given my device is registered as a Thing
    And my device is running Greengrass
    And I start an assertion server
    When I add random shadow for MyThing with name MyThingNamedShadow in context
    When I add random shadow for MyThing2 with name MyThingNamedShadow2 in context

  @functional @JavaSDK @smoke @RunWithHSM
  Scenario: Shadow-8-T1: As a developer, I can sync a local named shadow between cloud and device.
    When I create a Greengrass deployment with components
      | aws.greengrass.Nucleus       | LATEST  |
      | aws.greengrass.ShadowManager | LATEST |
    And I update my Greengrass deployment configuration, setting the component aws.greengrass.ShadowManager configuration to:
        """
        {
           "MERGE":{
              "synchronize":{
                 "direction": "betweenDeviceAndCloud",
                 "shadowDocuments":[
                    {
                       "thingName":"${MyThing}",
                       "classic":false,
                       "namedShadows":[
                          "${MyThingNamedShadow}"
                       ]
                    },
                    {
                       "thingName":"${MyThing2}",
                       "classic":true
                    }
                 ],
                 "provideSyncStatus":true,
                 "maxOutboundSyncUpdatesPerSecond":50
              },
              "shadowDocumentSizeLimitBytes":8192,
              "maxDiskUtilizationMegaBytes":16
           }
        }
        """
    And I update my Greengrass deployment configuration, setting the component aws.greengrass.Nucleus configuration to:
        """
        {"MERGE":{"logging": {"level": "DEBUG"}}}
        """
    And I deploy the Greengrass deployment configuration
    Then the Greengrass deployment is COMPLETED on the device after 4 minutes
    When I install the component ShadowComponentPing from local store with configuration
        """
        {
           "MERGE":{
                "assertionServerPort": ${assertionServerPort},
                "Operation": "UpdateThingShadow",
                "ThingName": "MyThing",
                "ShadowName": "MyThingNamedShadow",
                "ShadowDocument": "{\"state\":{\"reported\":{\"color\":{\"r\":255,\"g\":255,\"b\":255},\"SomeKey\":\"SomeValue\"}}}",
                "ExpectedShadowDocument": "{\"version\":1,\"state\":{\"reported\":{\"color\":{\"r\":255,\"g\":255,\"b\":255},\"SomeKey\":\"SomeValue\"}}}"
           }
        }
        """
    Then I get 1 assertions with context "Updated shadow document" within 15 seconds
    Then I can get cloud shadow for MyThing with name MyThingNamedShadow with state {"state":{"reported":{"color":{"r":255,"g":255,"b":255},"SomeKey":"SomeValue"}}} within 30 seconds
    When I can create cloud shadow for MyThing with name MyThingNamedShadow with state {"state":{"reported":{"color":{"r":255,"g":0,"b":0},"SomeKey":"SomeValue"}}}
    Then I can get cloud shadow for MyThing with version 2 and state {"state":{"reported":{"color":{"r":255,"g":0,"b":0},"SomeKey":"SomeValue"}}} within 30 seconds
    When I install the component ShadowComponentPing from local store with configuration
        """
        {
           "MERGE":{
                "assertionServerPort": ${assertionServerPort},
                "Operation": "GetThingShadow",
                "ThingName": "MyThing",
                "ShadowName": "MyThingNamedShadow",
                "ShadowDocument": "{\"version\":2,\"state\":{\"reported\":{\"color\":{\"r\":255,\"g\":0,\"b\":0},\"SomeKey\":\"SomeValue\"}}}"
           }
        }
        """
    Then I get 1 assertions with context "Retrieved matching shadow document" within 15 seconds

  Scenario: Shadow-8-T2: As a developer, I can sync a local named shadow from device to cloud and not vice versa.
    When I create a Greengrass deployment with components
      | aws.greengrass.Nucleus       | LATEST |
      | aws.greengrass.ShadowManager | LATEST |
    And I update my Greengrass deployment configuration, setting the component aws.greengrass.ShadowManager configuration to:
        """
        {
           "MERGE":{
              "synchronize":{
                 "direction": "deviceToCloud",
                 "shadowDocuments":[
                    {
                       "thingName":"${MyThing}",
                       "classic":false,
                       "namedShadows":[
                          "${MyThingNamedShadow}"
                       ]
                    },
                    {
                       "thingName":"${MyThing2}",
                       "classic":true
                    }
                 ],
                 "provideSyncStatus":true,
                 "maxOutboundSyncUpdatesPerSecond":50
              },
              "shadowDocumentSizeLimitBytes":8192,
              "maxDiskUtilizationMegaBytes":16
           }
        }
        """
    And I update my Greengrass deployment configuration, setting the component aws.greengrass.Nucleus configuration to:
        """
        {"MERGE":{"logging": {"level": "DEBUG"}}}
        """
    And I deploy the Greengrass deployment configuration
    Then the Greengrass deployment is COMPLETED on the device after 4 minutes
    When I install the component ShadowComponentPing from local store with configuration
        """
        {
           "MERGE":{
                "assertionServerPort": ${assertionServerPort},
                "Operation": "UpdateThingShadow",
                "ThingName": "MyThing",
                "ShadowName": "MyThingNamedShadow",
                "ShadowDocument": "{\"state\":{\"reported\":{\"color\":{\"r\":255,\"g\":255,\"b\":255},\"SomeKey\":\"SomeValue\"}}}",
                "ExpectedShadowDocument": "{\"version\":1,\"state\":{\"reported\":{\"color\":{\"r\":255,\"g\":255,\"b\":255},\"SomeKey\":\"SomeValue\"}}}"
           }
        }
        """
    Then I get 1 assertions with context "Updated shadow document" within 15 seconds
    Then I can get cloud shadow for MyThing with version 1 and state {"state":{"reported":{"color":{"r":255,"g":255,"b":255},"SomeKey":"SomeValue"}}} within 30 seconds
    When I can create cloud shadow for MyThing with name MyThingNamedShadow with state {"state":{"reported":{"color":{"r":255,"g":0,"b":0},"SomeKey":"SomeValue"}}}
    Then I can get cloud shadow for MyThing with version 2 and state {"state":{"reported":{"color":{"r":255,"g":0,"b":0},"SomeKey":"SomeValue"}}} within 30 seconds
    # Local should still have version 1 of the shadow document
    When I install the component ShadowComponentPing from local store with configuration
        """
        {
           "MERGE":{
                "assertionServerPort": ${assertionServerPort},
                "Operation": "GetThingShadow",
                "ThingName": "MyThing",
                "ShadowName": "MyThingNamedShadow",
                "ShadowDocument": "{\"version\":1,\"state\":{\"reported\":{\"color\":{\"r\":255,\"g\":255,\"b\":255},\"SomeKey\":\"SomeValue\"}}}"
           }
        }
        """
    Then I get 1 assertions with context "Retrieved matching shadow document" within 15 seconds

  Scenario: Shadow-8-T3: As a developer, I can sync a local named shadow from cloud to device and not vice versa.
    When I create a Greengrass deployment with components
      | aws.greengrass.Nucleus       | LATEST |
      | aws.greengrass.ShadowManager | LATEST |
    And I update my Greengrass deployment configuration, setting the component aws.greengrass.ShadowManager configuration to:
        """
        {
           "MERGE":{
              "synchronize":{
                 "direction": "cloudToDevice",
                 "shadowDocuments":[
                    {
                       "thingName":"${MyThing}",
                       "classic":false,
                       "namedShadows":[
                          "${MyThingNamedShadow}"
                       ]
                    },
                    {
                       "thingName":"${MyThing2}",
                       "classic":true
                    }
                 ],
                 "provideSyncStatus":true,
                 "maxOutboundSyncUpdatesPerSecond":50
              },
              "shadowDocumentSizeLimitBytes":8192,
              "maxDiskUtilizationMegaBytes":16
           }
        }
        """
    And I update my Greengrass deployment configuration, setting the component aws.greengrass.Nucleus configuration to:
        """
        {"MERGE":{"logging": {"level": "DEBUG"}}}
        """

    And I deploy the Greengrass deployment configuration
    Then the Greengrass deployment is COMPLETED on the device after 4 minutes
    When I can create cloud shadow for MyThing with name MyThingNamedShadow with state {"state":{"reported":{"color":{"r":255,"g":0,"b":0},"SomeKey":"SomeValue"}}}
    Then I can get cloud shadow for MyThing with version 1 and state {"state":{"reported":{"color":{"r":255,"g":0,"b":0},"SomeKey":"SomeValue"}}} within 30 seconds
    When I install the component ShadowComponentPing from local store with configuration
        """
        {
           "MERGE":{
                "assertionServerPort": ${assertionServerPort},
                "Operation": "GetThingShadow",
                "ThingName": "MyThing",
                "ShadowName": "MyThingNamedShadow",
                "ShadowDocument": "{\"version\":1,\"state\":{\"reported\":{\"color\":{\"r\":255,\"g\":0,\"b\":0},\"SomeKey\":\"SomeValue\"}}}"
           }
        }
        """
    Then I get 1 assertions with context "Retrieved matching shadow document" within 15 seconds
    When I install the component ShadowComponentPing from local store with configuration
        """
        {
           "MERGE":{
                "assertionServerPort": ${assertionServerPort},
                "Operation": "UpdateThingShadow",
                "ThingName": "MyThing",
                "ShadowName": "MyThingNamedShadow",
                "ShadowDocument": "{\"state\":{\"reported\":{\"color\":{\"r\":255,\"g\":255,\"b\":255},\"SomeKey\":\"SomeValue\"}}}",
                "ExpectedShadowDocument":"{\"version\":2,\"state\":{\"reported\":{\"color\":{\"r\":255,\"g\":255,\"b\":255},\"SomeKey\":\"SomeValue\"}}}"
           }
        }
        """
    Then I get 1 assertions with context "Updated shadow document" within 15 seconds
    # Cloud should still have version 1 of the shadow document
    Then I can get cloud shadow for MyThing with version 1 and state {"state":{"reported":{"color":{"r":255,"g":0,"b":0},"SomeKey":"SomeValue"}}} within 30 seconds