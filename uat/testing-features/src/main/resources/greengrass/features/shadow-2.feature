@ShadowManager @Shadow2
Feature: Shadow-2

  As a developer, I can synchronize my local and cloud shadows.

  Background:
    Given my device is registered as a Thing
    And my device is running Greengrass
    When I add random shadow for MyThing with name MyThingNamedShadow in context
    When I add random shadow for MyThing2 with name MyThingNamedShadow2 in context

  @stable @functional @JavaSDK @smoke @RunWithHSM
  Scenario Outline: Shadow-2-T1-<strategy>: As a developer, I can sync a local named shadow to the cloud.
    When I create a Greengrass deployment with components
      | aws.greengrass.Nucleus       | LATEST |
      | aws.greengrass.ShadowManager | LATEST |
    And I update my Greengrass deployment configuration, setting the component aws.greengrass.ShadowManager configuration to:
        """
        {
           "MERGE":{
              "strategy":{
                 "type":"<strategy>",
                 "delay":5
              },
              "synchronize":{
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
    Then the Greengrass deployment is COMPLETED on the device after 2 minutes
    And I install the component ShadowComponentPing from local store with configuration
      | key                    | value                                                                                                          |
      | assertionServerPort    | ${assertionServerPort}                                                                                         |
      | Operation              | UpdateThingShadow                                                                                              |
      | ThingName              | MyThing                                                                                                        |
      | ShadowName             | MyThingNamedShadow                                                                                             |
      | ShadowDocument         | {\"state\":{\"reported\":{\"color\":{\"r\":255,\"g\":255,\"b\":255},\"SomeKey\":\"SomeValue\"}}}               |
      | OperationTimeout       | <timeout>                                                                                                      |
#    Then I get 1 assertions with context "Updated shadow document" within 15 seconds
    Then I can get cloud shadow for MyThing with name MyThingNamedShadow with state {"state":{"reported":{"color":{"r":255,"g":255,"b":255},"SomeKey":"SomeValue"}}} within 30 seconds
    When I install the component ShadowComponentPong from local store with configuration
      | key              | value              |
      | Operation        | DeleteThingShadow  |
      | ThingName        | MyThing            |
      | ShadowName       | MyThingNamedShadow |
      | ShadowDocument   |                    |
      | OperationTimeout | <timeout>          |
#    Then I get 1 assertions with context "Deleted shadow document" within 15 seconds
    And I can not get cloud shadow for MyThing with name MyThingNamedShadow within 30 seconds
    
    Examples:
      | strategy | timeout |
      | realTime | 5       |
    
    @gamma
    Examples:
      | strategy | timeout |
      | periodic | 30      |