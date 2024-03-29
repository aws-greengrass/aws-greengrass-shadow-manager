@ShadowManager @Shadow2
Feature: Shadow-2

  As a developer, I can synchronize my local and cloud shadows.

  Background:
    Given my device is registered as a Thing
    And my device is running Greengrass
    When I add random shadow for MyThing with name MyThingNamedShadow in context
    When I add random shadow for MyThing2 with name MyThingNamedShadow2 in context

  @unstable
  Scenario Outline: Shadow-2-T1-<strategy>: As a developer, I can sync a local named shadow to the cloud.
    When I create a Greengrass deployment with components
      | aws.greengrass.Nucleus       | LATEST |
      | aws.greengrass.ShadowManager | classpath:/greengrass/recipes/recipe.yaml |
      | aws.greengrass.Cli       | LATEST |
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
    When I install the component ShadowComponentPing from local store with configuration
        """
        {
           "MERGE":{
                "Operation": "UpdateThingShadow",
                "ThingName": "${MyThing}",
                "ShadowName": "${MyThingNamedShadow}",
                "OperationTimeout": "<timeout>",
                "ShadowDocument": "{\\\"state\\\":{\\\"reported\\\":{\\\"color\\\":{\\\"r\\\":255,\\\"g\\\":255,\\\"b\\\":255},\\\"SomeKey\\\":\\\"SomeValue\\\"}}}"
           }
        }
    """
    Then the local Greengrass deployment is SUCCEEDED on the device after 120 seconds
    Then I can get cloud shadow for MyThing with name MyThingNamedShadow with state {"state":{"reported":{"color":{"r":255,"g":255,"b":255},"SomeKey":"SomeValue"}}} within 30 seconds
    When I install the component ShadowComponentPong from local store with configuration
    """
        {
           "MERGE":{
                "Operation": "DeleteThingShadow",
                "ThingName": "${MyThing}",
                "ShadowName": "${MyThingNamedShadow}",
                "OperationTimeout": "<timeout>"
           }
        }
    """
      Then the local Greengrass deployment is SUCCEEDED on the device after 120 seconds
      And I can not get cloud shadow for MyThing with name MyThingNamedShadow within 30 seconds
    
    Examples:
      | strategy | timeout |
      | realTime | 5       |

