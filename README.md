Needed a simple rule based was to submit rollups to druid.

cd to Bash and run EnvironmentSetup.sh

Environments are selected from a json file with the structure below. Each environment has a name, zookeeper quorum, and root path used for node discovery.<br />
[Environment/druidEnvironment.json]<br />
{<br />
    "MyProdDruid": {<br />
        "Zookeeper": [ "192.168.1.100:2181", "192.168.1.101:2181", "192.168.1.102:2181" ],<br />
        "RootZNode": "/druid/discovery"<br />
    },<br />
    "MyDevDruid": {<br />
        "Zookeeper": [ "192.168.0.100:2181" ],<br />
        "RootZNode": "/druid/discovery"<br />
    }<br />
}<br />
<br />
Rollup rules are defined in the below file. Rules are evaluated in top down order for dataSource intervals and will apply the rule to the first match for any given intreval to be processed.<br />
[Environment/druidRollUpRules.json]<br />
[<br />
    {<br />
        "Period": "P7D",<br />
        "segmentGranularity": "NONE",<br />
        "queryGranularity": "NONE"<br />
    },<br />
    {<br />
        "Period": "P1M",<br />
        "segmentGranularity": "HOUR",<br />
        "queryGranularity": "HOUR"<br />
    },<br />
    {<br />
        "Period": "P2Y",<br />
        "segmentGranularity": "DAY",<br />
        "queryGranularity": "DAY"<br />
    }<br />
]


