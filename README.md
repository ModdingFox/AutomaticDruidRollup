Needed a simple rule based was to submit rollups to druid.

cd to Bash and run EnvironmentSetup.sh

Environments are selected from a json file with the structuce below. Each environment has a name, zookeeper quorum, and root path used for node discovery.
[Environment/druidEnvironment.json]
{
    "MyProdDruid": {
        "Zookeeper": [ "192.168.1.100:2181", "192.168.1.101:2181", "192.168.1.102:2181" ],
        "RootZNode": "/druid/discovery"
    },
    "MyDevDruid": {
        "Zookeeper": [ "192.168.0.100:2181" ],
        "RootZNode": "/druid/discovery"
    }
}

Rollup rules are defined in the below file. Rules are evaluated in top down order for dataSource intervals and will apply the rule to the first match for any given intreval to be processed.
[Environment/druidRollUpRules.json]
[
    {
        "Period": "P7D",
        "segmentGranularity": "NONE",
        "queryGranularity": "NONE"
    },
    {
        "Period": "P1M",
        "segmentGranularity": "HOUR",
        "queryGranularity": "HOUR"
    },
    {
        "Period": "P2Y",
        "segmentGranularity": "DAY",
        "queryGranularity": "DAY"
    }
]


