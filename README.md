Needed a simple rule based was to submit rollups to druid.

cd to Bash and run EnvironmentSetup.sh

Environments are selected from a json file with the structure below. Each environment has a name, zookeeper quorum, and root path used for node discovery.<br />
[Environment/druidEnvironment.json]<br />
{<br />
&nbsp;"MyProdDruid": {<br />
&nbsp;&nbsp;"Zookeeper": [ "192.168.1.100:2181", "192.168.1.101:2181", "192.168.1.102:2181" ],<br />
&nbsp;&nbsp;"RootZNode": "/druid/discovery"<br />
&nbsp;},<br />
&nbsp;"MyDevDruid": {<br />
&nbsp;&nbsp;"Zookeeper": [ "192.168.0.100:2181" ],<br />
&nbsp;&nbsp;"RootZNode": "/druid/discovery"<br />
&nbsp;}<br />
}<br />
<br />
Rollup rules are defined in the below file. Rules are evaluated in top down order for dataSource intervals and will apply the rule to the first match for any given intreval to be processed.<br />
[Environment/druidRollUpRules.json]<br />
[<br />
&nbsp;{<br />
&nbsp;&nbsp;"Period": "P7D",<br />
&nbsp;&nbsp;"segmentGranularity": "NONE",<br />
&nbsp;&nbsp;"queryGranularity": "NONE"<br />
&nbsp;},<br />
&nbsp;{<br />
&nbsp;&nbsp;"Period": "P1M",<br />
&nbsp;&nbsp;"segmentGranularity": "HOUR",<br />
&nbsp;&nbsp;"queryGranularity": "HOUR"<br />
&nbsp;},<br />
&nbsp;{<br />
&nbsp;&nbsp;"Period": "P2Y",<br />
&nbsp;&nbsp;"segmentGranularity": "DAY",<br />
&nbsp;&nbsp;"queryGranularity": "DAY"<br />
&nbsp;}<br />
]


