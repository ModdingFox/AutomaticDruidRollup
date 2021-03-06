#!/bin/python3

import getopt, sys, os
import json
import getDruidEnvironment
import Find_Druid
import Fetch_Druid_Rules
from kazoo.client import KazooClient
from kazoo.exceptions import NodeExistsError
import requests
import re
import datetime
import isodate

druidConfigPath = None;
druidSelectedEnvironment = None;

zookeeper = None;
rootZNode = None;
configRootZNode = None;

druidRouters = None;

validGranularities = { "NONE": None, "SECOND": 86400, "MINUTE": 1440, "FIFTEEN_MINUTE": 96, "THIRTY_MINUTE": 48, "HOUR": 24, "DAY": 1};

rules = {};

def getTaskTemplate():
    taskTemplate = {};
    taskTemplate["type"] = "index_parallel";
    taskTemplate["spec"] = {};
    taskTemplate["spec"]["tuningConfig"] = {};
    taskTemplate["spec"]["tuningConfig"]["type"] = "index_parallel";
    taskTemplate["spec"]["tuningConfig"]["forceExtendableShardSpecs"] = True;
    taskTemplate["spec"]["tuningConfig"]["forceGuaranteedRollup"] = True;
    taskTemplate["spec"]["tuningConfig"]["maxNumSubTasks"] = 50;
    taskTemplate["spec"]["tuningConfig"]["partitionsSpec"] = {};
    taskTemplate["spec"]["tuningConfig"]["partitionsSpec"]["type"] = "hashed";
    taskTemplate["spec"]["ioConfig"] = {};
    taskTemplate["spec"]["ioConfig"]["type"] = "index_parallel";
    taskTemplate["spec"]["ioConfig"]["appendToExisting"] = False;
    taskTemplate["spec"]["ioConfig"]["inputSource"] = {};
    taskTemplate["spec"]["ioConfig"]["inputSource"]["type"] = "druid";
    taskTemplate["spec"]["ioConfig"]["inputSource"]["dataSource"] = None;
    taskTemplate["spec"]["ioConfig"]["inputSource"]["interval"] = None;
    taskTemplate["spec"]["dataSchema"] = {};
    taskTemplate["spec"]["dataSchema"]["dataSource"] = None;
    taskTemplate["spec"]["dataSchema"]["granularitySpec"] = {};
    taskTemplate["spec"]["dataSchema"]["granularitySpec"]["type"] = "uniform";
    taskTemplate["spec"]["dataSchema"]["granularitySpec"]["rollup"] = "true";
    taskTemplate["spec"]["dataSchema"]["granularitySpec"]["queryGranularity"] = None;
    taskTemplate["spec"]["dataSchema"]["granularitySpec"]["segmentGranularity"] = None;
    taskTemplate["spec"]["dataSchema"]["granularitySpec"]["intervals"] = None;
    taskTemplate["spec"]["dataSchema"]["timestampSpec"] = None;
    taskTemplate["spec"]["dataSchema"]["dimensionsSpec"] = {};
    taskTemplate["spec"]["dataSchema"]["dimensionsSpec"]["dimensions"] = None;
    taskTemplate["spec"]["dataSchema"]["metricsSpec"] = None;
    return taskTemplate;

def Print_Command_Help( ):
    print("Usage:");
    print("--druidConfigPath=\"/etc/Druid/druidEnvironment.json\" - Path to drud environments config");
    print("--druidSelectedEnvironment=\"Azure\" - Environment name to load configs for");
    print("--zookeeper=\"192.168.1.100:2181,192.168.1.101:2181,192.168.1.102:2181\" - Used with rootZNode and targetProtocol to discover brokers");
    print("--rootZNode=\"/brokers\" - Root ZNode for brokers");
    print("--configRootZNode=\"/druidRollUpRules\" - Root Z Node for data source rules");

def Load_EnvVars():
    global druidConfigPath;
    global druidSelectedEnvironment;
    global zookeeper;
    global rootZNode;
    global configRootZNode;
    
    druidConfigPath = os.getenv('druidConfigPath');
    druidSelectedEnvironment = os.getenv('druidSelectedEnvironment');
    zookeeper = os.getenv('zookeeper');
    rootZNode = os.getenv('rootZNode');
    configRootZNode = os.getenv('configRootZNode');
    return;

def Load_Agrs():
    global druidConfigPath;
    global druidSelectedEnvironment;
    global zookeeper;
    global rootZNode;
    global druidRouters;
    global configRootZNode;
    global rules;
    
    fullCmdArguments = sys.argv;
    argumentList = fullCmdArguments[1:];
    
    unixOptions = "ho:v";
    Command_Line_Options = [ "druidConfigPath=", "druidSelectedEnvironment=", "zookeeper=", "rootZNode=", "configRootZNode=" ];
    
    try:
        arguments, values = getopt.getopt(argumentList, unixOptions, Command_Line_Options);
    except getopt.error as err:
        print (str(err));
        exit(-1);
    
    for currentArgument, currentValue in arguments:
        if currentArgument in ("--druidConfigPath"):
            druidConfigPath=currentValue;
        elif currentArgument in ("--druidSelectedEnvironment"):
            druidSelectedEnvironment=currentValue;
        elif currentArgument in ("--zookeeper"):
            zookeeper=currentValue;
        elif currentArgument in ("--rootZNode"):
            rootZNode=currentValue;
        elif currentArgument in ("--configRootZNode"):
            configRootZNode=currentValue;
    
    if druidConfigPath is not None and druidSelectedEnvironment is not None:
        zookeeperReturn, rootZNodeReturn, configRootZNodeReturn = getDruidEnvironment.loadEnvironment(druidConfigPath, druidSelectedEnvironment);
        if zookeeperReturn is not None:
            zookeeper = zookeeperReturn;
        if rootZNodeReturn is not None:
            rootZNode = rootZNodeReturn;
        if configRootZNodeReturn is not None:
            configRootZNode = configRootZNodeReturn;
    
    if zookeeper is not None and rootZNode is not None:
       druidRouters = Find_Druid.Get_Druid_Config(zookeeper, rootZNode, "druid", "router");

    if zookeeper is not None and configRootZNode is not None:
        rules = Fetch_Druid_Rules.getRollupRules(zookeeper, configRootZNode);
    
    if druidRouters is None:
        print("druid not valid ensure that either druidConfigPath and druidSelectedEnvironment or zookeeper and rootZNode are set correctly");
        Print_Command_Help();
        sys.exit(-1);
    elif len(rules.keys()) == 0:
        print("Rules empty. Ensure that zookeeper it up, the configRootZNode is correct, and that at least the \"_default\" znode exists");
        Print_Command_Help();
        sys.exit(-1);

def submitGetToDruid(DRUID_TARGET_HOSTS, API_TARGET):
    for Druid_Target_Host in DRUID_TARGET_HOSTS:
        h={"User-Agent":"curl/pythonrequests"}
        r = requests.get("http://" + Druid_Target_Host + API_TARGET, verify=False, headers=h)
        if r.status_code == requests.codes.ok:
            print("Called get " + Druid_Target_Host + API_TARGET);
            return r.text;
        else:
            print("Could not call get " + Druid_Target_Host + API_TARGET);
    return False;

def submitPayLoadToDruid(DRUID_TARGET_HOSTS, API_TARGET, Druid_Data):
    for Druid_Target_Host in DRUID_TARGET_HOSTS:
        h={"User-Agent":"curl/pythonrequests"}
        r = requests.post("http://" + Druid_Target_Host + API_TARGET, json = Druid_Data, verify=False, headers=h)
        if r.status_code == requests.codes.ok:
            print("Uploaded json to " + Druid_Target_Host + API_TARGET);
            return r.text;
        else:
            print("Could not upload json to " + Druid_Target_Host + API_TARGET);
    return False;

def getSegmentCountsByDayInInterval(DRUID_TARGET_HOSTS, dataSource, startTime, endTime):
    segmentQuery = {};
    segmentQuery["query"] = "SELECT LEFT(\"start\",10) AS \"start\", COUNT(*) AS \"count\" FROM sys.segments WHERE datasource = '{0}' AND \"start\" BETWEEN '{1}' AND '{2}' GROUP BY 1".format(dataSource, startTime, endTime);
    segmentQuery["resultFormat"] = "object";
    queryResults = submitPayLoadToDruid(DRUID_TARGET_HOSTS, "/druid/v2/sql", segmentQuery);
    if queryResults is False:
        print("Failed to retrieve segment info failing");
        exit(-1);
    queryResults = json.loads(queryResults);
    result = {};
    for queryResult in queryResults:
        if "start" in queryResult.keys() and "count" in queryResult.keys():
            result[queryResult["start"]] = queryResult["count"];
        else:
            print("Got bad data back from druid segment count query");
            exit(-1);
    return result;

def validateRollupRules(dataSource, rules, validGranularities):
    for rule in rules:
        if "Period" in rule.keys() and "segmentGranularity" in rule.keys() and "queryGranularity" in rule.keys():
            if rule["segmentGranularity"] in validGranularities.keys() and rule["queryGranularity"] in validGranularities.keys():
                m = re.search('^P\d+[M|D|Y]$', rule["Period"])
                if m is None:
                    print("Invalid Period of {0}".format(rule["Period"]));
                    exit(-1)
            else:
                print("segmentGranularity and queryGranularity must be one of {0}".format(",".join(validGranularities)));
                exit(-1);
        else:
            print("All rules must contain Period, segmentGranularity, and queryGranularity");
            exit(-1);

Load_EnvVars();
Load_Agrs();

zk = KazooClient(hosts=zookeeper);
zk.start();
try:
    zk.create("/druidAutoRollupCoordinator", acl=None, ephemeral=True, sequence=False, makepath=False);
except NodeExistsError:
    print("Another node is already the leader stopping now");
    zk.stop();
    exit(0);

print("Got leadership running tasks now");

for rule in rules.keys():
    validateRollupRules(rule, rules[rule], validGranularities);

supervisors = submitGetToDruid(druidRouters.split(','), "/druid/indexer/v1/supervisor?full");
if supervisors is False:
    zk.stop();
    exit(-1);

supervisors = json.loads(supervisors);
supervisorSpecInfo = {};

for supervisor in supervisors:
   if "id" in supervisor.keys():
       if "spec" in supervisor.keys() and "dataSchema" in supervisor["spec"].keys():
           dataSchema = supervisor["spec"]["dataSchema"];
           if "dataSource" in dataSchema.keys() and "metricsSpec" in dataSchema.keys() and len(dataSchema["metricsSpec"]) > 0:
               if "parser" in dataSchema.keys() and "parseSpec" in dataSchema["parser"].keys() and "dimensionsSpec" in dataSchema["parser"]["parseSpec"].keys() and "dimensions" in dataSchema["parser"]["parseSpec"]["dimensionsSpec"].keys() and "timestampSpec" in dataSchema["parser"]["parseSpec"].keys() and len(dataSchema["parser"]["parseSpec"]["dimensionsSpec"]["dimensions"]) > 0:
                   supervisorSpecInfo[supervisor["id"]] = {};
                   supervisorSpecInfo[supervisor["id"]]["dataSource"] = dataSchema["dataSource"];
                   supervisorSpecInfo[supervisor["id"]]["metricsSpec"] = dataSchema["metricsSpec"];
                   supervisorSpecInfo[supervisor["id"]]["dimensions"] = dataSchema["parser"]["parseSpec"]["dimensionsSpec"]["dimensions"];
                   supervisorSpecInfo[supervisor["id"]]["timestampSpec"] = dataSchema["parser"]["parseSpec"]["timestampSpec"];
               elif "dimensionsSpec" in dataSchema.keys() and dataSchema["dimensionsSpec"] is not None and "dimensions" in dataSchema["dimensionsSpec"].keys() and dataSchema["dimensionsSpec"]["dimensions"] is not None and "timestampSpec" in dataSchema.keys() and len(dataSchema["dimensionsSpec"]["dimensions"]) > 0:
                   supervisorSpecInfo[supervisor["id"]] = {};
                   supervisorSpecInfo[supervisor["id"]]["dataSource"] = dataSchema["dataSource"];
                   supervisorSpecInfo[supervisor["id"]]["metricsSpec"] = dataSchema["metricsSpec"];
                   supervisorSpecInfo[supervisor["id"]]["dimensions"] = dataSchema["dimensionsSpec"]["dimensions"];
                   supervisorSpecInfo[supervisor["id"]]["timestampSpec"] = dataSchema["timestampSpec"];

if len(supervisorSpecInfo.keys()) <= 0:
    print("No running supervisors found");
    zk.stop();
    exit(0);

tasks = submitGetToDruid(druidRouters.split(','), "/druid/indexer/v1/tasks?type=index_parallel");

if tasks is False:
    zk.stop();
    exit(-1);

tasks = json.loads(tasks);
taskInfo = {};
runningIntervalList = {};

for task in tasks:
    if "status" in task.keys() and "id" in task.keys():
        if task["status"] in [ "RUNNING", "PENDING", "WAITING" ]:
            taskInfo[task["id"]] = {};
    else:
        print("A Task is missing a status or id failing as we have somehow got unreliable info: {0}".format(task));
        zk.stop();
        exit(-1);

for task in taskInfo.keys():
    taskPayload = submitGetToDruid(druidRouters.split(','), "/druid/indexer/v1/task/{0}".format(task));
    if taskPayload is False:
        print("Could not verify task info");
        zk.stop();
        exit(-1);
    taskPayload = json.loads(taskPayload);
    if "payload" in taskPayload.keys() and "spec" in taskPayload["payload"].keys():
        payloadSpec = taskPayload["payload"]["spec"];
        if "dataSchema" in payloadSpec.keys() and "dataSource" in payloadSpec["dataSchema"].keys() and "granularitySpec" in payloadSpec["dataSchema"].keys() and "intervals" in payloadSpec["dataSchema"]["granularitySpec"].keys() and len(payloadSpec["dataSchema"]["granularitySpec"]["intervals"]) == 1 and "ioConfig" in payloadSpec.keys() and "inputSource" in payloadSpec["ioConfig"].keys() and "interval" in payloadSpec["ioConfig"]["inputSource"].keys():
                if payloadSpec["dataSchema"]["granularitySpec"]["intervals"][0] == payloadSpec["ioConfig"]["inputSource"]["interval"]:
                    if payloadSpec["dataSchema"]["dataSource"] not in runningIntervalList.keys():
                        runningIntervalList[payloadSpec["dataSchema"]["dataSource"]] = [];
                    runningIntervalList[payloadSpec["dataSchema"]["dataSource"]].append(payloadSpec["ioConfig"]["inputSource"]["interval"]);
                else:
                    print("Detected a task with mismatched intervals");
                    zk.stop();
                    exit(-1);
        else:
            print("Task payload with missing data found");
            zk.stop();
            exit(-1);
    else:
        print("Task payload with missing data found");
        zk.stop();
        exit(-1);

#Rules are evaluated top down and execute based on first match for each interval. The rules uses the segmentGranularity to determine if a rollup should be ran for each day in the interval its checking. Valid segmentGranularity options are NONE, SECOND, MINUTE, FIFTEEN_MINUTE, THIRTY_MINUTE, HOUR, DAY. This limits the ability to roll data up beyond 1 day granularity though in most cases 1 day should be sufficent.

for supervisor in supervisorSpecInfo.keys():
    currentSupervisor = supervisorSpecInfo[supervisor];
    currentRules = rules["_default"];
    
    if currentSupervisor["dataSource"] in rules.keys() and len(rules[currentSupervisor["dataSource"]]) > 0:
        currentRules = rules[currentSupervisor["dataSource"]]
    
    currentEndDateTime = datetime.datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0);
    lastDuration = isodate.parse_duration("P0D");
    
    for rule in currentRules:
        print("{0}: Running {1} with segmentGranularity: {2} and queryGranularity: {3}".format(currentSupervisor["dataSource"], rule["Period"], rule["segmentGranularity"], rule["queryGranularity"]));
        currentDuration = isodate.parse_duration(rule["Period"]);
        currentStartDateTime = currentEndDateTime - (currentDuration - lastDuration);
        lastDuration = currentDuration;
        if validGranularities[rule["segmentGranularity"]] is not None:
            segmentCounts = getSegmentCountsByDayInInterval(druidRouters.split(','), currentSupervisor["dataSource"], currentStartDateTime.strftime("%Y-%m-%d"), currentEndDateTime.strftime("%Y-%m-%d"));
            daysMeetingRollRuleReq = [];
            for segmentDate in segmentCounts.keys():
                if segmentCounts[segmentDate] > validGranularities[rule["segmentGranularity"]]:
                    daysMeetingRollRuleReq.append(segmentDate);
            daysNotRunningTasks = [];
            for dayMeetingRollRuleReq in daysMeetingRollRuleReq:
                dayMeetingRollRuleReqDate = isodate.parse_datetime(dayMeetingRollRuleReq + "T00:00:00Z");
                hasRunningTask = False;
                if currentSupervisor["dataSource"] in runningIntervalList.keys():
                    for runningDate in runningIntervalList[currentSupervisor["dataSource"]]:
                        runningDates = runningDate.split('/');
                        if len(runningDates) is not 2:
                            print("Found interval({0}) for {1} that is improperly formatted".format(currentSupervisor["dataSource"], runningDate));
                            zk.stop();
                            exit(-1);
                        runningTaskStartDateTime = isodate.parse_datetime(runningDates[0]);
                        runningTaskEndDateTime = isodate.parse_datetime(runningDates[1]);
                        if dayMeetingRollRuleReqDate >= runningTaskStartDateTime and dayMeetingRollRuleReqDate <= runningTaskEndDateTime:
                            print("{0} already has a running task for {1}".format(currentSupervisor["dataSource"], dayMeetingRollRuleReq));
                            hasRunningTask = True;
                            break;
                if hasRunningTask is False:
                    daysNotRunningTasks.append(dayMeetingRollRuleReq);
            for dayToSubmit in daysNotRunningTasks:
                intervalStartDate = isodate.parse_datetime(dayToSubmit + "T00:00:00Z");
                intervalEndDate = intervalStartDate + isodate.parse_duration("P1D");
                intervalString = intervalStartDate.strftime("%Y-%m-%dT%H:%M:%S") + "/" + intervalEndDate.strftime("%Y-%m-%dT%H:%M:%S");
                taskSpec = getTaskTemplate();
                taskSpec["spec"]["ioConfig"]["inputSource"]["dataSource"] = currentSupervisor["dataSource"];
                taskSpec["spec"]["ioConfig"]["inputSource"]["interval"] = intervalString;
                taskSpec["spec"]["dataSchema"]["dataSource"] = currentSupervisor["dataSource"];
                taskSpec["spec"]["dataSchema"]["granularitySpec"]["queryGranularity"] = rule["queryGranularity"];
                taskSpec["spec"]["dataSchema"]["granularitySpec"]["segmentGranularity"] = rule["segmentGranularity"];
                taskSpec["spec"]["dataSchema"]["granularitySpec"]["intervals"] = [ intervalString ];
                taskSpec["spec"]["dataSchema"]["timestampSpec"] = currentSupervisor["timestampSpec"];
                taskSpec["spec"]["dataSchema"]["dimensionsSpec"]["dimensions"] = currentSupervisor["dimensions"];
                taskSpec["spec"]["dataSchema"]["metricsSpec"] = currentSupervisor["metricsSpec"];
                submitPayLoadToDruid(druidRouters.split(','), "/druid/indexer/v1/task", taskSpec);
                print("Submitted rollup for {0}({1})".format(currentSupervisor["dataSource"], dayToSubmit));
        currentEndDateTime = currentStartDateTime;

zk.stop();
exit(0);
