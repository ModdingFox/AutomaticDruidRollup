#!/bin/python3

import re
import getopt, sys
from kazoo.client import KazooClient
import json

ZOOKEEPER_HOSTS=None
DISCOVERY_ROOT="/druid/discovery"
SERVICE_NAME="druid"
TARGET=None

def Print_Command_Help( ):
    print("Usage:")
    print("--ZOOKEEPER_HOSTS=192.168.1.100:2181,192.168.1.101:2181,192.168.1.102:2181 - Location of zookeeper to use")
    print("--DISCOVERY_ROOT=\"/druid/discovery\" - Overrides the default druid discovery path")
    print("--SERVICE_NAME=\"druid\" - Overrides the default service name")
    print("--TARGET=[Broker, Coordinator, Overlord, or Router] - Select the target for the request")
    return

def Load_Agrs( ):
    global ZOOKEEPER_HOSTS
    global DISCOVERY_ROOT
    global SERVICE_NAME
    global TARGET
    
    fullCmdArguments = sys.argv
    argumentList = fullCmdArguments[1:]
    
    unixOptions = "ho:v"
    Command_Line_Options = [ "ZOOKEEPER_HOSTS=", "DISCOVERY_ROOT=", "SERVICE_NAME=", "TARGET=" ]
    
    try:
        arguments, values = getopt.getopt(argumentList, unixOptions, Command_Line_Options)
    except getopt.error as err:
        print (str(err))
        sys.exit(1)
    
    for currentArgument, currentValue in arguments:
        if currentArgument in ("--ZOOKEEPER_HOSTS"):
            ZOOKEEPER_HOSTS=currentValue
        elif currentArgument in ("--DISCOVERY_ROOT"):
            DISCOVERY_ROOT=currentValue
        elif currentArgument in ("--SERVICE_NAME"):
            SERVICE_NAME=currentValue
        elif currentArgument in ("--TARGET"):
            if currentValue in ("broker", "coordinator", "overlord", "router"):
                TARGET=currentValue.lower()
            else:
                print("Cannot use --TARGET must be one of broker, coordinator, overlord, or router")
                Print_Command_Help()
                sys.exit(-1)

                
    ## Check All Options Are Set
    if ZOOKEEPER_HOSTS is None:
        print("ZOOKEEPER_HOSTS is not set")
        Print_Command_Help()
        sys.exit(-1)
    elif TARGET is None:
        print("TARGET is not set")
        Print_Command_Help()
        sys.exit(-1)
    return

## Get Data From Zookeeper
def Get_Druid_Config( ZOOKEEPER_HOSTS_In, DISCOVERY_ROOT_In, SERVICE_NAME_In, TARGET_In ):
    Zookeeper_Path_In = DISCOVERY_ROOT_In + "/" + SERVICE_NAME_In + ":" + TARGET_In;
    
    zk = KazooClient(hosts=ZOOKEEPER_HOSTS_In)
    zk.start()
    Hosts_Found = []
    
    if zk.exists(Zookeeper_Path_In):
        for Zookeeper_Child in zk.get_children(Zookeeper_Path_In):
            Zookeeper_Child_Path = Zookeeper_Path_In + "/" + Zookeeper_Child
            if zk.exists(Zookeeper_Child_Path):
                Data, Stat = zk.get(Zookeeper_Child_Path)
                Host_Data = json.loads(Data)
                Hosts_Found.append(Host_Data['address'] + ":" + str(Host_Data['port']))
            else:
                print("Reported ZK path no longer exists: " + Zookeeper_Child_Path)
    else:
        print("Could not find active node root at: " + Zookeeper_Path_In)
    
    zk.stop()
    return ','.join(Hosts_Found)

if __name__=="__main__":
    Load_Agrs()
    print(Get_Druid_Config(ZOOKEEPER_HOSTS, DISCOVERY_ROOT, SERVICE_NAME, TARGET));
