#!/bin/python3

import re
import getopt, sys
from kazoo.client import KazooClient
import json

def getRollupRules(zookeeperHostsIn, zNodePath):
    zk = KazooClient(hosts=zookeeperHostsIn);
    zk.start();
    result = {};
    
    if zk.exists(zNodePath):
        for zookeeperChild in zk.get_children(zNodePath):
            zookeeperChildPath = zNodePath + "/" + zookeeperChild
            if zk.exists(zookeeperChildPath):
                Data, Stat = zk.get(zookeeperChildPath)
                result[zookeeperChild] = json.loads(Data)
            else:
                print("Reported ZK path no longer exists: " + Zookeeper_Child_Path)
    
    zk.stop();
    return result;
