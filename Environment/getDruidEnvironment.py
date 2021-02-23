#!/bin/python3
import json;

def loadEnvironment(filePath, environmentName):
    zookeeper = None;
    rootZNode = None;
    
    environmentFile = open(filePath, 'r');
    jsonData = json.load(environmentFile);
    
    if environmentName in jsonData.keys():
        selectedEnvironment = jsonData[environmentName];
        if 'Zookeeper' in selectedEnvironment.keys():
            zookeeper = selectedEnvironment['Zookeeper'];
        else:
            print("Missing key {0} in environment config".format('Zookeeper'));
            exit(-1);
         
        if 'RootZNode' in selectedEnvironment.keys():
            rootZNode = selectedEnvironment['RootZNode'];
        else:
            print("Missing key {0} in environment config".format('RootZNode'));
            exit(-1);     
    else:
        print("Error could not find an environment named {0}".format(environmentName));
        exit(-1);
    
    return zookeeper, rootZNode
