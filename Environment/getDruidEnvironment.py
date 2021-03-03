#!/bin/python3
import json;

def loadEnvironment(filePath, environmentName):
    zookeeper = None;
    rootZNode = None;
    configRootZNode = None;
    
    environmentFile = open(filePath, 'r');
    jsonData = json.load(environmentFile);
    
    if environmentName in jsonData.keys():
        selectedEnvironment = jsonData[environmentName];
        if 'Zookeeper' in selectedEnvironment.keys():
            zookeeper = selectedEnvironment['Zookeeper'];
         
        if 'RootZNode' in selectedEnvironment.keys():
            rootZNode = selectedEnvironment['RootZNode'];
        
        if 'ConfigRootZNode' in selectedEnvironment.keys():
            configRootZNode = selectedEnvironment['ConfigRootZNode'];
    else:
        print("Error could not find an environment named {0}".format(environmentName));
        exit(-1);
    
    return zookeeper, rootZNode, configRootZNode
