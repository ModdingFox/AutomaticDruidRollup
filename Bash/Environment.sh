#!/bin/bash

shopt -s expand_aliases

export EnvironmentRoot="/home/e0180009/Desktop/DR/DruidRollup/AutomaticDruidRollup"
export PYTHONPATH="${EnvironmentRoot}/Environment:${EnvironmentRoot}/Zookeeper"
export druidConfigPath="${EnvironmentRoot}/Environment/druidEnvironment.json"

alias RunDruidRollUps="${EnvironmentRoot}/Bash/RunDruidRollUp.sh"
