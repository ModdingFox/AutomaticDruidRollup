#!/bin/bash

source "${EnvironmentRoot}/python3-virtualenv/bin/activate"

python3 ${EnvironmentRoot}/Druid/rollUpTaskControl.py --druidConfigPath="${EnvironmentRoot}/Environment/druidEnvironment.json" --druidSelectedEnvironment="${1}"
exit $?

