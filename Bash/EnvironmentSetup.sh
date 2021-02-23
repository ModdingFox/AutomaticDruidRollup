#!/bin/bash

ls EnvironmentSetup.sh
if [ $? -ne 0 ]; then
    echo "Could not find the EnvironmentSetup.sh in the current dir. Ensure when you run this script that your in the Bash dir."
    exit -1
fi

cd ..
Working_Directory="$(pwd)"
cd -
sed -ie "s|export EnvironmentRoot=\"\"|export EnvironmentRoot=\"${Working_Directory}\"|g" Environment.sh
cd ..

yum install -y python3.x86_64 python3-pip.noarch

python3 -m venv python3-virtualenv

source python3-virtualenv/bin/activate

pip3 install kazoo
pip3 install requests
pip3 install isodate
