#!/bin/bash

ls installSystemdService.sh
if [ $? -ne 0 ]; then
    echo "Could not find the installSystemdService.sh in the current dir. Ensure when you run this script that your in the Bash dir."
    exit -1
fi

cd ..
Working_Directory="$(pwd)"

read -p "Enter environment name: " druidEnvironment

cat > /usr/lib/systemd/system/druidRollup-${druidEnvironment}.service <<EOF
[Unit]
Description=Druid Rollup(${druidEnvironment})

[Service]
Environment="EnvironmentRoot=${Working_Directory}"
Environment="PYTHONPATH=${Working_Directory}/Environment:${Working_Directory}/Zookeeper"
Environment="druidConfigPath=${Working_Directory}/Environment/druidEnvironment.json"
Type=simple
ExecStart=/bin/bash ${Working_Directory}/Bash/RunDruidRollUp.sh "${druidEnvironment}"

[Install]
WantedBy=multi-user.target
EOF

cat > /usr/lib/systemd/system/druidRollup-${druidEnvironment}.timer <<EOF
[Unit]
Description=Druid Rollup Timer(${druidEnvironment})

[Timer]
OnUnitActiveSec=6h 

[Install]
WantedBy=timers.target
EOF

systemctl daemon-reload
systemctl enable druidRollup-${druidEnvironment}.timer
systemctl start druidRollup-${druidEnvironment}.timer

