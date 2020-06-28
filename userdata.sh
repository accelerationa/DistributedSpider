Content-Type: multipart/mixed; boundary="//"
MIME-Version: 1.0

--//
Content-Type: text/cloud-config; charset="us-ascii"
MIME-Version: 1.0
Content-Transfer-Encoding: 7bit
Content-Disposition: attachment; filename="cloud-config.txt"

#cloud-config
cloud_final_modules:
- [scripts-user, always]

--//
Content-Type: text/x-shellscript; charset="us-ascii"
MIME-Version: 1.0
Content-Transfer-Encoding: 7bit
Content-Disposition: attachment; filename="userdata.txt"

#!/bin/bash
set -e
exec > >(sudo tee /var/log/user-data.log|sudo logger -t user-data -s 2>/dev/console) 2>&1
cd /home/ubuntu

if [ -d "DistributedSpider" ]; then
    rm -rf DistributedSpider
fi
git clone https://github.com/accelerationa/DistributedSpider.git
cd ./DistributedSpider
make
FILE=accounts.json
if [ -f "$FILE" ]; then
    rm accounts.json
fi
cat >> accounts.json << EOF
{"mysql": {"ip": "54.70.196.132", "password": "Lyc135790!", "user": "spider"}}
EOF

BASE_NAME=4node10processrowlock
for PROCESS in {1..10} 
do 
nohup python ./src/worker.py --name $BASE_NAME$PROCESS > spider$PROCESS.out&
done
--//