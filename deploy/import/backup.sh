#!/bin/sh

# crontab -e
# 0 5 * * * sh -c 'date > /home/nftocean/beefy/backup.log; export FTP_USER="xxx"; export FTP_PASSWORD="xxx"; export FTP_HOST="xxx"; export PGPASSWORD="xxx"; /home/nftocean/beefy/beefy-bi/deploy/import/backup.sh 2>&1 >> /home/nftocean/beefy/backup.log'

# if nothing happens
# sudo apt-get install postfix
# -> local only
# sudo tail -f /var/mail/<user>

cd ~/beefy/snapshots
docker run --rm --network container:deploy-timescaledb-1 -e PGPASSWORD=$PGPASSWORD -u root -w /data -v $PWD:/data timescale/timescaledb-ha:pg14.4-ts2.7.2-latest pg_dump -h timescaledb -p 5432 -U beefy -Fc -v -f beefy.`date +'%Y-%m-%d.%H-%M-%S'`.pg_dump.bak beefy
ncftpput -u $FTP_USER -p $FTP_PASSWORD $FTP_HOST ~/beefy/snapshots `ls beefy.*.bak | tail -n 1`
