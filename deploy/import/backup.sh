#!/bin/sh

cd ~/beefy/snapshots
docker run --rm --network container:deploy-timescaledb-1 -e PGPASSWORD=$PGPASSWORD -u root -w /data -v $PWD:/data timescale/timescaledb-ha:pg14.4-ts2.7.2-latest pg_dump -h timescaledb -p 5432 -U beefy -Fc -v -f beefy.`date +'%Y-%m-%d.%H-%M-%S'`.pg_dump.bak beefy
ncftpput -u $FTP_USER -p $FTP_PASSWORD $FTP_HOST ~/beefy/snapshots `ls beefy.*.bak | tail -n 1`
