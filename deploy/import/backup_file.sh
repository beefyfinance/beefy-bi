#!/bin/sh

# crontab -e
# 0 5 * * * sh -c 'date > /home/nftocean/beefy/backup.log; export FTP_USER="xxx"; export FTP_PASSWORD="xxx"; export FTP_HOST="xxx"; export PGPASSWORD="xxx"; /home/nftocean/beefy/beefy-bi/deploy/import/backup.sh 2>&1 >> /home/nftocean/beefy/backup.log'

# if nothing happens
# sudo apt-get install postfix
# -> local only
# sudo tail -f /var/mail/<user>

DB_CONTAINER=deploy-timescaledb-1
DOCKER_IMAGE=timescale/timescaledb-ha:pg14.4-ts2.7.2-latest
DB_HOST=timescaledb
DB_PORT=5432
DB_USER=beefy
DB_DB=beefy
BACKUP_DIR=~/beefy/snapshots

# rolling backup (keep 7 days)
BACKUP_FILE_NAME=$(beefy.`date +'%u'`.pg_dump.gz)

# backup server ssh name
BACKUP_SERVER=storage-box

cd $BACKUP_DIR
docker run --rm --network container:$DB_CONTAINER -e PGPASSWORD=$PGPASSWORD -u root -w /data -v $PWD:/data $DOCKER_IMAGE pg_dump -h $DB_HOST -p $DB_PORT -U $DB_USER -Fc $DB_DB | gzip > $BACKUP_FILE_NAME
cd -