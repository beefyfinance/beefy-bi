#!/bin/sh

# crontab -e
# 0 5 * * * sh -c 'date > /home/backup.log; export PGPASSWORD="xxx"; /home/beefy-bi/deploy/import/backup_file.sh 2>&1 >> /home/backup.log'

# if nothing happens
# sudo apt-get install postfix
# -> local only
# sudo tail -f /var/mail/<user>

DB_CONTAINER=deploy-timescaledb-1
DOCKER_IMAGE=timescale/timescaledb-ha:pg14.6-ts2.9.1-latest
DB_HOST=timescaledb
DB_PORT=5432
DB_USER=beefy
DB_DB=beefy
BACKUP_DIR=~/beefy-bi-backups

# rolling backup (keep 7 days)
BACKUP_FILE_NAME=beefy.`date +'%u'`.pg_dump.gz

# backup server ssh name
BACKUP_SERVER=storage-box

cd $BACKUP_DIR
docker run --rm --network container:$DB_CONTAINER -e PGPASSWORD=$PGPASSWORD -u root -w /data -v $PWD:/data $DOCKER_IMAGE pg_dump -h $DB_HOST -p $DB_PORT -U $DB_USER -Fc $DB_DB --exclude-table rpc_error_ts --exclude-table price_ts_cagg_1h --exclude-table price_ts_cagg_1d | gzip > $BACKUP_FILE_NAME
cd -