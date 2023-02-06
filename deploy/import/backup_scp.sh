#!/bin/sh

# crontab -e
# 0 5 * * * sh -c 'date > /home/nftocean/beefy/backup.log; export FTP_USER="xxx"; export FTP_PASSWORD="xxx"; export FTP_HOST="xxx"; export PGPASSWORD="xxx"; /home/nftocean/beefy/beefy-bi/deploy/import/backup.sh 2>&1 >> /home/nftocean/beefy/backup.log'

# if nothing happens
# sudo apt-get install postfix
# -> local only
# sudo tail -f /var/mail/<user>

TARGET_DIR=/home/data-storage/beefy-bi-backups
BACKUP_SSH_NAME=storage-box


BACKUP_DIR=~/beefy/snapshots
cd $BACKUP_DIR
FILE_NAME=$(ls -t beefy.*.bak | tail -n 1)

rsync -v -e "ssh $BACKUP_SSH_NAME"
cd -
