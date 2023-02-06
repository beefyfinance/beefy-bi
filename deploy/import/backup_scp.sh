#!/bin/sh

# crontab -e
# 0 5 * * * sh -c 'date > /home/backup.log; export PGPASSWORD="xxx"; /home/beefy-bi/deploy/import/backup_file.sh 2>&1 >> /home/backup.log'

# if nothing happens
# sudo apt-get install postfix
# -> local only
# sudo tail -f /var/mail/<user>

TARGET_DIR=/home/data-storage/beefy-bi-backups
BACKUP_SSH_NAME=storage-box


BACKUP_DIR=~/beefy-bi-backups
cd $BACKUP_DIR

rsync -v -e "ssh $BACKUP_SSH_NAME" ./ $TARGET_DIR $TARGET_USER"@"$TARGET_HOST":"$TARGET_DIR
cd -
