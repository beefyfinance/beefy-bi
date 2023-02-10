#!/bin/sh

# crontab -e
# 0 5 * * * sh -c 'date > /home/backup.log; /home/beefy-bi/deploy/import/backup_file.sh 2>&1 >> /home/backup.log'

# if nothing happens
# sudo apt-get install postfix
# -> local only
# sudo tail -f /var/mail/<user>

BACKUP_DIR=~/beefy-bi-backups
BACKUP_SSH_NAME=storage-box
TARGET_DIR=/home/data-storage/beefy-bi-backups


cd $BACKUP_DIR
FILE_NAME=$(ls -t beefy.*.gz | tail -n 1)

# can't use rsync because target box is echoing some prompt on login https://serverfault.com/a/328404
#rsync -v -e 'ssh '$BACKUP_SSH_NAME ./ $TARGET_USER"@"$TARGET_HOST":"$TARGET_DIR
scp $FILE_NAME $BACKUP_SSH_NAME:"$TARGET_DIR/$FILE_NAME
cd -
