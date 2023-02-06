#!/bin/sh

# crontab -e
# 0 5 * * * sh -c 'date > /home/backup.log; export PGPASSWORD="xxx"; /home/beefy-bi/deploy/import/backup_file.sh 2>&1 >> /home/backup.log'

# if nothing happens
# sudo apt-get install postfix
# -> local only
# sudo tail -f /var/mail/<user>


BACKUP_DIR=~/beefy/snapshots
cd $BACKUP_DIR
FILE_NAME=$(ls -t beefy.*.pg_dump | tail -n 1)

ncftpput -u $FTP_USER -p $FTP_PASSWORD $FTP_HOST ~/beefy/snapshots $FILE_NAME
cd -