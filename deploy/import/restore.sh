#!/bin/sh

SOURCE_TS_VERSION=2.7.2
TARGET_TS_VERSION=2.9.1
export TARGET_CONTAINER=deploy-timescaledb_new-1


# first, downgrade the extension to the backuped version
ALTER EXTENSION timescaledb UPDATE TO '2.9.0';
ALTER EXTENSION timescaledb UPDATE TO '2.8.1';
ALTER EXTENSION timescaledb UPDATE TO '2.8.0';
ALTER EXTENSION timescaledb UPDATE TO '2.7.2';


# prepare for restore
SELECT timescaledb_pre_restore();

# do the restore
cd ~/beefy/snapshots
docker run --rm --network container:$TARGET_CONTAINER -e PGPASSWORD=$PGPASSWORD -u root -w /data -v $PWD:/data timescale/timescaledb-ha:pg14.6-ts2.9.1-latest pg_restore -h timescaledb_new -p 5432 -U beefy -Fc -v -d beefy beefy.2023-01-14.14-53-46.pg_dump.bak 


SELECT timescaledb_post_restore();


# then, upgrade the extension to the target version
ALTER EXTENSION timescaledb UPDATE;