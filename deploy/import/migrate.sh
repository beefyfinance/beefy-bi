#!/bin/sh


cd ~/beefy/beefy-bi
docker run --rm --network container:deploy-timescaledb-1 --env-file=.env -e PGPASSWORD=$PGPASSWORD -e LOG_LEVEL=trace -e WORK_CMD='./dist/src/script/run.js db:migrate' beefy-data-importer 
