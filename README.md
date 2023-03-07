## Presentation

Beefy Dashboard is an opinionated multi-chain blockchain data importer that tracks user investment value over time.

### Functional features:

- imports all beefy products data
- auto-discover new products are they are added
- exposes historical data through an api
- optimized for low manual maintenance and low hardware requirements
- aim is to deliver raw historical data, not a full analysis

### Technical features:

- low level, reusable building blocks
- fine grained request batching
- fine grained RPC limits handling (to configure batching)
- support for multiple RPC endpoint per chain
- remembers already imported ranges with no data
- retries failed requests automatically
- ability to add new projects as needed or products

## Installation

### Requirements

To install and use this project, you will need to have the following software installed on your computer:

- [Node.js](https://nodejs.org)
- [npm](https://www.npmjs.com)
- [Docker](https://www.docker.com)

### Steps

Switch to the proper node version:

```bash
nvm use $(cat .nvmrc)
```

Then, run the following command to install the necessary dependencies:

```bash
npm install
```

Then, create an `.env` file in the root directory:

```bash
cp .env.sample .env
```

Then, fill in the `.env` file with the appropriate values. Plz find the configuration details in the src/utils/config.ts file.

Map local volumes to docker volumes

```bash
mkdir -p ${PWD}/data/db/timescaledb
docker volume create --driver local --opt type=none --opt device=${PWD}/data/db/timescaledb --opt o=bind beefy_timescaledb_data_directory
mkdir -p ${PWD}/data/db/grafana
docker volume create --driver local --opt type=none --opt device=${PWD}/data/db/grafana --opt o=bind beefy_grafana_data_directory
mkdir -p ${PWD}/data/db/grafana_plugins
docker volume create --driver local --opt type=none --opt device=${PWD}/data/db/grafana_plugins --opt o=bind beefy_grafana_plugins_directory
```

Depending on your docker setup, you might need to set the right permissions on those folders:

```bash
chmod <permission> ${PWD}/data/db/timescaledb
chmod <permission> ${PWD}/data/db/grafana
chmod <permission> ${PWD}/data/db/grafana_plugins
```

Start the docker containers (db, redis, etc):

```bash
npm run infra:start
```

Finally, apply db migrations:

```bash
npm run db:migrate
```

Database migrations refer to the process of managing and transforming the structure and/or data of a database. This is often done as part of the development process for a software application, where changes to the underlying database are required to accommodate new features or to fix bugs. Migrations typically involve writing code to modify the structure of the database, such as by adding new tables or columns, and to move or transform data from one format to another. The goal of database migrations is to ensure that the database is kept in a consistent and correct state, and that changes to the database are managed in a way that is safe and reversible.

## Usage

### Indexer

Finally, run an import command:

```bash
LOG_LEVEL=trace node -r ts-node/register ./src/script/run.ts beefy:run --task recent --chain ethereum
```

More command and option doc available:

```bash
node -r ts-node/register ./src/script/run.ts --help
node -r ts-node/register ./src/script/run.ts beefy:run --help
```

### API

Start the API:

```bash
LOG_LEVEL=trace npx ts-node ./src/api/api.ts
```

Then, you can access the API at http://localhost:3001.

Since the API is heavily using redis as a cache, you might want to clear redis cache before running the api again.

```bash
npm run redis:clear; LOG_LEVEL=trace npx ts-node ./src/api/api.ts
```

## Common actions

### Run unit tests

Unit tests use Jest. To run the unit tests, run the following command:

```bash
npm run test

npm run test -t 'multiplex' --watch
```

### Add a new RPC

Making a new RPC available for the importer means we add it to the rpc-limitations.json file:

```bash
LOG_LEVEL=debug node -r ts-node/register -r dotenv/config ./src/script/find-out-rpc-limitations.ts -c optimism -r https://optimism-mainnet.infura.io/v3/xxxxxx -w true -d true
LOG_LEVEL=debug node -r ts-node/register -r dotenv/config ./src/script/find-out-rpc-limitations.ts --help
```

You might also want to update the `src/utils/rpc/remove-secrets-from-rpc-url.ts` and `src/utils/rpc/remove-secrets-from-rpc-url.test.ts` to avoid committing any secret.

### How to add a new chain?

- Update the chain enum in the `src/types/chain.ts` file
- Run `npx ncu --upgrade blockchain-addressbook` and `npm install` to get the wtoken address
- Run `npm run build` and solve any remainning typescript error
- Add a new RPC using the above guide

### Update a grafana dashboard

Grafana dashboards are stored in the `deploy/db/dashboards` folder. To update a dashboard, import it in grafana, make the changes you want, then export it and replace the existing dashboard file.

### Build the docker image locally

```bash
docker build -t beefy-data-importer -f ./deploy/import/Dockerfile ./
```

### Grafana get locked out of his own db

When the db returns too much data, the sqlite db gets locked. https://community.grafana.com/t/database-is-locked-unable-to-use-grafana-anymore/16557

```bash
docker exec --user root -it deploy-grafana-1 apk add sqlite3
docker exec -it -w /var/lib/grafana deploy-grafana-1 /bin/sh

sqlite3 grafana.db '.clone grafana-new.db'
mv grafana.db grafana-old.db
mv grafana-new.db grafana.db

docker compose -f deploy/docker-compose.yml restart grafana
```

## Internals

### Moving parts

Import/indexing:

- [redis](https://redis.io/) import cache
- [timescaledb](https://www.timescale.com/) database
- import workers (src)

User facing

- [redis](https://redis.io/): provides a cache for the api
- [fastify](https://www.fastify.io/) api

Operations

- [grafana](https://grafana.com/): ingestion monitoring, quick prototyping

### FAQ

#### Why is this using typescript?

- Types definitions are our first layer of tests
- More difficult to read/write, but less stupid mistakes

#### Why is this using rxjs?

- It's a tradeoff, the code is more complex but it makes creating reusable building blocks way easier. And we can fine tune batching, throttling, etc.
- Think of rxjs as a stream++ lib (it's not, but it's easier to think of it that way)

#### Where are db migrations?

- Not implemented yet

#### Why timescaledb?

- It's a time series database, it's made for this kind of data
- Since we are optimizing for cost (running and maintenance) it's a good fit compared to vendor specific solutions
- It's postgresql based, so we're in a familiar territory

## Contributing

If you would like to contribute to the project, please fork this repository and make any desired changes. Then, submit a pull request and we will review your changes.

Thank you for your interest in contributing to the Blockchain Product Index!
