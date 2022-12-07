# Presentation

Multi-chain blockchain data importer.
Download blockchain data from rate limited rpcs locally to be analyzed further.

## Requirements

To install and use this project, you will need to have the following software installed on your computer:

- [Node.js](https://nodejs.org)
- [npm](https://www.npmjs.com)
- [Docker](https://www.docker.com)

## Installation

To install and use this project, you will need to have [Node.js](https://nodejs.org) and [npm](https://www.npmjs.com) installed on your computer.

Once you have Node.js and npm installed, clone this repository to your local machine and navigate to the project directory.

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

Start the docker containers (db, redis, etc):

```bash
npm run infra:start
```

Finally, apply db migrations:

```bash
npm run db:migrate
```

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
export USE_DEFAULT_LIMITATIONS_IF_NOT_FOUND=true
LOG_LEVEL=debug node -r ts-node/register ./src/script/find-out-rpc-limitations.ts -c optimism -r https://optimism-mainnet.infura.io/v3/xxxxxx -w
LOG_LEVEL=debug node -r ts-node/register ./src/script/find-out-rpc-limitations.ts --help
```

You might also want to update the `src/utils/rpc/remove-secrets-from-rpc-url.ts` and `src/utils/rpc/remove-secrets-from-rpc-url.test.ts` to avoid committing any secret.

### Update a grafana dashboard

Grafana dashboards are stored in the `deploy/db/dashboards` folder. To update a dashboard, import it in grafana, make the changes you want, then export it and replace the existing dashboard file.

## Contributing

If you would like to contribute to the project, please fork this repository and make any desired changes. Then, submit a pull request and we will review your changes.

Thank you for your interest in contributing to the Blockchain Product Index!
