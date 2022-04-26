# Benthos - Value Investment Research Tools

This project is the backend code for our project called Benthos - an investment research tool for fundamental stock analysis. This project is a combined effort (under construction) built by myself and [@daniellediloreto](https://github.com/daniellediloreto), who's background is in investment analytics.

The stock price and company fundamental data is sourced from [SimFin](https://simfin.com/) and [Alpha Vantage](https://www.alphavantage.co/). Simfin is the source for all pre-2020 data for US companies. The Alpha Vantage data provides current fundamental data, and recent data is added to the Postgres database from this source as it becomes available.


## Table of Contents
1. [Setup](#1-setup)
2. [Configuration](#2-configuration)

## 1. Setup

First, clone the repo using the command:
```shell
git clone git@github.com:h-morgan/benthos-cli.git
```

Move into the project directory:
```shell
cd benthos-cli
```

Next, intialize a virtual environment to store project dependencies. The following command uses the `venv` Python command to intialize a virtual environment called `venv`:
```shell
python3 -m venv venv
```

Then, install project dependencies in the `venv` using the following command:
```shell
pip install -r requirements.txt
```

## 2. Configuration

This project requies a `.env` file to store environment variables and configuations. Create a `.env` file in the `src/` directory with the following contents:

```
DB_USER=username
DB_PASSWORD=password
DB_HOST=0.0.0.0
DB_PORT=5432
DB_NAME=invest-db
```

In order to use use the project, an API key from [Alpha Vantage](https://www.alphavantage.co/) is needed. as described above. Once a free API key is obtained, the value needs to be stored in the `.env` file as shown above.

The database connection values need to be filled in to connect to the database I have running on a [Digital Ocean](https://www.digitalocean.com/) server droplet. Contact me for connection details if interested.
