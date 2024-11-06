# Library data analysis

This repository represent a simplified version of data processing projects written in Python with data processing engine and a medallion-like architecture.  
It represents combination of data science, written in notebooks, and data engineer and it should describe (through code) process of generating dataset, starting with ingestion all the way to the model training.

Repository contains:
1) **data** - local representation of [medallion architecture] (https://www.databricks.com/glossary/medallion-architecture) (bronze/silver/gold layers).
2) **model** - saved model.
3) **notebook** - notebooks for data analysis and model training.
4) **data processing jobs** - applications processing data using Polars.

# Getting Started

## Prerequisites

Requirements for running CLI:

* Python 3.11 (>= 3.10, <= 3.12)
* Poetry
* Python virtual environment (virtualenv, pyenv)

## Local setup

1) Install Poetry - `curl -sSL https://install.python-poetry.org | python3 -`
2) Clone this repository
```sh
$ git clone https://github.com/predrag-njegovanovic/library-analysis.git
```
3) Create virtual environment and activate it
4) Install python libraries
```sh
$ poetry install
```
5) Inside `storage` directory create `bronze`, `silver` and `gold` folders.

Project is setup in a way that no additional changes should be needed for a local run. Project configuration is stored inside `src/config/settings.toml` and it's adapted for default (local) environment, but there are options for extensions.  

## The idea

The main application abstractions are defined in the `common.py` module and they are used for creating processing pipelines. 
There are three pipelines which are moving data through storage layers thus making it more "usable".  
Those three layers are:
1) Ingestion,
2) Transformation and
3) Aggregation

And applications are conceived in that way.  

Notebooks are there to simulate data science workload such as data analysis, feature analysis and model training.

## CLI

After poetry installation, the script with an entrypoint is installed with the project. This creates a 'symlink' for the CLI activation.

Running ingestion:
```sh
lt ingest
```

Running processing:
```sh
lt process
```

After these two commands data should be in the `bronze` layer and `silver` layer.  

**Only after this, notebooks can be run.**

To create dataset run
```sh
lt create-dataset
```

All commands have `--help` option which shows the arguments.  

There is also a command for getting predictions for customers and books and their late returns.
This is implemented to mimick real implementation and it's very basic version of model serving.

```sh
lt predict --customer-id <customer_id> --book-id <book_id>
```