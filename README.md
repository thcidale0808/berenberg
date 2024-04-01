# Berenberg Trading Metrics

## Overview
This repository contains the code of the Berenberg Trading Metrics Calculator.

## Setup
Instal dependencies by running the following command:

`pip install -r requirements.txt`

These environment variables can be set to configure the Trading Metrics Tool:

* EXECUTIONS_FILE_PATH: Path to the executions data file.
* REFDATA_FILE_PATH: Path to the reference data file.
* MARKETDATA_FILE_PATH: Path to the market data file.
* OUTPUT_FILE_PATH: Path where the output file will be saved.

If this is not set, the default values are:
* EXECUTIONS_FILE_PATH: 'data/executions.csv'
* REFDATA_FILE_PATH: 'data/refdata.csv'
* MARKETDATA_FILE_PATH: 'data/marketdata.csv'
* OUTPUT_FILE_PATH: 'output.csv'


## Execution
To run the Trading Metrics Tool, navigate to the directory containing `berenberg.py` and execute:
```sh
python berenberg.py
```

