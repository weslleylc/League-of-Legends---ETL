# League-of-Legends - A pyspark ETL

The objective of this repository is to prototype an ETL process to extract useful information from different tables stored in CSV, and then save the files in Parquet format. Our datasets are based on the famous game League of Legends and contain statistics from ranked games in 2020. At the end of this process, we will have tables containing information about the best player and the best builds for each champion. Programs like Blitz perform similar queries to help players to improve their performance.

## ETL Project Structure

The basic project structure is as follows:

```bash
root/
 |-- configs/
 |   |-- etl_config.json
 |-- dependencies/
 |   |-- logging.py
 |   |-- spark.py
 |   |-- utils.py
 |-- notebooks/
 |   |-- Spark-lol.ipynb
 |-- jobs/
 |   |-- etl_job.py
 |-- tests/
 |   |-- test_data/
 |   |-- | -- champions/
 |   |-- | -- players/
 |   |-- | -- build/
 |   |-- | -- build_first_item/
 |   |-- test_etl_job.py
 |   build_dependencies.sh
 |   packages.zip
 |   Pipfile
 |   Pipfile.lock
```

## Passing Configuration Parameters to the ETL Job
Change "YOUR_PATH" for you current user path:
```
{
  "input_path":{
    "match_data_path":"YOUR_PATH/test_match_data.csv",
    "itens_data_path":"YOUR_PATH/riot_item.csv",
    "champions_data_path":"YOUR_PATH/riot_champion.csv"
  },
  "output_path":{
    "players_data_path":"YOUR_PATH/players.parquet",
    "champions_data_path":"YOUR_PATH/champions.parquet",
    "build_first_item_data_path":"YOUR_PATH/build_first_item.parquet",
    "build_data_path":"YOUR_PATH/build.parquet"
  }
}
```

## Running the tests
```
spark-submit --master local[*] --py-files packages.zip tests/test_etl_job.py
```

## Running the ETL job

Local:
```bash
spark-submit \
--master local[*] \
--py-files packages.zip \
--files configs/etl_config.json \
jobs/etl_job.py
```

