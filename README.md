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

Outuput
Table Champions Win Rate
```

                           +-------------+-----------+-------------+-------------------+
                           |name_champion|won_matches|total_matches|           win_rate|
                           +-------------+-----------+-------------+-------------------+
                           |      Lee Sin|      17527|        35226| 0.4975586214727758|
                           |       Ezreal|      14920|        30133|0.49513822055553713|
                           | Miss Fortune|      14834|        28829| 0.5145513198515383|
                           |       Thresh|      12454|        24663| 0.5049669545472976|
                           |       Lucian|      11778|        24157| 0.4875605414579625|
                           |         Sett|      11056|        21694| 0.5096340001843828|
                           |       Kai'Sa|      10053|        21041|0.47778147426453116|
                           |     Aphelios|      10259|        20247| 0.5066923494838742|
                           |        Senna|      10365|        20217| 0.5126873423356582|
                           |        Sylas|       9508|        19842|0.47918556597117223|
                           |         Ekko|       9846|        19618| 0.5018860230400652|
                           |        Elise|       9553|        18065| 0.5288126210905065|
                           |     Nautilus|       8191|        16555|0.49477499244941103|
                           |       Irelia|       7377|        15885| 0.4644003777148253|
                           |       Aatrox|       7703|        15657| 0.4919844159162036|
                           |      Rek'Sai|       7887|        15126|  0.521420071400238|
                           |         Bard|       7686|        14121| 0.5442957297641811|
                           |     Renekton|       7059|        14092| 0.5009225092250923|
                           |   Blitzcrank|       6650|        13302|0.49992482333483684|
                           |         Pyke|       6482|        13241| 0.4895400649497772|
                           +-------------+-----------+-------------+-------------------+
```

Table Champions Build
```
                                 +------------+--------------------+-------------+
                                 |championName|          first_item|total_matches|
                                 +------------+--------------------+-------------+
                                 |      Aatrox|      Doran's Shield|         2110|
                                 |        Ahri|     Hextech GLP-800|          801|
                                 |       Akali|    Hextech Gunblade|         2490|
                                 |     Alistar|Bulwark of the Mo...|          643|
                                 |       Amumu|Enchantment: Runi...|           36|
                                 |      Anivia|    Seraph's Embrace|          113|
                                 |       Annie|        Doran's Ring|          117|
                                 |    Aphelios|       Doran's Blade|         4572|
                                 |        Ashe|Blade of the Ruin...|         1594|
                                 |Aurelion Sol|   Corrupting Potion|          408|
                                 |        Azir|      Nashor's Tooth|          324|
                                 |        Bard|   Shard of True Ice|         2007|
                                 |  Blitzcrank|Pauldrons of Whit...|         1647|
                                 |       Brand|        Luden's Echo|           61|
                                 |       Braum|Pauldrons of Whit...|         1021|
                                 |     Caitlyn|       Doran's Blade|         1362|
                                 |     Camille|      Ravenous Hydra|         1210|
                                 |  Cassiopeia|    Seraph's Embrace|         2017|
                                 |    Cho'Gath|     Hextech GLP-800|          447|
                                 |       Corki|       Trinity Force|          125|
                                 +------------+--------------------+-------------+
```


