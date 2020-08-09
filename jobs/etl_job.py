"""
etl_job.py
~~~~~~~~~~

This Python module contains an example Apache Spark ETL job definition
that implements best practices for production ETL jobs. It can be
submitted to a Spark cluster (or locally) using the 'spark-submit'
command found in the '/bin' directory of all Spark distributions
(necessary for running any Spark job, locally or otherwise). For
example, this example script can be executed as follows,

    $SPARK_HOME/bin/spark-submit \
    --master spark://localhost:7077 \
    --py-files packages.zip \
    --files configs/etl_config.json \
    jobs/etl_job.py

where packages.zip contains Python modules required by ETL job (in
this example it contains a class to provide access to Spark's logger),
which need to be made available to each executor process on every node
in the cluster; etl_config.json is a text file sent to the cluster,
containing a JSON object with all of the configuration parameters
required by the ETL job; and, etl_job.py contains the Spark application
to be executed by a driver process on the Spark master node.

For more details on submitting Spark applications, please see here:
http://spark.apache.org/docs/latest/submitting-applications.html

Our chosen approach for structuring jobs is to separate the individual
'units' of ETL - the Extract, Transform and Load parts - into dedicated
functions, such that the key Transform steps can be covered by tests
and jobs or called from within another environment (e.g. a Jupyter or
Zeppelin notebook).
"""
from pyspark.sql import SparkSession, SQLContext, Row
from pyspark.sql.functions import col, concat_ws, lit, udf, explode, flatten, explode_outer, concat
from pyspark.sql.types import ArrayType, IntegerType, StringType, StructType, StructField, DoubleType, LongType, MapType, BooleanType
import json
from dependencies.spark import start_spark
from dependencies.utils import flatten_df, transform_colum

def main():
    """Main ETL script definition.

    :return: None
    """
    # start Spark application and get Spark session, logger and config
    spark, log, config = start_spark(
       app_name='lol_etl_job',
       files=['configs/etl_config.json'])




    # log that main ETL job is starting
    log.warn('etl_job is up-and-running')

    # execute ETL pipeline
    print(config['input_path'])
    match_data, itens, champions = extract_data(spark, config['input_path'])
    players, champions, build_first_item, build = transform_data(spark, match_data, itens, champions)
    load_data(spark, players, champions, build_first_item, build, config['output_path'])

    # log the success and terminate Spark application
    log.warn('lol_etl_job is finished')
    spark.stop()
    return None


def extract_data(spark, input_path):
    """Load data from CSV file format.

    :param spark: Spark session object.
    :param input_path: dictionary with data paths.
    :return: Spark DataFrame.
    """
	# partial shcmea for match data
    match_schema = StructType(
		[
		    StructField('_c0', IntegerType(), True),
		    StructField('gameCreation', DoubleType(), True),
		    StructField('gameDuration', DoubleType(), True),
		    StructField('gameId', DoubleType(), True),
		    StructField('gameMode', StringType(), True),
		    StructField('gameType', StringType(), True),
		    StructField('gameVersion', StringType(), True),
		    StructField('mapId', DoubleType(), True),
		    StructField('participantIdentities', StringType(), True),
		    StructField('participants',  StringType(), True),
		    StructField('platformId', StringType(), True),
		    StructField('queueId', DoubleType(), True),
		    StructField('seasonId', DoubleType(), True),
		    StructField('status.message', StringType(), True),
		    StructField('status.status_code', StringType(), True)
		]
	)

	# shcmea for itens data
    itens_schema = StructType(
		[
		StructField('_c0', IntegerType(), True),
		StructField('item_id', IntegerType(), True),
		StructField('name', StringType(), True),
		StructField('upper_item', StringType(), True),
		StructField('explain', StringType(), True),
		StructField('buy_price', IntegerType(), True),
		StructField('sell_price', IntegerType(), True),
		StructField('tag', StringType(), True)
		]
	)

	# shcmea for champions data
    champions_schema = StructType(
		[
		StructField('_c0', IntegerType(), True),
		StructField('version', StringType(), True),
		StructField('id', StringType(), True),
		StructField('key', IntegerType(), True),
		StructField('name', StringType(), True),
		StructField('title', StringType(), True),
		StructField('blurb', StringType(), True),
		StructField('tags', StringType(), True),
		StructField('partype', StringType(), True),
		StructField('info.attack', IntegerType(), True),
		StructField('info.defense', IntegerType(), True),
		StructField('info.magic', IntegerType(), True),
		StructField('info.difficulty', IntegerType(), True),
		StructField('image.full', StringType(), True),
		StructField('image.sprite', StringType(), True),
		StructField('image.group', StringType(), True),
		StructField('image.x', IntegerType(), True),
		StructField('image.y', IntegerType(), True),
		StructField('image.w', IntegerType(), True),
		StructField('image.h', IntegerType(), True),
		StructField('stats.hp', DoubleType(), True),
		StructField('stats.hpperlevel', IntegerType(), True),
		StructField('stats.mp', DoubleType(), True),
		StructField('stats.mpperlevel', DoubleType(), True),
		StructField('stats.movespeed', IntegerType(), True),
		StructField('stats.armor', DoubleType(), True),
		StructField('stats.armorperlevel', DoubleType(), True),
		StructField('stats.spellblock', DoubleType(), True),
		StructField('stats.spellblockperlevel', DoubleType(), True),
		StructField('stats.attackrange', IntegerType(), True),
		StructField('stats.hpregen', DoubleType(), True),
		StructField('stats.hpregenperlevel', DoubleType(), True),
		StructField('stats.mpregen', DoubleType(), True),
		StructField('stats.mpregenperlevel', DoubleType(), True),
		StructField('stats.crit', IntegerType(), True),
		StructField('stats.critperlevel', IntegerType(), True),
		StructField('stats.attackdamage', DoubleType(), True),
		StructField('stats.attackdamageperlevel', DoubleType(), True),
		StructField('stats.attackspeedperlevel', DoubleType(), True),
		StructField('stats.attackspeed', DoubleType(), True),
		]
	)
    # read match data
    match_data = spark.read.csv(input_path["match_data_path"],
		             header='true',
		             schema=match_schema)

	# https://www.kaggle.com/tk0802kim/kerneld01a1ec7ad
    itens = spark.read.csv(input_path["itens_data_path"],
		             header='true',
		             schema=itens_schema)

    champions = spark.read.csv(input_path["champions_data_path"],
		             header='true',
		             schema=champions_schema)

    return match_data, itens, champions


def transform_data(spark, match_data, itens, champions):
    """Transform original dataset.
    :param spark: Spark session object.
    :param match_data: match_data DataFrame
    :param itens: itens DataFrame
    :param champions: champions DataFrame

    :return: 
	players: Dataframe
	champions: Dataframe 
	build_first_item: 
	Dataframe build: Dataframe
    """
    # initiate sqlContext for sql queries
    sqlContext = SQLContext(spark)
    # Rename columns with '.'
    match_data = match_data.withColumnRenamed("status.message", "status_message")
    match_data = match_data.withColumnRenamed("status.status_code", "status_status_code")

	# transform string rows into psypsark rows
    match_data, schema_partifipants = transform_colum(spark, match_data, "participants")
    match_data, schema_identities = transform_colum(spark, match_data, "participantIdentities")

	# here we have to array columns. Before flatten our dataset we need first concatanate this arrays into a single array, and them explode their rows. 
    combine = udf(lambda x, y: list(zip(x, y)),ArrayType(StructType([StructField("ids", schema_partifipants),
		                          StructField("info", schema_identities)]))
		        )
    match_data = match_data.withColumn("participants_info", combine("participants", "participantIdentities"))

	# remove the old columns
    columns_to_drop = ['participants', 'participantIdentities']
    match_data = match_data.drop(*columns_to_drop)
    match_data = match_data.withColumn("participants_info", explode("participants_info"))
	
	# flatten structs
    match_data=flatten_df(match_data)

	# get the dictionary with itens names and keys
    itens_dict = itens.select("item_id", "name").distinct().collect()
    itens_dict = {v["item_id"]:v["name"] for v in itens_dict}

	# help function to translate a item key into a item name
    def transform_itens(x):
	    try:
		    value = itens_dict[int(x)] 
	    except:
		    value = "Name Not Found"
	    return value


    new_cols_itens = udf(lambda x : transform_itens(x), StringType())

	# apply the translate function for each item column
    match_data = match_data.withColumn("name_item0", new_cols_itens(col("participants_info_ids_stats_item0")))
    match_data = match_data.withColumn("name_item1", new_cols_itens(col("participants_info_ids_stats_item1")))
    match_data = match_data.withColumn("name_item2", new_cols_itens(col("participants_info_ids_stats_item2")))
    match_data = match_data.withColumn("name_item3", new_cols_itens(col("participants_info_ids_stats_item3")))
    match_data = match_data.withColumn("name_item4", new_cols_itens(col("participants_info_ids_stats_item4")))
    match_data = match_data.withColumn("name_item5", new_cols_itens(col("participants_info_ids_stats_item5")))
    match_data = match_data.withColumn("name_item6", new_cols_itens(col("participants_info_ids_stats_item6")))


	# get the dictionary with champions names and keys

    champions_dict = champions.select("key", "name").distinct().collect()
    champions_dict = {v["key"]:v["name"] for v in champions_dict}

	# help function to translate a champion key into a champion name
    def transform_champions(x):
	    try:
		    value = champions_dict[int(x)] 
	    except:
		    value = "Name Not Found"
	    return value


    new_cols_champions = udf(lambda x : transform_champions(x), StringType())

	# apply the translate function for champion column
    match_data = match_data.withColumn("name_champion", new_cols_champions(col("participants_info_ids_championId")))

	# Register the DataFrame as a SQL temporary view
    match_data.createOrReplaceTempView("match_data")

	# SQL querrie to extract the victory stats for each  champion
    champions = sqlContext.sql("""
		                     SELECT victorys.name_champion as name_champion, victorys.won_matches, matches.total_matches, victorys.won_matches/matches.total_matches as win_rate \
		                     FROM \
		                        (SELECT match_data.name_champion as name_champion, COUNT(DISTINCT(match_data.gameId)) as won_matches \
		                        FROM match_data \
		                        WHERE match_data.participants_info_ids_stats_win == true \
		                        GROUP BY match_data.name_champion) as victorys \
		                     LEFT JOIN (SELECT match_data.name_champion as name_champion, COUNT(DISTINCT(match_data.gameId)) as total_matches \
		                              FROM match_data \
		                              GROUP BY match_data.name_champion) as matches \
		                     ON victorys.name_champion = matches.name_champion
		                     ORDER BY matches.total_matches DESC
		                  """) 
    champions.createOrReplaceTempView("champions")

	# SQL querrie to extract the victory stats for each  player with an especific champion
    players = sqlContext.sql("""
		                    SELECT victorys.id as user, victorys.name_champion as name_champion, victorys.won_matches, matches.total_matches, victorys.won_matches/matches.total_matches as win_rate \
		                    FROM \
		                       (SELECT match_data.participants_info_info_player_accountId as id, match_data.name_champion as name_champion, COUNT(DISTINCT(match_data.gameId)) as won_matches \
		                       FROM match_data \
		                       WHERE match_data.participants_info_ids_stats_win == true \
		                       GROUP BY match_data.participants_info_info_player_accountId, match_data.name_champion) as victorys \
		                    LEFT JOIN (SELECT match_data.participants_info_info_player_accountId as id, match_data.name_champion as name_champion, COUNT(DISTINCT(match_data.gameId)) as total_matches \
		                            FROM match_data \
		                            GROUP BY match_data.participants_info_info_player_accountId, match_data.name_champion) as matches \
		                    ON victorys.id=matches.id AND victorys.name_champion = matches.name_champion
		                    ORDER BY matches.total_matches DESC
		                """) 
    players.createOrReplaceTempView("players")


	# SQL querrie to extract the most common first iten for each champion

    connector = "-"
    build_first_item = sqlContext.sql("""
		                           SELECT build.championName, build.build_name as first_item, COUNT(build.build_name) as total_matches \
		                           FROM \
		                              (SELECT match_data.name_champion as championName, match_data.name_item0  as build_name \
		                              FROM match_data \
		                              WHERE match_data.participants_info_info_player_accountId \
		                              IN ( \
		                                  SELECT players.user
		                                  FROM players \
		                                  WHERE players.win_rate > 0.5 AND players.total_matches > 2)) as build \
		                           GROUP BY build.championName, build.build_name \
		                           ORDER BY total_matches DESC
		                        """)
    build_first_item = build_first_item.dropDuplicates((['championName'])).sort((['championName']))

	# SQL querrie to extract the most common full build for each champion

    connector = "-"
    build = sqlContext.sql("""
		                  SELECT build.championName, build.build_name, COUNT(build.build_name) as total_matches \
		                  FROM \
		                     (SELECT match_data.name_champion as championName, CONCAT(match_data.name_item0, "%s",
		                                 match_data.name_item1, "%s",
		                                 match_data.name_item2, "%s",
		                                 match_data.name_item3, "%s",
		                                 match_data.name_item4, "%s",
		                                 match_data.name_item5, "%s",
		                                 match_data.name_item6) as build_name \
		                     FROM match_data \
		                     WHERE match_data.participants_info_info_player_accountId \
		                     IN ( \
		                         SELECT players.user
		                         FROM players \
		                         WHERE players.win_rate > 0.5 AND players.total_matches > 2)) as build \
		                 GROUP BY build.championName, build.build_name \
		                 ORDER BY total_matches DESC
		             """ % tuple([connector]*6))
    build = build.dropDuplicates((['championName'])).sort((['championName']))

    return players, champions, build_first_item, build


def load_data(spark, players, champions, build_first_item, build, output_path):
    """Collect data locally and write to CSV.

    :param df: DataFrame to print.
    :return: None
    """
    players.write.parquet(output_path["players_data_path"])
    champions.write.parquet(output_path["champions_data_path"])
    build_first_item.write.parquet(output_path["build_first_item_data_path"])
    build.write.parquet(output_path["build_data_path"])
    return None


def create_test_data(spark, config):
    """Create test data.

    This function creates both both pre- and post- transformation data
    saved as Parquet files in tests/test_data. This will be used for
    unit tests as well as to load as part of the example ETL job.
    :return: None
    """
    # create example data from scratch
    local_records = [
       Row(id=1, first_name='Dan', second_name='Germain', floor=1),
       Row(id=2, first_name='Dan', second_name='Sommerville', floor=1),
       Row(id=3, first_name='Alex', second_name='Ioannides', floor=2),
       Row(id=4, first_name='Ken', second_name='Lai', floor=2),
       Row(id=5, first_name='Stu', second_name='White', floor=3),
       Row(id=6, first_name='Mark', second_name='Sweeting', floor=3),
       Row(id=7, first_name='Phil', second_name='Bird', floor=4),
       Row(id=8, first_name='Kim', second_name='Suter', floor=4)
    ]

    df = spark.createDataFrame(local_records)

    # write to Parquet file format
    (df
    .coalesce(1)
    .write
    .parquet('tests/test_data/employees', mode='overwrite'))

    # create transformed version of data
    df_tf = transform_data(df, config['steps_per_floor'])

    # write transformed version of data to Parquet
    (df_tf
    .coalesce(1)
    .write
    .parquet('tests/test_data/employees_report', mode='overwrite'))

    return None


# entry point for PySpark ETL application
if __name__ == '__main__':
    main()
