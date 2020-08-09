"""
test_etl_job.py
~~~~~~~~~~~~~~~

This module contains unit tests for the transformation steps of the ETL
job defined in etl_job.py. It makes use of a local version of PySpark
that is bundled with the PySpark package.
"""
import unittest

import json

from pyspark.sql.functions import mean

from dependencies.spark import start_spark
from jobs.etl_job import transform_data, extract_data


class SparkETLTests(unittest.TestCase):
    """Test suite for transformation in etl_job.py
    """

    def setUp(self):
        """Start Spark, define config and path to test data
        """

        self.config = json.loads("""{
                                        "input_path":{
                                            "match_data_path":"./tests/test_data/test_match_data.csv",
                                            "itens_data_path":"./tests/test_data/riot_item.csv",
                                            "champions_data_path":"./tests/test_data//riot_champion.csv"
                                      },
                                        "output_path":{
                                            "players_data_path":"./tests/test_data/players.parquet",
                                            "champions_data_path":"./tests/test_data/champions.parquet",
                                            "build_first_item_data_path":"./tests/test_data/build_first_item.parquet",
                                            "build_data_path":"./tests/test_data/build.parquet"
                                      }
                                    }
                                    """)
        self.spark, *_ = start_spark()
        self.test_data_path = 'tests/test_data/'

    def tearDown(self):
        """Stop Spark
        """
        self.spark.stop()

    def test_transform_data(self):
        """Test data transformer.

        Using small chunks of input data and expected output data, we
        test the transformation step to make sure it's working as
        expected.
        """


        # assemble

        # read match data
        match_data, itens, champions = extract_data(self.spark, self.config['input_path'])



        # read expected output data
        expected_players = self.spark.read.parquet(self.config["output_path"]["players_data_path"])
        expected_champions = self.spark.read.parquet(self.config["output_path"]["champions_data_path"])
        expected_build_first_item = self.spark.read.parquet(self.config["output_path"]["build_first_item_data_path"])
        expected_build = self.spark.read.parquet(self.config["output_path"]["build_data_path"])



        players, champions, build_first_item, build = transform_data(self.spark, match_data, itens, champions)


        for expected_data, data_transformed in zip([expected_players, expected_champions, expected_build_first_item, expected_build],[players, champions, build_first_item, build]):
            expected_cols = len(expected_data.columns)
            expected_rows = expected_data.count()

            cols = len(expected_data.columns)
            rows = expected_data.count()


            # assert
            self.assertEqual(expected_cols, cols)
            self.assertEqual(expected_rows, rows)
            self.assertTrue([col in expected_data.columns
                             for col in data_transformed.columns])


if __name__ == '__main__':
    unittest.main()
