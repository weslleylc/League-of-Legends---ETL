"""
utils.py
~~~~~~~~

Module containing helper function for transforming data
"""
from pyspark.sql.functions import *
from pyspark.sql.types import *
import pyspark.sql.functions as F
import json

# Convenience function for turning JSON strings into DataFrames.
# https://docs.databricks.com/_static/notebooks/transform-complex-data-types-scala.html
def jsonToDataFrame(spark, json_input, schema=None):
    # SparkSessions are available with Spark 2.0+
    reader = spark.read
    if schema:
        reader.schema(schema)
    return reader.json(spark.sparkContext.parallelize([json_input]))


# Convenience function flatten dataframes with structs.
#https://stackoverflow.com/questions/38753898/how-to-flatten-a-struct-in-a-spark-dataframe
def flatten_df(nested_df):
    stack = [((), nested_df)]
    columns = []

    while len(stack) > 0:
        parents, df = stack.pop()

        flat_cols = [
            col(".".join(parents + (c[0],))).alias("_".join(parents + (c[0],)))
            for c in df.dtypes
            if c[1][:6] != "struct"
        ]

        nested_cols = [
            c[0]
            for c in df.dtypes
            if c[1][:6] == "struct"
        ]

        columns.extend(flat_cols)

        for nested_col in nested_cols:
            projected_df = df.select(nested_col + ".*")
            stack.append((parents + (nested_col,), projected_df))

    return nested_df.select(columns)

# Convenience function transform dict array string into rows
def transform_colum(spark, df, column):
    df_select = df.select(col(column))
    str_ = df_select.take(1)[0].asDict()[column]
    df_select = jsonToDataFrame(spark, json.dumps(eval(str_)))
    schema = df_select.schema
    
    eval_column = udf(lambda x : eval(x), ArrayType(schema))

    df = df.withColumn(column, eval_column(col(column)))
    
    return df, schema

