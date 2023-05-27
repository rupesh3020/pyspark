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
import sys
print(sys.path)
from pyspark.sql import DataFrame
from pyspark.sql.functions import *
import json
from dependencies.spark import start_spark


def main():
    """Main ETL script definition.

    :return: None
    """
    with open("configs/spark_config.json") as f:
        data = f.read()
    spark_config = json.loads(data)
    # start Spark application and get Spark session, logger and config
    spark, log, config = start_spark(
        app_name='my_etl_job',
        files=['configs/etl_config.json'],
        spark_config=spark_config)

    # log that main ETL job is starting
    log.warn('etl_job is up-and-running')

    # execute ETL pipeline
    bets,transactions = extract_data(spark)
    data_transformed = transform_data(bets,transactions)
    load_data(data_transformed)

    # log the success and terminate Spark application
    log.warn('test_etl_job is finished')
    spark.stop()
    return None


def extract_data(spark):
    """Load data from Parquet file format.

    :param spark: Spark session object.
    :return: Spark DataFrame.
    """
    bets = spark.read.format("delta").load("tests/test_data/bets_v1")
    transactions = spark.read.format("delta").load("tests/test_data/trans_v1")

    return bets,transactions


def transform_data(bets, transactions):
    """Transform original dataset.

    :param df: Input DataFrame.
    :param steps_per_floor_: The number of steps per-floor at 43 Tanner
        Street.
    :return: Transformed DataFrame.
    """
    combined_df = bets.withColumn("legs", explode("legs")).withColumn("markets",explode("markets")).filter(col("legs.legPart.outcomeRef")==col("markets.outcomeRef"))
    final=combined_df.withColumn("combined", struct("legs.*","markets.*")).groupBy("sportsbook_id", "account_id").agg(collect_list("legs").alias("legs"),collect_list("markets").alias("markets"),collect_list("combined").alias("outcomes"))
    transactions_renamed=transactions.withColumnRenamed("sportsbook_id","sportsbook_id_tra")
    print(transactions_renamed.columns)
    joined_dataset=final.join(transactions_renamed,final.sportsbook_id==transactions_renamed.sportsbook_id_tra,"left")
    joined_grouped_dataset=joined_dataset.groupBy("sportsbook_id").agg(collect_list("trans_uuid").alias("all_related_transactions"),first("legs").alias("legs"),first("markets").alias("markets"),first("outcomes").alias("outcomes"),first("account_id").alias("account_id"))
    return joined_grouped_dataset


def load_data(df:DataFrame):
    """Collect data locally and write to CSV.

    :param df: DataFrame to print.
    :return: None
    """
    (df.write
     .parquet('tests/test_data/bets_interview_completed', mode='overwrite'))
    return None

# entry point for PySpark ETL application
if __name__ == '__main__':
    main()
