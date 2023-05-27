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
from jobs.assignment import transform_data
from chispa.dataframe_comparer import *

class SparkETLTests(unittest.TestCase):
    """Test suite for transformation in etl_job.py
    """
    @classmethod
    def setUp(self):
        """Start Spark, define config and path to test data
        """
        with open("configs/spark_config.json") as f:
            data = f.read()
        self.spark_config = json.loads(data)
        self.jar_packages = ["org.apache.hadoop:hadoop-azure:3.2.4","io.delta:delta-core_2.12:1.0.0"]
        self.spark, logger,*_ = start_spark(spark_config=self.spark_config, jar_packages=self.jar_packages)
        self.test_data_path = 'tests/test_data'

    @classmethod
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
        # bets = (
        #     self.spark
        #     .read.option("multiline", "true")
        #     .format("json").load(self.test_data_path + '/bets.json'))

        bets = self.spark.read.format("delta").load(self.test_data_path + "/bets_v1")

        transactions = self.spark.read.format("delta").load(self.test_data_path + "/trans_v1")
        
        expected_result = self.spark.read.format("delta").load(self.test_data_path + "/output")
        expected_output = transform_data(bets,transactions)
        assert expected_result.count()==expected_output.count()
        assert_basic_rows_equality(expected_output,expected_result)        

if __name__ == '__main__':
    unittest.main()
