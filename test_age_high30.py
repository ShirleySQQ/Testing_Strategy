# test_main.py
import pyspark.sql.functions as F

import filter_Age_Above30
from filter_Age_Above30 import output_ageHigh30, out_30_data  # Import the function from main.py file
from pyspark.sql import SparkSession

spark = filter_Age_Above30.create_spark_session()


def test_age_high30():
    # Setup
    output_ageHigh30(spark)
    # Then
    df_out = spark.read.format("csv").option("delimiter", ",").option("header", True).load(out_30_data)
    assert df_out.filter(F.col('Age') <= 30).count() == 0
