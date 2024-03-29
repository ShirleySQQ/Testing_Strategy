from pyspark.sql import SparkSession
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
raw_data = 'CustomerAges.csv'
out_30_data = 'higher_30.csv'

def output_ageHigh30(spark):
    try:
        logger.info("Loading data")
        df = spark.read.format("csv").option("delimiter",",").option("header",True).load(raw_data)
        df.show(2)
        logger.info("Transforming data")
        df_transformed = df.filter(df.Age>30)
        df_transformed.toPandas().to_csv(out_30_data)
    except Exception as e:
        logger.error(e)
        raise


def create_spark_session():
    spark = SparkSession.builder.getOrCreate()

    return spark

"""
if __name__ == "__main__":
    spark = create_spark_session()
    output_ageHigh30(spark)

"""