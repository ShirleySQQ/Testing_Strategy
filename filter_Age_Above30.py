from pyspark.sql import SparkSession
import logging
import pyspark.sql.functions as F
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def output_ageHigh30(spark):
    try:
        out_30_data = 'higher_30.csv'
        raw_data='CustomerAges.csv'
        # Perform transformation
        df = spark.read.format("csv").option("delimiter", ",").option("header", True).load(raw_data)
        df_transformed = filter_age(df)
        # Write transformed data
        logger.info("Writing to csv.")
        df_transformed.toPandas().to_csv(out_30_data)
    except Exception as e:
        logger.error(e)
        raise


def filter_age(df):
    logger.info("Loading data")
   # df.show(2)
    logger.info("Transforming data")
    return df.filter(df.Age > 30)


def create_spark_session():
    spark = SparkSession.builder.getOrCreate()
    print('hello')
    return spark

'''
if __name__ == "__main__":
    spark = create_spark_session()
    output_ageHigh30(spark)
'''