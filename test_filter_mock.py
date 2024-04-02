from unittest.mock import patch, MagicMock

from pyspark.sql import DataFrame, SparkSession
import pyspark.sql.functions as F

import filter_Age_Above30  # Replace with your Python file name

spark = SparkSession.builder.getOrCreate()


@patch('filter_Age_Above30.create_spark_session', return_value=spark)
def test_filter_age(mock_spark_secion):
    mock_test_data = 'CustomerAges_mockTest.csv'
    mock_higer30_out = 'mock_higer30_out.csv'
    # Create a Mock DataFrame
    df = MagicMock(spec=DataFrame)
    # Mock the filter operation to return itself
    df.filter.return_value = df
    df.Age = True
    mock_spark_secion.read.format = MagicMock(return_value=mock_spark_secion.read)
    mock_spark_secion.read.option = MagicMock(return_value=mock_spark_secion.read)
    mock_spark_secion.read.load = MagicMock(return_value=df)
    df.filter.return_value = filter_Age_Above30.filter_age(mock_spark_secion)
    df.filter.assert_called_once_with(df.Age > 31)
