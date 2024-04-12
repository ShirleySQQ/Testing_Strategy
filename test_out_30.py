from unittest.mock import patch, MagicMock

import pandas
import pandas as pd
from pandas import DataFrame

import filter_Age_Above30


@patch('filter_Age_Above30.filter_age')
def test_out_30_records(mock_filter_30):
    mock_higer30_out = 'mock_higer30_out.csv'
    mock_spark_session = MagicMock(name='Spark')
    df = MagicMock(name='df')
    df.toPandas.return_value = pd.DataFrame()
    # mock_filter_30.return_value = MagicMock(spec = DataFrame)
    mock_filter_30.return_value = df
    # mock_filter_30.return_value.toPandas.return_value = pd.DataFrame()
    mock_pandas_to_csv = patch('pandas.DataFrame.to_csv', return_value=None)
    with mock_pandas_to_csv as mocked_to_csv:
        # with mock_pandas_to_csv:
        filter_Age_Above30.output_ageHigh30(mock_spark_session)
    mocked_to_csv.assert_called()


# mock_pandas_to_csv.assert_called()

def test_filter_age():
    spark = filter_Age_Above30.create_spark_session()
    filter_test = 'CustomerAges_mockTest.csv'
    df = spark.read.option("header", True).csv(filter_test)

    # Call the function
    result = filter_Age_Above30.filter_age(df)

    assert result is not None
    assert result.count()==5

