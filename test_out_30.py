from unittest.mock import patch, MagicMock

import pandas
import pandas as pd
from pandas import DataFrame

import filter_Age_Above30


@patch('filter_Age_Above30.filter_age')
def test_out_30_records(mock_filter_30):
    mock_higer30_out = 'mock_higer30_out.csv'
    mock_spark_session = MagicMock(name='Spark')
    df = MagicMock(name = 'df')
    df.toPandas.return_value = pd.DataFrame()
    #mock_filter_30.return_value = MagicMock(spec = DataFrame)
    mock_filter_30.return_value = df
    #mock_filter_30.return_value.toPandas.return_value = pd.DataFrame()
    mock_pandas_to_csv = patch('pandas.DataFrame.to_csv',return_value = None)
    with mock_pandas_to_csv as mocked_to_csv:
        filter_Age_Above30.output_ageHigh30(mock_spark_session)
    mocked_to_csv.assert_called()

