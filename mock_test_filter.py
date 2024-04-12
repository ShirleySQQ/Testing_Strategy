from pyspark.sql import SparkSession
from unittest.mock import MagicMock, patch
import filter_Age_Above30  # Replace with your module name

class TestFilterAge:
    @patch("pyspark.sql.SparkSession.builder.getOrCreate")
    def test_filter_age(self, spark):
        # Mock the dataframe
        df = MagicMock()

        # Mock the age attribute and set '> 30' condition always to True
        df.Age.__gt__.return_value = True

        # When spark.read.format().option().option().load() function is called, return the mock DataFrame df
        spark.read.format.return_value.option.return_value.option.return_value.load.return_value = df

        # Mock filter method to return the same DataFrame when filter() is called
        df.filter.return_value = df

        # Call the function
        result = filter_Age_Above30.filter_age(spark)

        # Check that the filter is called on the DataFrame
        df.filter.assert_called()
       # df.filter.assert_called_once_with(df.Age>30)

        # Validate that the function returns the expected result
        assert result == df

# Run the test
TestFilterAge().test_filter_age()