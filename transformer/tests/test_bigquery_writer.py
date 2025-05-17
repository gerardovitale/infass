from unittest import TestCase
from unittest.mock import MagicMock
from unittest.mock import patch

from bigquery_writer import check_destination_table
from bigquery_writer import CLUSTERING_FIELDS
from bigquery_writer import PARTITION_FIELD
from bigquery_writer import PARTITION_TYPE
from google.cloud.bigquery import TimePartitioning

TEST_MODULE = "bigquery_writer"


class TestCheckDestinationTable(TestCase):

    @patch(f"{TEST_MODULE}.Client")
    def test_valid_table(self, mock_client_class):
        mock_client = MagicMock()
        mock_client_class.return_value = mock_client

        mock_table = MagicMock()
        mock_table.time_partitioning = TimePartitioning(type_=PARTITION_TYPE, field=PARTITION_FIELD)
        mock_table.clustering_fields = CLUSTERING_FIELDS
        mock_client.get_table.return_value = mock_table

        check_destination_table("my-project", "my_dataset", "my_table")

    @patch(f"{TEST_MODULE}.Client")
    def test_wrong_partition_type(self, mock_client_class):
        mock_client = MagicMock()
        mock_client_class.return_value = mock_client

        mock_table = MagicMock()
        mock_table.time_partitioning = TimePartitioning(type_="HOUR", field=PARTITION_FIELD)
        mock_table.clustering_fields = CLUSTERING_FIELDS
        mock_client.get_table.return_value = mock_table

        with self.assertRaises(RuntimeError) as context:
            check_destination_table("my-project", "my_dataset", "bad_partition_table")
        assert "Incompatible partitioning" in str(context.exception)

    @patch(f"{TEST_MODULE}.Client")
    def test_wrong_partition_field(self, mock_client_class):
        mock_client = MagicMock()
        mock_client_class.return_value = mock_client

        mock_table = MagicMock()
        mock_table.time_partitioning = TimePartitioning(type_=PARTITION_TYPE, field="wrong_field")
        mock_table.clustering_fields = CLUSTERING_FIELDS
        mock_client.get_table.return_value = mock_table

        with self.assertRaises(RuntimeError) as context:
            check_destination_table("my-project", "my_dataset", "bad_partition_field")
        assert "Incompatible partitioning" in str(context.exception)

    @patch(f"{TEST_MODULE}.Client")
    def test_wrong_clustering_fields(self, mock_client_class):
        mock_client = MagicMock()
        mock_client_class.return_value = mock_client

        mock_table = MagicMock()
        mock_table.time_partitioning = TimePartitioning(type_=PARTITION_TYPE, field=PARTITION_FIELD)
        mock_table.clustering_fields = ["only_name"]
        mock_client.get_table.return_value = mock_table

        with self.assertRaises(RuntimeError) as context:
            check_destination_table("my-project", "my_dataset", "bad_clustering")
        assert "Incompatible clustering" in str(context.exception)
