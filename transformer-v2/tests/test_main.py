from main import parse_args


def test_parse_args(monkeypatch):
    test_args = ["prog", "--gcs-source-bucket", "bucket", "--bq-destination-table", "table"]
    monkeypatch.setattr("sys.argv", test_args)
    args = parse_args()
    assert args.gcs_source_bucket == "bucket"
    assert args.bq_destination_table == "table"
