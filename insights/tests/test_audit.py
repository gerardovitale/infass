import json
import os
from datetime import date
from unittest.mock import patch

from insight_generator import audit_run


class TestAuditRun:
    def test_saves_input_json(self, tmp_path):
        @audit_run(audit_dir=str(tmp_path))
        def my_func(data, name):
            return "ok"

        my_func([{"a": 1}], "test")

        run_dirs = os.listdir(tmp_path / "my_func")
        assert len(run_dirs) == 1
        input_path = tmp_path / "my_func" / run_dirs[0] / "input.json"
        saved = json.loads(input_path.read_text())
        assert saved == {"data": [{"a": 1}], "name": "test"}

    def test_saves_output_txt(self, tmp_path):
        @audit_run(audit_dir=str(tmp_path))
        def my_func():
            return "hello world"

        my_func()

        run_dirs = os.listdir(tmp_path / "my_func")
        output_path = tmp_path / "my_func" / run_dirs[0] / "output.txt"
        assert output_path.read_text() == "hello world"

    def test_excludes_params(self, tmp_path):
        @audit_run(audit_dir=str(tmp_path), exclude_params=["secret"])
        def my_func(data, secret):
            return "ok"

        my_func(["x"], "my-key")

        run_dirs = os.listdir(tmp_path / "my_func")
        saved = json.loads((tmp_path / "my_func" / run_dirs[0] / "input.json").read_text())
        assert "secret" not in saved
        assert saved == {"data": ["x"]}

    def test_handles_date_in_input(self, tmp_path):
        @audit_run(audit_dir=str(tmp_path))
        def my_func(data):
            return "ok"

        my_func([{"date": date(2026, 1, 15)}])

        run_dirs = os.listdir(tmp_path / "my_func")
        saved = json.loads((tmp_path / "my_func" / run_dirs[0] / "input.json").read_text())
        assert saved == {"data": [{"date": "2026-01-15"}]}

    def test_no_output_file_when_none_returned(self, tmp_path):
        @audit_run(audit_dir=str(tmp_path))
        def my_func():
            return None

        my_func()

        run_dirs = os.listdir(tmp_path / "my_func")
        output_path = tmp_path / "my_func" / run_dirs[0] / "output.txt"
        assert not output_path.exists()

    def test_non_string_output_is_json_serialized(self, tmp_path):
        @audit_run(audit_dir=str(tmp_path))
        def my_func():
            return {"key": "value", "count": 42}

        my_func()

        run_dirs = os.listdir(tmp_path / "my_func")
        output_path = tmp_path / "my_func" / run_dirs[0] / "output.txt"
        saved = json.loads(output_path.read_text())
        assert saved == {"key": "value", "count": 42}

    def test_returns_original_result(self, tmp_path):
        @audit_run(audit_dir=str(tmp_path))
        def my_func():
            return "the result"

        assert my_func() == "the result"

    def test_uses_function_name_in_directory(self, tmp_path):
        @audit_run(audit_dir=str(tmp_path))
        def specific_name():
            return "ok"

        specific_name()

        assert "specific_name" in os.listdir(tmp_path)

    def test_uses_timestamp_in_directory(self, tmp_path):
        with patch("insight_generator.datetime") as mock_dt:
            mock_dt.now.return_value.strftime.return_value = "2026-02-09T10-30-00"

            @audit_run(audit_dir=str(tmp_path))
            def my_func():
                return "ok"

            my_func()

        run_dirs = os.listdir(tmp_path / "my_func")
        assert run_dirs == ["2026-02-09T10-30-00"]
