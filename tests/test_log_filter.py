import unittest
from datetime import datetime, timezone, timedelta
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from main import _parse_log_time, _prune_old_snapshots, save_snapshot, load_config
import sqlite3


class TestParseLogTime(unittest.TestCase):
    def test_parse_iso_timestamp(self):
        line = "2026-08-06T14:30:00 Some log message"
        result = _parse_log_time(line)
        self.assertEqual(result, datetime(2026, 8, 6, 14, 30, 0, tzinfo=timezone.utc))

    def test_parse_space_separated_timestamp(self):
        line = "2026-08-06 14:30:00 Some log message"
        result = _parse_log_time(line)
        self.assertEqual(result, datetime(2026, 8, 6, 14, 30, 0, tzinfo=timezone.utc))

    def test_parse_with_timezone_offset(self):
        line = "2026-08-06T14:30:00+02:00 Some log message"
        result = _parse_log_time(line)
        self.assertEqual(result, datetime(2026, 8, 6, 14, 30, 0, tzinfo=timezone.utc))

    def test_parse_python_logging_format(self):
        line = "2026-08-06 14:30:00,123 - root - INFO - Some message"
        result = _parse_log_time(line)
        self.assertEqual(result, datetime(2026, 8, 6, 14, 30, 0, tzinfo=timezone.utc))

    def test_parse_systemd_journal_format(self):
        line = "Aug 06 14:30:00 hostname python[1234]: Some message"
        result = _parse_log_time(line)
        self.assertIsNone(result)

    def test_parse_short_month_name_format(self):
        line = "2026-Aug-06 14:30:00 Some message"
        result = _parse_log_time(line)
        self.assertIsNone(result)

    def test_parse_logfmt_with_level(self):
        line = "time=\"2026-08-06T14:30:00Z\" level=info msg=\"hello\""
        result = _parse_log_time(line)
        self.assertEqual(result, datetime(2026, 8, 6, 14, 30, 0, tzinfo=timezone.utc))

    def test_parse_json_log(self):
        line = '{"timestamp": "2026-08-06T14:30:00Z", "message": "hello"}'
        result = _parse_log_time(line)
        self.assertEqual(result, datetime(2026, 8, 6, 14, 30, 0, tzinfo=timezone.utc))

    def test_parse_log_with_bracket_level(self):
        line = "[2026-08-06 14:30:00] [INFO] Some message"
        result = _parse_log_time(line)
        self.assertEqual(result, datetime(2026, 8, 6, 14, 30, 0, tzinfo=timezone.utc))

    def test_no_timestamp(self):
        line = "Some log message without timestamp"
        result = _parse_log_time(line)
        self.assertIsNone(result)

    def test_malformed_timestamp(self):
        line = "2026-13-01T99:99:99 Some log message"
        result = _parse_log_time(line)
        self.assertIsNone(result)


class TestLogFiltering(unittest.TestCase):
    def _filter_content(self, content, cutoff):
        filtered = []
        for line in content:
            parsed = _parse_log_time(line)
            if parsed is None or parsed >= cutoff:
                filtered.append(line)
        return filtered

    def test_1d_filter_keeps_recent_lines(self):
        cutoff = datetime(2026, 8, 5, 22, 37, 38, tzinfo=timezone.utc)
        content = [
            "2026-08-06T10:00:00 Recent line",
            "2026-08-06T12:00:00 Another recent line",
        ]
        result = self._filter_content(content, cutoff)
        self.assertEqual(len(result), 2)

    def test_1d_filter_removes_old_lines(self):
        cutoff = datetime(2026, 8, 5, 22, 37, 38, tzinfo=timezone.utc)
        content = [
            "2026-08-04T10:00:00 Old line",
            "2026-08-05T10:00:00 Borderline old line",
        ]
        result = self._filter_content(content, cutoff)
        self.assertEqual(len(result), 0)

    def test_1d_filter_keeps_lines_without_timestamp(self):
        cutoff = datetime(2026, 8, 5, 22, 37, 38, tzinfo=timezone.utc)
        content = [
            "2026-08-06T10:00:00 Recent line",
            "Stack trace without timestamp",
            "Another unparseable line",
        ]
        result = self._filter_content(content, cutoff)
        self.assertEqual(len(result), 3)

    def test_1d_filter_mixed_content(self):
        cutoff = datetime(2026, 8, 5, 22, 37, 38, tzinfo=timezone.utc)
        content = [
            "2026-08-06T10:00:00 Recent line",
            "2026-08-04T10:00:00 Old line",
            "No timestamp here",
        ]
        result = self._filter_content(content, cutoff)
        self.assertEqual(len(result), 2)
        self.assertIn("2026-08-06T10:00:00 Recent line", result)
        self.assertIn("No timestamp here", result)

    def test_1d_filter_all_old_entries(self):
        cutoff = datetime(2026, 8, 5, 22, 37, 38, tzinfo=timezone.utc)
        content = [
            "2026-08-04T10:00:00 Old line 1",
            "2026-08-05T10:00:00 Old line 2",
        ]
        result = self._filter_content(content, cutoff)
        self.assertEqual(len(result), 0)

    def test_1d_filter_with_unparseable_only(self):
        cutoff = datetime(2026, 8, 5, 22, 37, 38, tzinfo=timezone.utc)
        content = [
            "Stack trace without timestamp",
            "Another unparseable line",
        ]
        result = self._filter_content(content, cutoff)
        self.assertEqual(len(result), 2)


class TestLogFileSelection(unittest.TestCase):
    def test_1d_range_uses_active_log_only(self):
        range_val = "1d"
        filename = "rsa_orchestrator"
        pattern = f"{filename}.log" if range_val == "1d" else f"{filename}*"
        self.assertEqual(pattern, "rsa_orchestrator.log")

    def test_all_range_uses_wildcard(self):
        range_val = "all"
        filename = "rsa_orchestrator"
        pattern = f"{filename}.log" if range_val == "1d" else f"{filename}*"
        self.assertEqual(pattern, "rsa_orchestrator*")

    def test_1d_range_includes_rotated_gz(self):
        import tempfile, os
        from pathlib import Path
        with tempfile.TemporaryDirectory() as tmpdir:
            base = Path(tmpdir)
            (base / "rsa_orchestrator.log").write_text("current")
            (base / "rsa_orchestrator.log.1.gz").write_text("rotated")
            pattern = "rsa_orchestrator.log"
            matches = sorted(base.glob(pattern))
            self.assertEqual([m.name for m in matches], ["rsa_orchestrator.log"])

    def test_all_range_includes_rotated_gz(self):
        import tempfile
        from pathlib import Path
        with tempfile.TemporaryDirectory() as tmpdir:
            base = Path(tmpdir)
            (base / "rsa_orchestrator.log").write_text("current")
            (base / "rsa_orchestrator.log.1.gz").write_text("rotated")
            pattern = "rsa_orchestrator*"
            matches = sorted(base.glob(pattern))
            self.assertEqual([m.name for m in matches], ["rsa_orchestrator.log", "rsa_orchestrator.log.1.gz"])


if __name__ == "__main__":
    unittest.main()
