import tempfile
import unittest
from pathlib import Path

from src.csv_io import read_csv_rows


class TestCsvIoEncoding(unittest.TestCase):
    def test_reads_cp1252_csv(self):
        # 0x95 is a common Windows-1252 bullet that fails UTF-8 decoding.
        raw = b"timestamp,comment\n2026-03-16 09:15:00,bullet:\x95 ok\n"

        with tempfile.TemporaryDirectory() as td:
            p = Path(td) / "in.csv"
            p.write_bytes(raw)
            rows = read_csv_rows(p)

        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["timestamp"], "2026-03-16 09:15:00")
        self.assertIn("bullet", rows[0]["comment"])


if __name__ == "__main__":
    unittest.main()