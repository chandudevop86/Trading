import unittest

from src.auto_run import resolve_auto_run_execution_type


class TestAutoRunPaperOnly(unittest.TestCase):
    def test_live_request_is_forced_to_paper(self):
        execution_type, note = resolve_auto_run_execution_type("LIVE")
        self.assertEqual(execution_type, "PAPER")
        self.assertIn("forced to PAPER", note)

    def test_paper_request_stays_paper(self):
        execution_type, note = resolve_auto_run_execution_type("PAPER")
        self.assertEqual(execution_type, "PAPER")
        self.assertIn("PAPER mode", note)

    def test_none_request_disables_execution(self):
        execution_type, note = resolve_auto_run_execution_type("NONE")
        self.assertEqual(execution_type, "NONE")
        self.assertIn("disabled", note)


if __name__ == "__main__":
    unittest.main()
