import unittest
import sys, os
from unittest.mock import patch, mock_open

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, PROJECT_ROOT)
from src.scheduler import init_scheduler


class TestScheduler(unittest.TestCase):

    @patch("src.dispatcher.get_mm_jobs_to_run")
    @patch("builtins.open", new_callable=mock_open, read_data="models:\n  - model: secured_capital PD")
    @patch("src.scheduler.yaml.safe_load")
    def test_init_scheduler(self, mock_yaml_load, mock_open, mock_get_jobs):
        """Test that init_scheduler reads YAML and calls get_mm_jobs_to_run."""

        # Mock YAML structure
        mock_yaml_load.return_value = {"models": [{"model": "secured_capital PD"}]}

        # Pass an empty list as the required argument
        today_jobs = []
        init_scheduler(today_jobs)

        # YAML file should be opened
        mock_open.assert_called_once_with(
            os.path.join(PROJECT_ROOT, "conf", "model_monitoring_schedule.yaml"), "r", encoding="utf-8"
        )

        # YAML parsed
        mock_yaml_load.assert_called()

        # Dispatcher should receive job list
        mock_get_jobs.assert_called_once()


if __name__ == "__main__":
    unittest.main()