import unittest
import sys
import os
import yaml

# ensure project root is in sys.path
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, PROJECT_ROOT)

from src.dispatcher import get_mm_jobs_to_run, invoke_mm_jobs


class TestDispatcher(unittest.TestCase):
    """Unit tests for dispatcher.py"""

    def setUp(self):
        # setup fake folder structure
        self.base_conf = os.path.join(PROJECT_ROOT, "conf")
        os.makedirs(os.path.join(self.base_conf, "secured_capital"), exist_ok=True)
        self.metric_path = os.path.join(self.base_conf, "secured_capital", "secured_capital_metric.yaml")
        with open(self.metric_path, "w", encoding="utf-8") as f:
            f.write("models:\n  - name: Secured_Capital PD\n    rs_mm_lib_version: 3.4\n")

    def tearDown(self):
        try:
            os.remove(self.metric_path)
        except FileNotFoundError:
            pass

    def test_get_mm_jobs_to_run_valid(self):
        """Test that valid jobs return correct metric paths"""
        today_jobs = [{
            "family": "secured_capital",
            "metrics": "secured_capital_metric.yaml"
        }]

        result = get_mm_jobs_to_run(today_jobs)
        self.assertEqual(len(result), 1)
        self.assertTrue(result[0].endswith("secured_capital_metric.yaml"))

    def test_invoke_mm_jobs_valid_yaml(self):
        """Test parsing of model details from metric YAML"""
        with open(self.metric_path, "r", encoding="utf-8") as f:
            job_yml = yaml.safe_load(f)

        lib_version, model_name = invoke_mm_jobs(job_yml)
        self.assertEqual(lib_version, 3.4)
        self.assertEqual(model_name, "Secured_Capital PD")

    def test_invoke_mm_jobs_invalid_yaml(self):
        """Test that invalid YAML returns (None, None)"""
        job_yml = {"not_models": []}
        lib_version, model_name = invoke_mm_jobs(job_yml)
        self.assertIsNone(lib_version)
        self.assertIsNone(model_name)


if __name__ == "__main__":
    unittest.main()