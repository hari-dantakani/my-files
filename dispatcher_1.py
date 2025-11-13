import yaml
import os
import datetime

import os
import yaml
import datetime



def get_mm_jobs_to_run(today_jobs):
    """
    today_jobs = list of dicts passed by scheduler, each job like:

    {
        "family": "secured",
        "model": "secured_capital",
        "library_ver": "3.4",
        "metrics": "secured_capital.yml",
        "schedule": "/2**"
    }

    This function converts them into absolute metric YAML paths.
    """

    base_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "conf"))

    jobs_to_run = []

    for job in today_jobs:
        family = job.get("family")
        metrics_file = job.get("metrics")

        if not family or not metrics_file:
            print(f" Skipping invalid job entry: {job}")
            continue

        job_path = os.path.join(base_dir, family, metrics_file)

        if not os.path.exists(job_path):
            print(f" Metrics file not found: {job_path}")
            continue

        jobs_to_run.append(job_path)

    return jobs_to_run


def process_jobs(list_jobs):
    for job_path in list_jobs:
        print(f"Processing job: {job_path}")

        job_yml = load_yaml(job_path)

        lib_version, model_name = invoke_mm_jobs(job_yml)

        print(f" Completed job — Library: {lib_version}, Model: {model_name}")


def load_yaml(file_path):
    with open(file_path, "r") as file:
        return yaml.safe_load(file)


def invoke_mm_jobs(job_yml):
    """
    Expected structure in each metrics YAML:

    models:
      - name: secured_capital
        rs_mm_lib_version: 3.4
    """

    try:
        models_selection = job_yml.get("models", [])
        if not models_selection:
            print(" No 'models' key in metrics file.")
            return None, None

        first_model = models_selection[0]

        model_name = first_model.get("name")
        lib_version = first_model.get("rs_mm_lib_version")

        print(f"➡ Model found: {model_name}")

        return lib_version, model_name

    except Exception as e:
        print(f" Error parsing job YAML: {e}")
        return None, None


if __name__ == "__main__":
    # Manual test execution (bypasses scheduler)
    today = datetime.datetime.now()

    # simulate scheduler output
    sample_jobs = [
        {
            "family": "secured",
            "model": "secured_capital",
            "library_ver": "3.4",
            "metrics": "secured_capital.yml",
            "schedule": "/2**"
        }
    ]

    jobs = get_mm_jobs_to_run(sample_jobs)

    if not jobs:
        print("no jobs scheduled for this time")
    else:
        process_jobs(jobs)