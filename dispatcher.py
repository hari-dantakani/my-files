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
            print(f"⚠ Skipping invalid job entry: {job}")
            continue

        job_path = os.path.join(base_dir, family, metrics_file)

        if not os.path.exists(job_path):
            print(f"⚠ Metrics file not found: {job_path}")
            continue

        jobs_to_run.append(job_path)

    return jobs_to_run


def process_jobs(list_jobs):
    for job_path in list_jobs:
        print(f"Processing job: {job_path}")

        job_yml = load_yaml(job_path)

        lib_version, model_name = invoke_mm_jobs(job_yml)

        print(f"✅ Completed job — Library: {lib_version}, Model: {model_name}")


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
            print("⚠ No 'models' key in metrics file.")
            return None, None

        first_model = models_selection[0]

        model_name = first_model.get("name")
        lib_version = first_model.get("rs_mm_lib_version")

        print(f"➡ Model found: {model_name}")

        return lib_version, model_name

    except Exception as e:
        print(f"❌ Error parsing job YAML: {e}")
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
  






# def get_mm_jobs_to_run(today):

#     jobs_to_run =[]

#     hour = today.hour

#     base_dir = os.path.join(os.path.dirname(__file__), "..","conf")

#     base_dir = os.path.abspath(base_dir)

#     if hour == 6:

#         jobs_to_run.append(os.path.join(base_dir, "secured_capital", "secured_capital_metric.yaml"))

#     elif hour ==7:

#         jobs_to_run.append(os.path.join(base_dir, "risk_science", "risk_science_metrics.yaml"))

#     return jobs_to_run

# def process_jobs(list_jobs):

#     for job_path in list_jobs:

#         print(f"processing job: {job_path}")

#         job_yml = load_yaml(job_path)

#         lib_version, model_name = invoke_mm_jobs(job_yml)

#         print(f"completed job Lib: {lib_version}, Model: {model_name}")

# def load_yaml(file_path):
#     with open (file_path, "r") as file: 
#         return yaml.safe_load(file)

# def invoke_mm_jobs(job_yml):

#     try:
#         models_selection = job_yml.get("models",{})

#         model_list = models_selection.get("model",[])

#         if not model_list:

#             print("no model entries") 
#             return None, None

#         first_model = model_list[0]

#         model_name = first_model.get("name")

#         lib_version = first_model.get("rs_mm_lib_version")

#         print(f" Model found: {model_name}")

#         #print(f"Library version: {lib_version}")

#         return lib_version, model_name

#     except Exception as e:

#         print(f"Error parsing job yaml:{e}")

#         return None, None

# if __name__ == "__main__":

#     today = datetime.datetime.now()#.replace(hour=6)

#     jobs = get_mm_jobs_to_run(today)

#     if not jobs:

#         print("no jobs scheduled for this time")

#     else:

#         process_jobs(jobs)









# # ...existing code...
# import yaml
# import os
# import datetime
# #import corniter

# # try to import croniter.match for full cron support; fallback to a simple hour/minute check
# try:
#     from croniter import match as cron_match  # croniter.match(expr, datetime) -> bool
#     _HAS_CRONITER = True
# except Exception:
#     cron_match = None
#     _HAS_CRONITER = False

# def get_mm_jobs_to_run(today):
#     """
#     Read conf/cron_mapping.yaml and return list of job yaml paths whose cron
#     expressions match the supplied `today` datetime.
#     """
#     jobs_to_run = []

#     base_dir = os.path.join(os.path.dirname(__file__), "..", "conf")
#     base_dir = os.path.abspath(base_dir)

#     mapping_path = os.path.join(base_dir, "cron_mapping.yaml")
#     try:
#         mapping = load_yaml(mapping_path) or {}
#     except FileNotFoundError:
#         print(f"cron mapping not found: {mapping_path}")
#         return jobs_to_run
#     except Exception as e:
#         print(f"error loading cron mapping: {e}")
#         return jobs_to_run

#     entries = mapping.get("cron_mapping", []) if isinstance(mapping, dict) else []
#     for entry in entries:
#         cron_expr = entry.get("cron")
#         job_rel = entry.get("job")  # e.g. "secured_capital/secured_capital_metric.yaml"
#         if not cron_expr or not job_rel:
#             continue

#         try:
#             matches = False
#             if _HAS_CRONITER and cron_match:
#                 # full cron support via croniter.match
#                 matches = cron_match(cron_expr, today)
#             else:
#                 # simple fallback: support basic 5-field crons where minute/hour are numbers or '*'
#                 parts = cron_expr.split()
#                 if len(parts) >= 2:
#                     minute_spec, hour_spec = parts[0], parts[1]
#                     minute_ok = (minute_spec == "*" or (minute_spec.isdigit() and int(minute_spec) == today.minute))
#                     hour_ok = (hour_spec == "*" or (hour_spec.isdigit() and int(hour_spec) == today.hour))
#                     matches = minute_ok and hour_ok
#             if matches:
#                 jobs_to_run.append(os.path.join(base_dir, job_rel))
#         except Exception as e:
#             print(f"error evaluating cron '{cron_expr}' for job '{job_rel}': {e}")

#     return jobs_to_run

# def process_jobs(list_jobs):
#     for job_path in list_jobs:
#         print(f"processing job: {job_path}")
#         job_yml = load_yaml(job_path)
#         lib_version, model_name = invoke_mm_jobs(job_yml)
#         print(f"completed job Lib: {lib_version}, Model: {model_name}")

# def load_yaml(file_path):
#     with open(file_path, "r", encoding="utf-8") as file:
#         return yaml.safe_load(file)

# def invoke_mm_jobs(job_yml):
#     try:
#         models_selection = job_yml.get("models", {})
#         model_list = models_selection.get("model", [])
#         if not model_list:
#             print("no model entries")
#             return None, None
#         first_model = model_list[0]
#         model_name = first_model.get("name")
#         lib_version = first_model.get("rs_mm_lib_version")
#         print(f" Model found: {model_name}")
#         return lib_version, model_name
#     except Exception as e:
#         print(f"Error parsing job yaml:{e}")
#         return None, None

# if __name__ == "__main__":
#     # example: force hour 6 for local runs; in production pass datetime.datetime.now()
#     today = datetime.datetime.now().replace(hour=6)
#     jobs = get_mm_jobs_to_run(today)
#     if not jobs:
#         print("no jobs scheduled for this time")
#     else:
#         process_jobs(jobs)
# # ...existing code...