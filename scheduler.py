from pathlib import Path
import logging
import yaml
import os 
import sys

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, PROJECT_ROOT)
from dispatcher import get_mm_jobs_to_run  
from pathlib import Path
import logging
import datetime
import yaml
from dispatcher import get_mm_jobs_to_run           
logger = logging.getLogger(__name__)

def init_scheduler():
    """
    Parses model_monitoring_schedule.yaml formatted as:

    monitor_ver: 1
    models:
      - family: secured
        model: secured_capital
        library_ver: 3.4
        metrics: secured_capital.yml
        schedule: "/2**"
    """
    from dispatcher import get_mm_jobs_to_run  
    repo_root = Path(__file__).resolve().parents[1]
    schedule_path = repo_root / "conf" / "model_monitoring_schedule.yaml"

    if not schedule_path.exists():
        logger.error("Schedule file not found: %s", schedule_path)
        return

    try:
        with schedule_path.open("r", encoding="utf-8") as f:
            data = yaml.safe_load(f) or {}
    except Exception:
        logger.exception("Failed to load YAML file")
        return

    # ✅ Expecting YAML with top-level "models"
    models = data.get("models", [])
    if not models:
        logger.warning("No 'models' section found in scheduler YAML")
        return

    # ✅ Directly treat each model as a scheduled job
    today_jobs = models

    # ✅ import dispatcher
    try:
        from src.dispatcher import get_mm_jobs_to_run
    except Exception:
        from dispatcher import get_mm_jobs_to_run

    try:
        get_mm_jobs_to_run(today_jobs)
    except Exception:
        logger.exception("Error dispatching model monitoring jobs")



# def init_scheduler():
#     """Read model_monitoring_schedule.yaml from the repo root and dispatch today's jobs.

#     The YAML may be:
#       - a list of job dicts with optional `days` field (string or list)
#       - a dict keyed by weekday names (e.g. monday: [ ... ])
#       - a single job dict with keys name/model/time

#     Calls get_mm_jobs_to_run(list_of_jobs) when jobs are found for today.
#     """
#     repo_root = Path(__file__).resolve().parents[1]
#     schedule_path = repo_root / "conf" /"model_monitor_schedule.yaml"

#     if not schedule_path.exists():
#         logger.warning("Schedule file not found: %s", schedule_path)
#         return

#     try:
#         with schedule_path.open("r", encoding="utf-8") as f:
#             schedule_raw = yaml.safe_load(f) or {}
#     except Exception:
#         logger.exception("Failed to load schedule YAML")
#         return

#     today = datetime.date.today()
#     weekday = today.strftime("%A").lower()  # e.g. "monday"

#     def jobs_for_today(parsed):
#         out = []

#         if isinstance(parsed, dict):
#             # entries keyed by weekday
#             for key, val in parsed.items():
#                 if isinstance(key, str) and key.strip().lower() == weekday:
#                     if isinstance(val, list):
#                         out.extend(val)
#                     elif isinstance(val, dict):
#                         out.append(val)
#             # support top-level 'daily'
#             daily = parsed.get("daily") or parsed.get("Daily")
#             if isinstance(daily, list):
#                 out.extend(daily)
#             # single job as dict
#             if all(k in parsed for k in ("name", "model", "time")):
#                 out.append(parsed)

#         if isinstance(parsed, list):
#             for job in parsed:
#                 if not isinstance(job, dict):
#                     continue
#                 days = job.get("days") or job.get("day")
#                 if days is None:
#                     out.append(job)
#                     continue
#                 if isinstance(days, str):
#                     days_norm = [d.strip().lower() for d in days.split(",")]
#                 else:
#                     days_norm = [str(d).strip().lower() for d in days]
#                 if today.isoformat() in days_norm or weekday in days_norm or "daily" in days_norm:
#                     out.append(job)

#         return out

#     today_jobs = jobs_for_today(schedule_raw)

#     if not today_jobs:
#         logger.info("No monitoring jobs scheduled for today (%s).", weekday)
#         return

#     # Try relative import first (when part of a package), then absolute.
#     try:
#         from .jobs import get_mm_jobs_to_run  # type: ignore
#     except Exception:
#         try:
#             from jobs import get_mm_jobs_to_run  # type: ignore
#         except Exception:
#             logger.error("get_mm_jobs_to_run() not available to receive today's jobs.")
#             return

#     try:
#         get_mm_jobs_to_run(today_jobs)
#     except Exception:
#         logger.exception("Error while dispatching today's monitoring jobs.")


if __name__ == "__main__":
    init_scheduler()
       # and finally call get_mm_jobs_to_run(jobs_for_today)