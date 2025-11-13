
from pathlib import Path
import logging
import yaml
import os 
import sys

# PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
# sys.path.insert(0, PROJECT_ROOT)
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(PROJECT_ROOT, "src"))
from dispatcher import get_mm_jobs_to_run
from dispatcher import get_mm_jobs_to_run  
from pathlib import Path
import logging
import datetime
import yaml
          
logger = logging.getLogger(__name__)

def should_run_now(schedule_str: str, now: datetime.datetime) -> bool:
    """
    Decide if a job should run now based on schedule pattern.
    Examples:
      /1** -> every 1 minute
      /2** -> every 2 minutes
      /1H  -> every 1 hour
    """
    if not schedule_str or not schedule_str.startswith("/"):
        return False

    try:
        # Pattern: /5** → every 5 minutes
        if schedule_str.endswith("**"):
            minutes = int(schedule_str[1:-2])
            return now.minute % minutes == 0
        # Pattern: /1H → every 1 hour
        elif schedule_str.endswith("H"):
            hours = int(schedule_str[1:-1])
            return now.minute == 0 and now.hour % hours == 0
    except Exception as e:
        logger.warning(f"Unrecognized schedule pattern '{schedule_str}': {e}")
    return False

def init_scheduler(today_jobs):
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
    #--> add line
    #now = datetime.datetime.now()
    #today_jobs = []
    # today_jobs = models
    now = datetime.datetime.now()
    today_jobs = []

    for job in models:
        schedule_str = job.get("schedule")
        if should_run_now(schedule_str, now):
            today_jobs.append(job)
        else:
            logger.debug(f"Skipping job '{job.get('model')}', not scheduled now.")

    if not today_jobs:
        logger.info(" No jobs scheduled to run at this time")
        return

    logger.info(f"Running {len(today_jobs)} scheduled job(s)...")

    try:
        get_mm_jobs_to_run(today_jobs)
    except Exception:
        logger.exception("Error while dispatching model monitoring jobs")
    
   

if __name__ == "__main__":
     today_jobs = []  
     init_scheduler(today_jobs)
