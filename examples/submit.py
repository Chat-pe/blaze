
from ..src.core import SubmitSequenceData
from ..src.daemon.manager import update_jobs


job = update_jobs(submitted_jobs = [SubmitSequenceData(
    seq_id="math_pipeline_1",
    seq_run_interval="*/1 * * * *",
    parameters={
        "generate_numbers": {
            "count": 4,
            "min_val": 180,
            "max_val": 280
        },
        "calculate_statistics": {
            "numbers": "@generate_numbers"
        },
        "transform_data": {
            "stats": "@calculate_statistics"
        }
    }
)])

print(job)