
from blaze.src.core import SubmitSequenceData
from blaze.src.daemon.manager import update_jobs


update_jobs(submitted_jobs = [SubmitSequenceData(
    seq_id="math_pipeline_1",
    seq_run_interval="*/1 * * * *",
    parameters={
        "generate_numbers": {
            "count": 10,
            "min_val": 100,
            "max_val": 200
        },
        "calculate_statistics": {
            "numbers": "@generate_numbers"
        },
        "transform_data": {
            "stats": "@calculate_statistics"
        }
    }
)])