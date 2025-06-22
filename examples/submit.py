
from src.core import SubmitSequenceData
from src.daemon.manager import update_jobs


update_jobs(submitted_jobs = [SubmitSequenceData(
    seq_id="math_pipeline_1",
    parameters={
        "generate_numbers": {
            "count": 10,
            "min_val": 1,
            "max_val": 100
        },
        "calculate_statistics": {
            "numbers": "@generate_numbers"
        },
        "transform_data": {
            "stats": "@calculate_statistics"
        }
    }
)])