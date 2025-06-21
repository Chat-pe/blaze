from src.daemon.manager import get_or_create_scheduler, start_scheduler
from examples.definition import blocks, sequences

scheduler = get_or_create_scheduler(blocks, sequences)

start_scheduler()