import sys
import os

# Add the parent directory to Python path so we can import src
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from src.daemon.manager import get_or_create_scheduler, start_scheduler
from examples.definition import blocks, sequences

scheduler = get_or_create_scheduler(blocks, sequences)

start_scheduler()