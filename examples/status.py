#!/usr/bin/env python3
"""
Example script to get job status from Blaze daemon manager.
This script calls the get_all_jobs_status function from the daemon manager
and prints the JSON response in a formatted way.
"""

import json
import sys
import os

# Add the src directory to Python path so we can import from it
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from daemon.manager import get_all_jobs_status

def main():
    """Main function to get and display job status."""
    print("Fetching job status from MongoDB...")
    print("=" * 50)
    
    # Get job status from daemon manager
    status_data = get_all_jobs_status()
    
    # Print formatted JSON response
    print(json.dumps(status_data, indent=2))
    
    # Additional user-friendly display
    if status_data.get("success"):
        jobs = status_data.get("jobs", [])
        total_jobs = status_data.get("total_jobs", 0)
        
        print("\n" + "=" * 50)
        print(f"Summary: {total_jobs} jobs found")
        
        if jobs:
            print("\nJob Status Overview:")
            for job in jobs:
                print(f"  • {job['seq_id']}: {job['status']} ({job['total_runs']} runs)")
        else:
            print("\nNo jobs currently scheduled.")
    else:
        print(f"\nError: {status_data.get('error', 'Unknown error occurred')}")
        sys.exit(1)

if __name__ == "__main__":
    main()