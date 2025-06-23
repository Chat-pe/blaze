# Blaze System Web API

A FastAPI-based web API for managing and monitoring the Blaze system jobs and sequences.

## Features

1. **System Status** - Get the current status of the Blaze system
2. **Active Blocks** - Show all active blocks in the system
3. **Sequence Management** - View sequence status and execution results
4. **Job Submission** - Submit new jobs for sequences
5. **Execution Logs** - View detailed logs for job executions

## Installation

1. Install dependencies:
```bash
pip install -r requirements.txt
```

2. Make sure the Blaze system is running (scheduler should be active)

## Running the API

### Development
```bash
python main.py
```

### Production
```bash
uvicorn main:app --host 0.0.0.0 --port 8000
```

## API Endpoints

### Root
- `GET /` - API information and available endpoints

### System Management
- `GET /status` - Get current Blaze system status
- `GET /health` - Health check endpoint
- `GET /blocks` - Get all active blocks

### Sequence Management
- `GET /sequences` - Get status of all sequences
- `GET /sequences/{seq_id}/status` - Get specific sequence status and last result
- `GET /sequences/{seq_id}/logs` - Get all execution logs for a sequence

### Job Management
- `GET /jobs` - Get all currently submitted jobs
- `POST /jobs/submit` - Submit a new job for a sequence

## API Documentation

Once the server is running, visit:
- Swagger UI: `http://localhost:8000/docs`
- ReDoc: `http://localhost:8000/redoc`

## Example Usage

### Get System Status
```bash
curl http://localhost:8000/status
```

### Submit a Job
```bash
curl -X POST "http://localhost:8000/jobs/submit" \
     -H "Content-Type: application/json" \
     -d '{
       "seq_id": "math_pipeline_1",
       "parameters": {
         "generate_numbers": {"count": 10, "min_val": 1, "max_val": 100}
       },
       "seq_run_interval": "*/5 * * * *"
     }'
```

### Get Sequence Status
```bash
curl http://localhost:8000/sequences/math_pipeline_1/status
```

### Get Sequence Logs
```bash
curl http://localhost:8000/sequences/math_pipeline_1/logs
```

## Data Sources

The API collects information from:
- `/tmp/scheduler_lock.json` - System status and active components
- `/tmp/scheduler_jobs.json` - Currently submitted jobs
- `./log/{seq_id}/state.json` - Sequence state and results
- `./log/{seq_id}/runs.json` - Execution history and logs

## Error Handling

- `503 Service Unavailable` - Blaze system is not running
- `404 Not Found` - Sequence or resource not found
- `400 Bad Request` - Invalid request parameters
- `500 Internal Server Error` - Server error

## Development

The API is built with:
- FastAPI for the web framework
- Pydantic for data validation
- Standard library for file operations
- Blaze core modules for integration 