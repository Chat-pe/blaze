#!/bin/bash

# Blaze Web API Startup Script

echo "Starting Blaze Web API..."

# Check if virtual environment exists
if [ -d "../../.venv" ]; then
    echo "Activating virtual environment..."
    source ../../.venv/bin/activate
fi

# Check if dependencies are installed
if ! python -c "import fastapi" 2>/dev/null; then
    echo "Installing dependencies..."
    pip install -r requirements.txt
fi

# Check if Blaze system is running
if [ ! -f "/tmp/scheduler_lock.json" ]; then
    echo "Warning: Blaze system may not be running. Lock file not found at /tmp/scheduler_lock.json"
    echo "Please start the Blaze scheduler first."
fi

# Set PYTHONPATH to include the project root
export PYTHONPATH="${PYTHONPATH}:$(pwd)/../.."

echo "Starting FastAPI server on http://localhost:8000"
echo "API Documentation available at:"
echo "  - Swagger UI: http://localhost:8000/docs"
echo "  - ReDoc: http://localhost:8000/redoc"
echo ""
echo "Press Ctrl+C to stop the server"

# Start the server
uvicorn main:app --host 0.0.0.0 --port 8000 --reload 