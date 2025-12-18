#!/bin/bash

# Check if .venv directory exists
if [ ! -d ".venv" ]; then
    echo "Virtual environment not found. Creating one..."
    python3 -m venv .venv
    if [ $? -ne 0 ]; then
        echo "Error: Failed to create virtual environment"
        exit 1
    fi
    echo "Virtual environment created successfully"
    
    # Activate and install dependencies
    echo "Installing dependencies..."
    source .venv/bin/activate
    pip install -r requirements.txt
    if [ $? -ne 0 ]; then
        echo "Error: Failed to install dependencies"
        exit 1
    fi
    echo "Dependencies installed successfully"
fi

# Activate virtual environment
source .venv/bin/activate

# If new dependencies were added (e.g., httpx for Spotify OAuth), ensure they're installed
python3 -c "import httpx" >/dev/null 2>&1
if [ $? -ne 0 ]; then
    echo "Missing dependencies detected (e.g., httpx). Installing/updating requirements..."
    pip install -r requirements.txt
    if [ $? -ne 0 ]; then
        echo "Error: Failed to install/update dependencies"
        exit 1
    fi
fi

# Run the application
python3 main.py
