#!/bin/bash

# Function to kill Airflow processes
kill_airflow_processes() {
  pids=$(pgrep -f "airflow")
  if [ -n "$pids" ]; then
    echo "Killing Airflow processes..."
    for pid in $pids; do
      sudo kill "$pid"
    done
  fi
  exit
}

# Trap Ctrl+C and call the kill_airflow_processes function
trap 'kill_airflow_processes' INT

# Check if the virtual environment is already activated
if [ -z "$VIRTUAL_ENV" ]; then
    source venv/bin/activate
fi

# Set environment variables
source .env
export AIRFLOW_HOME=$(pwd)
export NO_PROXY="*"
database_url=$(echo $AIRFLOW__DATABASE__SQL_ALCHEMY_CONN)
export AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=$database_url

# Start the Airflow scheduler in the background
airflow scheduler &

# Wait for a moment to allow the scheduler to start
sleep 5

# Start the Airflow webserver in the background
airflow webserver &

# Wait for the Airflow webserver to start (you can adjust the delay if needed)
while ! nc -z localhost 8080; do
  sleep 1
done

# Open a web browser to the Airflow web interface
open "http://localhost:8080"

# Keep the script running
wait

