#!/bin/bash

echo "Creating Database..."
python3 createDatabase.py
sleep 0.5

echo "Running sqlfunctions.py..."
python3 sqlfunctions.py
sleep 0.5

echo "Running processData.py..."
python3 processData.py

sleep 0.5

echo "Running dataVisualization.py..."
python3 dataVisualization.py

echo "All scripts executed successfully!"
