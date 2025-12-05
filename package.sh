#!/bin/bash

echo "Packaging PyFlink job..."

# Create a zip file with the Python code
cd transaction_analytics
zip -r ../transaction_analytics.zip . -x "*.pyc" -x "__pycache__/*"
cd ..

echo "PyFlink job packaged successfully: transaction_analytics.zip"
echo "You can submit this to Flink cluster"

