#!/bin/bash

# Exit immediately if a command exits with a non-zero status.
set -e

echo "🚀 Starting Deployment of Local Aviation Data Pipeline..."

# 1. Check for Docker
if ! command -v docker &> /dev/null
then
    echo "❌ Docker could not be found. Please install Docker."
    exit 1
fi

# 2. Deploy Airbyte
echo "📦 Deploying Airbyte..."
if [ ! -d "airbyte" ]; then
    echo "   Cloning Airbyte repository..."
    git clone https://github.com/airbytehq/airbyte.git
fi
cd airbyte
echo "   Checking out stable version v0.50.33..."
git fetch --all --tags
git checkout v0.50.33

echo "   Starting Airbyte..."
./run-ab-platform.sh -b
cd ..

# 3. Deploy other services
echo "📦 Deploying other services..."
docker compose -f docker-compose.yaml up -d --build

echo "✅ Deployment complete!"
echo "See README.md for post-deployment steps."
