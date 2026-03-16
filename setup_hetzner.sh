#!/bin/bash

# Exit on error
set -e

echo "🚀 Starting server setup..."

# 1. Update system
sudo apt update && sudo apt upgrade -y

apt install make

# 2. Setup Swap (Safety net for RAM)
if [ ! -f /swapfile ]; then
    echo "💾 Creating 2GB Swap file..."
    sudo fallocate -l 2G /swapfile
    sudo chmod 600 /swapfile
    sudo mkswap /swapfile
    sudo swapon /swapfile
    echo '/swapfile none swap sw 0 0' | sudo tee -a /etc/fstab
else
    echo "✅ Swap file already exists."
fi

# 3. Install Docker if not present
if ! command -v docker &> /dev/null; then
    echo "🐳 Installing Docker..."
    curl -fsSL https://get.docker.com | sh
else
    echo "✅ Docker is already installed."
fi

echo "🎉 Setup complete! You can now run: docker compose up --build -d"

# 3. docker network
docker network create movie-network