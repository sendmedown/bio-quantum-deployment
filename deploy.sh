#!/bin/bash

# Bio-Quantum AI Trading Platform Deployment Script

echo "🚀 Starting Bio-Quantum AI Trading Platform Deployment..."

# Check if we're in the right directory
if [ ! -f "README.md" ] || [ ! -d "frontend" ] || [ ! -d "backend" ]; then
    echo "❌ Error: Please run this script from the bio-quantum-trading-platform root directory"
    exit 1
fi

# Build Frontend
echo "📦 Building frontend..."
cd frontend/bio-quantum-ui

# Install dependencies if node_modules doesn't exist
if [ ! -d "node_modules" ]; then
    echo "📥 Installing frontend dependencies..."
    pnpm install
fi

# Build for production
echo "🔨 Building React app for production..."
pnpm run build

if [ $? -ne 0 ]; then
    echo "❌ Frontend build failed"
    exit 1
fi

# Copy build to backend static directory
echo "📋 Copying build files to backend..."
cd ../../
mkdir -p backend/bio_quantum_api/src/static
cp -r frontend/bio-quantum-ui/dist/* backend/bio_quantum_api/src/static/

# Setup Backend
echo "🐍 Setting up backend..."
cd backend/bio_quantum_api

# Create virtual environment if it doesn't exist
if [ ! -d "venv" ]; then
    echo "🔧 Creating Python virtual environment..."
    python3 -m venv venv
fi

# Activate virtual environment and install dependencies
echo "📥 Installing backend dependencies..."
source venv/bin/activate
pip install -r requirements.txt

if [ $? -ne 0 ]; then
    echo "❌ Backend dependency installation failed"
    exit 1
fi

echo "✅ Deployment preparation complete!"
echo ""
echo "🎯 To start the application:"
echo "   cd backend/bio_quantum_api"
echo "   source venv/bin/activate"
echo "   python src/main.py"
echo ""
echo "🌐 The application will be available at http://localhost:5000"
echo "📊 API endpoints will be available at http://localhost:5000/api/"
echo ""
echo "🔗 For Render deployment:"
echo "   - Build Command: cd backend/bio_quantum_api && pip install -r requirements.txt"
echo "   - Start Command: cd backend/bio_quantum_api && python src/main.py"
echo "   - Environment: Python 3.11"
echo ""
echo "📁 For GitHub deployment:"
echo "   - Push the entire bio-quantum-trading-platform directory"
echo "   - The frontend is already built and included in backend/src/static/"

