#!/bin/bash

# MerchantIQ Setup Script
# Automated environment setup for MerchantIQ application

echo "🏪 MerchantIQ - Setting up your environment..."
echo "============================================="

# Check Python version
echo "📋 Checking Python version..."
if ! command -v python3 &> /dev/null; then
    echo "❌ Python 3 is not installed. Please install Python 3.8 or higher."
    exit 1
fi

python_version=$(python3 --version 2>&1 | awk '{print $2}')
echo "✅ Found Python $python_version"

# Create virtual environment
echo "🛠️  Creating virtual environment..."
if [ ! -d "venv" ]; then
    python3 -m venv venv
    echo "✅ Virtual environment created"
else
    echo "✅ Virtual environment already exists"
fi

# Activate virtual environment
echo "🔄 Activating virtual environment..."
source venv/bin/activate

# Upgrade pip
echo "⬆️  Upgrading pip..."
pip install --upgrade pip

# Install dependencies
echo "📦 Installing dependencies..."
if [ -f "requirements.txt" ]; then
    pip install -r requirements.txt
    echo "✅ Dependencies installed successfully"
else
    echo "❌ requirements.txt not found"
    exit 1
fi

# Create necessary directories
echo "📁 Creating directories..."
mkdir -p assets/screenshots
mkdir -p logs
echo "✅ Directories created"

# Check if app files exist
echo "🔍 Checking application files..."
if [ -f "app/merchantiq_dashboard.py" ]; then
    echo "✅ Main application found"
else
    echo "❌ Main application file not found at app/merchantiq_dashboard.py"
    exit 1
fi

if [ -f "app/live_mcp_connector.py" ]; then
    echo "✅ Data connector found"
else
    echo "❌ Data connector file not found at app/live_mcp_connector.py"
    exit 1
fi

# Test basic imports
echo "🧪 Testing dependencies..."
python3 -c "
try:
    import streamlit
    import pandas
    import plotly
    import numpy
    print('✅ All core dependencies working')
except ImportError as e:
    print(f'❌ Import error: {e}')
    exit(1)
"

echo ""
echo "🎉 Setup completed successfully!"
echo "============================================="
echo "📝 Next steps:"
echo "   1. Run: ./scripts/run.sh"
echo "   2. Open: http://localhost:8500"
echo "   3. Enjoy MerchantIQ!"
echo ""
echo "💡 Need help? Check the README.md for more information."