#!/bin/bash

# MerchantIQ Run Script
# Launch the MerchantIQ Streamlit application

echo "🏪 MerchantIQ - Starting application..."
echo "===================================="

# Check if virtual environment exists
if [ ! -d "venv" ]; then
    echo "❌ Virtual environment not found. Please run setup first:"
    echo "   ./scripts/setup.sh"
    exit 1
fi

# Activate virtual environment
echo "🔄 Activating virtual environment..."
source venv/bin/activate

# Check if main app exists
if [ ! -f "app/merchantiq_dashboard.py" ]; then
    echo "❌ Main application not found at app/merchantiq_dashboard.py"
    echo "   Please ensure you're running from the project root directory"
    exit 1
fi

# Set environment variables for better Streamlit experience
export STREAMLIT_BROWSER_GATHER_USAGE_STATS=false
export STREAMLIT_SERVER_HEADLESS=true

# Create logs directory if it doesn't exist
mkdir -p logs

echo "🚀 Launching MerchantIQ Dashboard..."
echo "   📊 Dashboard will be available at: http://localhost:8500"
echo "   ⏹️  Press Ctrl+C to stop the application"
echo ""

# Launch Streamlit with custom configuration
streamlit run app/merchantiq_dashboard.py \
    --server.port 8500 \
    --server.address localhost \
    --browser.gatherUsageStats false \
    --logger.level info \
    --server.fileWatcherType none