#!/bin/bash
# Setup script for Counterfactual Time Series Analysis project
# Installs all necessary Python packages

echo "=========================================="
echo "Counterfactual Time Series Analysis - Setup"
echo "=========================================="
echo ""

# Check if Python 3 is available
if ! command -v python3 &> /dev/null; then
    echo " Error: Python 3 is not installed."
    echo "Please install Python 3.8 or higher."
    exit 1
fi

echo " Python 3 found: $(python3 --version)"
echo ""

# Check if pip is available
if ! command -v pip3 &> /dev/null; then
    echo " Error: pip3 is not installed."
    echo "Please install pip3."
    exit 1
fi

echo " pip3 found: $(pip3 --version)"
echo ""

# Install packages from requirements.txt
echo "Installing packages from requirements.txt..."
echo ""

if pip3 install -r requirements.txt; then
    echo ""
    echo "=========================================="
    echo " Setup complete!"
    echo "=========================================="
    echo ""
    echo "Installed packages:"
    pip3 list | grep -E "(pandas|numpy|requests)"
    echo ""
    echo "You can now run the test scripts:"
    echo "  python3 tests/test_simple.py"
    echo "  python3 tests/test_counterfactuals.py"
else
    echo ""
    echo " Error: Failed to install packages."
    exit 1
fi

