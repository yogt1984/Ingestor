#!/bin/bash

set -e  # Exit on error

echo "🔧 Setting up Python environment for parquet testing notebooks..."

# Check for python3
if ! command -v python3 &> /dev/null; then
    echo "❌ python3 is not installed. Please install Python 3.8+ first."
    exit 1
fi

PYTHON_VERSION=$(python3 --version | cut -d' ' -f2 | cut -d'.' -f1,2)
echo "✓ Found python3 version: $(python3 --version)"

# Determine pip command (pip3 or pip)
if command -v pip3 &> /dev/null; then
    PIP_CMD="pip3"
elif command -v pip &> /dev/null; then
    PIP_CMD="pip"
else
    echo "❌ Neither pip nor pip3 found. Installing pip..."
    python3 -m ensurepip --upgrade || python3 -m pip --version || {
        echo "❌ Failed to install pip. Please install pip manually."
        exit 1
    }
    PIP_CMD="python3 -m pip"
fi

echo "✓ Using pip command: $PIP_CMD"

# Required packages
REQUIRED_PACKAGES=(
    "polars>=0.19.0"
    "pandas>=1.5.0"
    "numpy>=1.20.0"
    "matplotlib>=3.5.0"
    "seaborn>=0.12.0"
    "jupyter>=1.0.0"
    "notebook>=6.0.0"
)

echo ""
echo "📦 Checking and installing required packages..."

for package in "${REQUIRED_PACKAGES[@]}"; do
    # Extract package name (before >=)
    pkg_name=$(echo "$package" | cut -d'>' -f1)
    
    if $PIP_CMD show "$pkg_name" &> /dev/null; then
        INSTALLED_VERSION=$($PIP_CMD show "$pkg_name" | grep "^Version:" | cut -d' ' -f2)
        echo "✓ $pkg_name is already installed (version $INSTALLED_VERSION)"
    else
        echo "📥 Installing $package..."
        $PIP_CMD install --upgrade "$package" || {
            echo "❌ Failed to install $package"
            exit 1
        }
        echo "✓ Installed $package"
    fi
done

echo ""
echo "🧪 Verifying installation..."

# Quick import test
python3 << EOF
import sys
try:
    import polars as pl
    import pandas as pd
    import numpy as np
    import matplotlib.pyplot as plt
    import seaborn as sns
    import json
    from pathlib import Path
    print("✓ All required packages imported successfully")
    print(f"  - polars: {pl.__version__}")
    print(f"  - pandas: {pd.__version__}")
    print(f"  - numpy: {np.__version__}")
    print(f"  - matplotlib: {plt.matplotlib.__version__}")
    print(f"  - seaborn: {sns.__version__}")
except ImportError as e:
    print(f"❌ Import error: {e}")
    sys.exit(1)
EOF

if [ $? -eq 0 ]; then
    echo ""
    echo "✅ Setup completed successfully!"
    echo ""
    echo "You can now run the Jupyter notebook:"
    echo "  cd notebooks"
    echo "  jupyter notebook test_parquet_features.ipynb"
    echo ""
    echo "Or start Jupyter in the notebooks directory:"
    echo "  cd notebooks"
    echo "  jupyter notebook"
else
    echo ""
    echo "❌ Setup verification failed. Please check the errors above."
    exit 1
fi

