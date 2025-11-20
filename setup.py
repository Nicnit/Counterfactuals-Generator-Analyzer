#!/usr/bin/env python3
"""
Setup script for Counterfactual Time Series Analysis project.
Installs all necessary Python packages from requirements.txt
"""

import subprocess
import sys
import os

def check_python_version():
    """Check if Python version is 3.8 or higher"""
    if sys.version_info < (3, 8):
        print(" Error: Python 3.8 or higher is required.")
        print(f"Current version: {sys.version}")
        return False
    return True

def install_requirements():
    """Install packages from requirements.txt"""
    requirements_file = os.path.join(os.path.dirname(__file__), 'requirements.txt')
    
    if not os.path.exists(requirements_file):
        print(f" Error: {requirements_file} not found.")
        return False
    
    print("Installing packages from requirements.txt...")
    print("")
    
    try:
        subprocess.check_call([
            sys.executable, '-m', 'pip', 'install', '-r', requirements_file
        ])
        return True
    except subprocess.CalledProcessError:
        print(" Error: Failed to install packages.")
        return False

def verify_installations():
    """Verify that key packages are installed"""
    required_packages = ['pandas', 'numpy', 'requests']
    missing = []
    
    for package in required_packages:
        try:
            __import__(package)
            print(f"   {package}")
        except ImportError:
            print(f"   {package} (missing)")
            missing.append(package)
    
    return len(missing) == 0

def main():
    print("=" * 50)
    print("Counterfactual Time Series Analysis - Setup")
    print("=" * 50)
    print("")
    
    # Check Python version
    if not check_python_version():
        sys.exit(1)
    
    print(f" Python {sys.version.split()[0]} detected")
    print("")
    
    # Install requirements
    if not install_requirements():
        sys.exit(1)
    
    print("")
    print("Verifying installations...")
    print("")
    
    if verify_installations():
        print("")
        print("=" * 50)
        print(" Setup complete!")
        print("=" * 50)
        print("")
        print("You can now run the test scripts:")
        print("  python3 tests/test_simple.py")
        print("  python3 tests/test_counterfactuals.py")
    else:
        print("")
        print(" Warning: Some packages may not be installed correctly.")
        print("Try running: pip3 install -r requirements.txt")

if __name__ == '__main__':
    main()

