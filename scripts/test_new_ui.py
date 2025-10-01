#!/usr/bin/env python3
"""
Test New UI Design
Starts the application with the new layout design
"""

import subprocess
import sys
import time
from pathlib import Path

def main():
    """Start the new UI design"""
    print("🚀 Starting NYC Housing Hub with new UI design...")
    
    # Check if we're in the right directory
    if not (Path.cwd() / "frontend").exists():
        print("❌ Please run this script from the project root directory")
        sys.exit(1)
    
    # Install new dependencies
    print("📦 Installing new dependencies...")
    try:
        subprocess.run([
            sys.executable, "-m", "pip", "install", 
            "folium==0.15.0", "streamlit-folium==0.15.0"
        ], check=True)
        print("✅ Dependencies installed")
    except subprocess.CalledProcessError as e:
        print(f"❌ Failed to install dependencies: {e}")
        sys.exit(1)
    
    # Start Streamlit with new app
    print("🌐 Starting Streamlit with new UI...")
    try:
        subprocess.run([
            sys.executable, "-m", "streamlit", 
            "run", "frontend/app_new.py",
            "--server.port", "8501",
            "--server.address", "0.0.0.0"
        ])
    except KeyboardInterrupt:
        print("👋 Shutting down...")
        sys.exit(0)

if __name__ == "__main__":
    main()
