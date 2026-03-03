"""Vercel serverless function entry point"""
import sys
import os
from pathlib import Path

# Add the parent directory to the path
parent_dir = str(Path(__file__).parent.parent)
if parent_dir not in sys.path:
    sys.path.insert(0, parent_dir)

# Set environment variable for Vercel
os.environ['VERCEL'] = '1'

# Import the Flask app
try:
    from app import app
except ImportError as e:
    print(f"Import error: {e}")
    print(f"Python path: {sys.path}")
    print(f"Current directory: {os.getcwd()}")
    print(f"Directory contents: {os.listdir('.')}")
    raise

# Export for Vercel
application = app

# For local testing
if __name__ == "__main__":
    app.run(debug=True)
