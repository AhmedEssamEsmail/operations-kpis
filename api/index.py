"""Vercel serverless function entry point"""
import sys
from pathlib import Path

# Add the parent directory to the path so we can import from src
sys.path.insert(0, str(Path(__file__).parent.parent))

from app import app

# Vercel expects a variable named 'app' or a handler function
# This is the WSGI application that Vercel will use
