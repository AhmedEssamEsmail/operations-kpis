"""WSGI entry point for Vercel deployment"""
from app import app

# Vercel expects the app to be named 'app'
if __name__ == "__main__":
    app.run()
