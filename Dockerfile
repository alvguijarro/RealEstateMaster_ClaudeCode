# Use official Python 3.12 image
FROM python:3.12-slim

# Set working directory
WORKDIR /app

# Install system dependencies required for Playwright and general usage
# - curl/wget: for downloading things if needed
# - lsof: used in our run_design_3.py for killing processes on Linux
# - Playwright system deps will be installed by "playwright install --with-deps" below,
#   but we need basic tools first.
RUN apt-get update && apt-get install -y \
    curl \
    lsof \
    && rm -rf /var/lib/apt/lists/*

# Copy only requirements first to leverage Docker cache
# We don't have a single requirements.txt in root, so we'll install manually 
# or copy the internal ones. For now, we'll install the known core deps.
RUN pip install --no-cache-dir \
    flask \
    Flask-BasicAuth \
    flask-socketio \
    google-generativeai \
    playwright \
    pandas \
    numpy \
    openpyxl \
    joblib \
    scikit-learn \
    requests \
    beautifulsoup4 \
    lxml

# Install Playwright browsers (Chromium only to save space, if possible, but standard is safer)
RUN playwright install --with-deps chromium

# Copy the entire application
COPY . .

# Create the output directory for persistent storage (volume mount point)
RUN mkdir -p /app/scraper/salidas

# Set environment variables
ENV PYTHONUNBUFFERED=1
ENV HEADLESS=1
ENV PORT=5004

# Expose the dashboard port (though Railway/Render handle this dynamically)
EXPOSE 5004

# Command to run the application
CMD ["python", "run_design_3.py"]
