# Use an official Python runtime as a parent image
FROM python:3.8-slim-bullseye

# Set the working directory in the container to /app
WORKDIR /app

# Install the necessary system packages, including Proj
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    libpq-dev \
    libgeos-dev \
    libproj-dev \
    libeccodes-dev \
    curl \
    proj-bin \
    libproj-dev \
    && rm -rf /var/lib/apt/lists/*

# Upgrade pip and install numpy
RUN pip install --upgrade pip && \
    pip install numpy

# Copy the current directory contents into the container at /app
COPY . /app

# Install any needed packages specified in packages.txt
RUN pip install --no-cache-dir -r packages.txt

# Run your script when the container launches
CMD ["python3", "merger.py"]
