FROM python:3.12-slim

# Setting the dir in the container
WORKDIR /Users/Tibo85/Documents/taxfix

# Copying the current dir contents into the container at /Users/Tibo85/Documents/taxfix
COPY . .

# Installing packages
RUN pip install --no-cache-dir -r requirements.txt

# Running the main script when the container launches
CMD ["python", "./main.py"]
