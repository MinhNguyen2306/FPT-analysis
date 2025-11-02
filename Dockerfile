FROM python:3.9-slim

# Cài Java & procps (cho ps)
RUN apt-get update && \
    apt-get install -y openjdk-21-jre-headless procps && \
    rm -rf /var/lib/apt/lists/*

# Set JAVA_HOME cho PySpark
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
ENV PATH="$JAVA_HOME/bin:$PATH"

# Thư mục làm việc
WORKDIR /app

# Cài dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy project
COPY . .

# Default command (có thể override trong compose)
CMD ["python", "ETL_scripts.py"]

