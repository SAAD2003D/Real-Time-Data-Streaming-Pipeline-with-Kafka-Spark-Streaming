FROM debian:bullseye

# Install Python, Java, and other dependencies
RUN apt-get update && apt-get install -y \
    python3 \
    python3-pip \
    openjdk-11-jdk \
    --no-install-recommends

# Set Python and Java as default
RUN update-alternatives --install /usr/bin/python python /usr/bin/python3 1

# Set environment variables for Java
ENV JAVA_HOME="/usr/lib/jvm/java-11-openjdk-amd64"
ENV PATH="$PATH:/usr/lib/jvm/java-11-openjdk-amd64/bin"

# Set working directory
WORKDIR /app

# Copy application files
COPY ./jobs /app
COPY ./requirements.txt /app

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt 

# Default command
CMD ["python", "consumer.py"]
