FROM flink:1.18

USER root

# Install Python, pip, and build dependencies including JDK with headers
RUN apt-get update && \
    apt-get install -y \
    python3 \
    python3-pip \
    python3-dev \
    openjdk-11-jdk \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Set JAVA_HOME for pemja compilation
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-arm64

# Install PyFlink (this will compile pemja with proper Java headers)
RUN pip3 install --no-cache-dir apache-flink==1.18.0

USER flink

