FROM ghcr.io/astral-sh/uv:python3.13-bookworm

ARG DEBIAN_FRONTEND=noninteractive
ENV HOME=/home/vscode
WORKDIR /app

# Ensure tools use a stable PATH and minimal layers
ENV UV_COMPILE_BYTECODE=1 UV_LINK_MODE=copy UV_TOOL_BIN_DIR=/usr/local/bin
ENV JAVA_HOME=/usr/lib/jvm/temurin-17-jdk-amd64
ENV HADOOP_HOME=/usr/local/hadoop
ENV PATH="$HADOOP_HOME/bin:/usr/local/bin:$PATH"

# Install system deps and Java 17 from Eclipse Adoptium
RUN apt-get update && apt-get install -y --no-install-recommends \
        ca-certificates curl gnupg2 lsb-release \
        build-essential wget unzip git bash \
    && wget -qO - https://packages.adoptium.net/artifactory/api/gpg/key/public | gpg --dearmor | tee /etc/apt/trusted.gpg.d/adoptium.gpg > /dev/null \
    && echo "deb https://packages.adoptium.net/artifactory/deb $(awk -F= '/^VERSION_CODENAME/{print$2}' /etc/os-release) main" | tee /etc/apt/sources.list.d/adoptium.list \
    && apt-get update \
    && apt-get install -y --no-install-recommends temurin-17-jdk \
    && rm -rf /var/lib/apt/lists/*

# Install Hadoop
RUN curl -fsSL https://downloads.apache.org/hadoop/common/hadoop-3.3.6/hadoop-3.3.6.tar.gz -o /tmp/hadoop.tar.gz \
        && tar -xzf /tmp/hadoop.tar.gz -C /usr/local/ \
        && mv /usr/local/hadoop-3.3.6 /usr/local/hadoop \
        && rm /tmp/hadoop.tar.gz

# Create a non-root user and give ownership of app and hadoop dirs
RUN useradd -ms /bin/bash vscode \
    && mkdir -p /app \
    && chown -R vscode:vscode /app /usr/local/hadoop /home/vscode

USER vscode

# Copy dependency files first for Docker layer caching
COPY --chown=vscode:vscode pyproject.toml README.md ./
COPY --chown=vscode:vscode configs/ ./configs/

# Install Python deps via uv as non-root (uv was installed system-wide)
RUN uv sync --compile-bytecode

# Copy remaining source files
COPY --chown=vscode:vscode . .

# Default command
CMD ["python3"]
