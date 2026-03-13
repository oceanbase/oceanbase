# SeekDB-style CI worker image for OceanBase.
FROM ubuntu:22.04

ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update && apt-get install -y --no-install-recommends \
    bash \
    coreutils \
    findutils \
    ca-certificates \
    curl \
    wget \
    build-essential \
    gcc \
    g++ \
    make \
    python3 \
    rpm \
    cpio \
    zstd \
    git \
    file \
    binutils \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /workspace
ENTRYPOINT ["/bin/bash"]
