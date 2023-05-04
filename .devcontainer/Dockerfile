FROM ubuntu:20.04

ENV TERM xterm-256color
ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update -yq && \
    apt-get install -yq locales git wget rpm rpm2cpio cpio make build-essential binutils libtinfo5 libtool-bin libaio1 libaio-dev m4 && \
    printf 'en_US.UTF-8 UTF-8\n' > /etc/locale.gen && \
    locale-gen && \
    rm -rf /var/lib/apt/lists/*

ENV LANGUAGE en_US
ENV LANG en_US.UTF-8
ENV LC_ALL en_US.UTF-8
