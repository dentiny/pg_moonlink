FROM mcr.microsoft.com/devcontainers/rust:latest

RUN apt-get update && \
    cargo install cargo-sort && \
    apt install -y python3-pip && \
    pip3 install pre-commit --break-system-packages

WORKDIR /workspaces/pg_moonlink
