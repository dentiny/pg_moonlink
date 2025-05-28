FROM mcr.microsoft.com/devcontainers/rust:latest

# Run system-level installs as root.
USER root

RUN apt-get update && \
    apt-get install -y python3-pip && \
    pip3 install pre-commit --break-system-packages

# Now switch to vscode user for cargo installations, otherwise later cargo commands required root permission.
USER vscode

RUN cargo install cargo-sort

WORKDIR /workspaces/pg_moonlink
