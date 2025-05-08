# VSCode devcontainer config.
git config devcontainers-theme.show-dirty 1

# pg access.
echo alias psql=\'psql -h localhost -U postgres\' >> ~/.bashrc

# rust precommit hook requirements.
sudo apt update -y
cargo install cargo-sort

# precommit hook requirement.
sudo apt install -y python3-pip
pip3 install pre-commit --break-system-packages
pre-commit install -t pre-push
