# VSCode devcontainer config.
git config devcontainers-theme.show-dirty 1

# pg access.
echo alias psql=\'psql -h localhost -U postgres\' >> ~/.bashrc

# rust precommit hook requirements.
rustup component add clippy

# precommit hook requirement.
pre-commit install -t pre-push
