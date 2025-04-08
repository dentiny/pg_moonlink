# Contributing

## Dev Container
The easiest way to start contributing is via our Dev Container. This container works locally in Visual Studio Code. To open the project in vscode you will need the [Dev Containers extension](https://marketplace.visualstudio.com/items?itemName=ms-vscode-remote.remote-containers).

First, you need to clone your fork repo to local directory and name it as `moonlink`.
Then, open the folder in devcontainer, with the extension installed you can run the following from the `Command Palette` to get started
```
> Dev Containers: Reopen in Container
```
This will create an isolated Workspace in vscode, including all tools required to build, test and run the `pg_moonlink`.

## Precommit hook
The following commands are used to setup precommit hooks.
```sh
sudo pip install pre-commit
pre-commit install -t pre-push
```
