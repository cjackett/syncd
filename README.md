# syncd

A lightweight Linux daemon and CLI for safe rsync-based code sync over SSH

## Development
- This repo uses pre-commit hooks; see `.pre-commit-config.yaml` and run `pre-commit install` before contributing.
- Design/spec docs: `DESIGN.md`, `CONFIG.md`, `IPC.md`, `CLI.md`, `STATE.md`, `TESTPLAN.md`.
- Current status: CLI/daemon/watch loop wiring is implemented; remaining work is hardening and end-to-end validation.
