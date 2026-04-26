# Contributing to typesense-langgraph-checkpointer

Thank you for considering contributing! This guide will help you get started.

## 📋 How to Contribute

### Reporting Bugs

Open an [issue](../../issues) with:

- A clear, descriptive title
- Steps to reproduce the problem
- Expected vs. actual behavior
- Your environment (OS, Python/Node version, Typesense version)

### Suggesting Features

Open an [issue](../../issues) labeled **enhancement** describing:

- The problem your feature would solve
- Your proposed solution
- Any alternatives you considered

### Submitting Pull Requests

1. **Fork** the repository
2. **Create a branch** from `main` (`git checkout -b feat/my-feature`)
3. **Make your changes** — follow existing code style
4. **Add tests** for any new functionality
5. **Run all tests** (see below) and ensure they pass
6. **Commit** with a clear message (`feat: add XYZ` / `fix: handle edge case`)
7. **Push** and open a Pull Request against `main`

## 🛠️ Development Setup

### Prerequisites

- [Docker](https://docs.docker.com/get-docker/) (for Typesense)
- **Python ≥ 3.9** + pip
- **Node.js ≥ 22** + npm

### 1. Clone & Start Typesense

```bash
git clone https://github.com/assim98/typesense-langgraph-checkpointer.git
cd typesense-langgraph-checkpointer
docker compose up -d
```

### 2. Python Package

```bash
cd python
python -m venv .venv && source .venv/bin/activate
pip install -e ".[dev]"
pytest tests/ -v
```

### 3. JavaScript / TypeScript Package

```bash
cd js
npm ci
npm run build
npm test
```

## 📝 Code Style

- **Python** — follow PEP 8; use type hints
- **TypeScript** — strict mode; prefer `async/await`

## 🔀 Commit Convention

Use [Conventional Commits](https://www.conventionalcommits.org/):

| Prefix | Purpose |
|---|---|
| `feat:` | New feature |
| `fix:` | Bug fix |
| `docs:` | Documentation only |
| `test:` | Adding or updating tests |
| `chore:` | Maintenance / tooling |

## 📜 License

By contributing, you agree that your contributions will be licensed under the [MIT License](LICENSE).
