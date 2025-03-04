# UV Transition Guide

## Overview

This document provides guidance on transitioning from `pip` to `uv` as your Python package manager in the Bettensor project. UV is a modern, fast package manager for Python, written in Rust, that offers significant performance improvements over traditional pip.

## Why UV?

UV provides several key benefits:

- **Speed**: UV can install packages 10-100x faster than pip
- **Reliability**: Better dependency resolution with fewer conflicts
- **Performance**: Rust-powered implementation for efficient resource usage
- **Native Virtual Environment Management**: Create and manage virtual environments directly with UV
- **Modern Standards**: First-class support for pyproject.toml and modern Python packaging

## Migration Process

We've provided a script to help you migrate your existing pip environment to UV. This script will:

1. Install UV if it's not already installed
2. Create a backup of your current dependencies
3. Reinstall your dependencies using UV
4. Provide information on how to use UV going forward

To migrate your environment:

```bash
bash scripts/migrate_to_uv.sh
```

## UV Workflow

UV provides both a native approach and a pip compatibility mode. Here's how the UV commands compare to pip:

| Task | pip | UV |
|------|-----|-----------|
| Create venv | `python -m venv .venv` | `uv venv` |
| Install package | `pip install package_name` | `uv pip install package_name` |
| Install from requirements | `pip install -r requirements.txt` | `uv pip install -r requirements.txt` |
| Editable install | `pip install -e .` | `uv pip install -e .` |
| Sync all dependencies | N/A | `uv sync` |

### Benefits of UV Commands

1. **Speed**: UV commands are optimized for maximum performance
2. **Dependency Resolution**: Improved handling of complex dependency trees
3. **Reproducibility**: More consistent environments across systems
4. **Error Handling**: More informative error messages

## Common UV Commands

Here are some common UV commands you'll use:

### Installation

```bash
# Install a package
uv pip install package_name

# Install a package with pre-release versions allowed
uv pip install package_name --pre

# Install the current project in editable mode
uv pip install -e .

# Install from requirements.txt
uv pip install -r requirements.txt
```

### Virtual Environment Management

```bash
# Create a virtual environment in .venv
uv venv

# Create a virtual environment in a custom location
uv venv /path/to/venv
```

### Dependency Management

```bash
# Synchronize dependencies
uv sync

# Allow pre-release versions when syncing
uv sync --prerelease=allow
```

## Troubleshooting

If you encounter issues with UV:

1. Try using the `--pre` flag with `uv pip install` commands if you're working with pre-release dependencies
2. Use `--prerelease=allow` flag with `uv sync` for the same purpose
3. Check the official [UV documentation](https://github.com/astral-sh/uv) for detailed information
4. If UV fails, our scripts will automatically fall back to pip

## FAQ

**Q: Should I use UV for all new installs?**  
A: Yes, we recommend using UV for all new installations. It provides significant performance improvements and better dependency resolution.

**Q: How does this affect my existing environments?**  
A: Your existing pip environments will continue to work. You can migrate them to UV using the provided migration script. The script includes fallback mechanisms to ensure your environments remain functional.

**Q: What if I encounter issues with UV?**  
A: UV is a newer tool, so you might encounter some issues. Our scripts include fallback mechanisms to pip if UV fails. If you encounter persistent issues:

1. Try using `uv pip install` commands
2. If you need to revert to pip, you can still use it as before
3. Report any issues in our GitHub repository

**Q: How do I handle pre-release dependencies?**  
A: Bittensor uses some pre-release dependencies (like `bt-decode==0.5.0a2`). When using UV, you need to allow pre-release installations using one of these methods:

For UV pip install:
```bash
uv pip install package_name --pre
```

For UV sync:
```bash
uv sync --prerelease=allow
```

Our migration and setup scripts have been updated to handle these pre-release dependencies automatically.

**Q: Can I still contribute to the project if I don't use UV?**  
A: Absolutely! All our scripts maintain backward compatibility with pip. While UV is recommended for better performance, using pip will not prevent you from contributing to the project.

**Q: What's the difference between `uv pip install -e .` and `pip install -e .`?**  
A: They accomplish the same goal of installing a package in editable/development mode, but `uv pip install -e .` is significantly faster and has better dependency resolution. While it uses UV's pip compatibility mode, it still offers major performance benefits over traditional pip.

## Getting Help

If you need assistance with UV:

- Consult the [UV documentation](https://github.com/astral-sh/uv)
- Ask in our community channels
- Check for known issues in our issue tracker 