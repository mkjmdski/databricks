.PHONY: help install lint format lint-fix check clean test

# Default target
help:
	@echo "Wheelie Data Warehouse - Makefile Commands"
	@echo "=========================================="
	@echo "make install      - Install development dependencies"
	@echo "make lint         - Run linter (check only)"
	@echo "make format       - Format code with Black"
	@echo "make lint-fix     - Run linter with auto-fix"
	@echo "make check        - Run both format check and linting"
	@echo "make clean        - Remove virtual environment and cache files"
	@echo "make test         - Run tests (when implemented)"

# Install dependencies from pyproject.toml
install:
	@echo "Installing development dependencies..."
	if [! -d .venv ]; then python3 -m venv .venv; fi
	source .venv/bin/activate
	pip install --upgrade pip
	pip install black ruff pre-commit pytest

# Run Black formatter (check only)
format:
	@echo "Formatting code with Black..."
	source .venv/bin/activate
	black notebooks/helpers/ --line-length 120

# Check formatting without changes
format-check:
	@echo "Checking code format..."
	source .venv/bin/activate
	black notebooks/helpers/ --check --line-length 120

# Run Ruff linter (check only)
lint:
	@echo "Running Ruff linter..."
	source .venv/bin/activate
	ruff check notebooks/helpers/

# Run Ruff linter with auto-fix
lint-fix:
	@echo "Running Ruff linter with auto-fix..."
	source .venv/bin/activate
	ruff check notebooks/helpers/ --fix

# Run all checks (format + lint)
check: format-check lint
	@echo "✅ All checks passed!"

# Clean up
clean:
	@echo "Cleaning up..."
	find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
	find . -type f -name "*.pyc" -delete
	find . -type d -name ".pytest_cache" -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name ".ruff_cache" -exec rm -rf {} + 2>/dev/null || true
	@echo "✅ Cleanup complete"

# Run tests (placeholder for future implementation)
test:
	@echo "Running tests..."
	pytest tests/ -v
