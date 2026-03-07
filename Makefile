.PHONY: help test coverage badge clean install lint format

help:
	@echo "Available commands:"
	@echo "  make test       - Run tests"
	@echo "  make coverage   - Run tests with coverage report"
	@echo "  make badge      - Generate coverage badge"
	@echo "  make clean      - Remove generated files"
	@echo "  make install    - Install package in development mode"
	@echo "  make lint       - Run linting checks"
	@echo "  make format     - Format code with ruff"

test:
	pytest tests/

coverage:
	pytest tests/ --cov=src/structframe --cov-report=term --cov-report=xml --cov-report=html
	@echo ""
	@echo "Coverage report saved to:"
	@echo "  - Terminal output above"
	@echo "  - coverage.xml (for CI/CD)"
	@echo "  - htmlcov/index.html (open in browser)"

badge:
	@echo "Generating coverage badge..."
	pytest tests/ --cov=src/structframe --cov-report=xml -q
	genbadge coverage -i coverage.xml -o coverage-badge.svg
	@echo "✓ Coverage badge generated: coverage-badge.svg"

clean:
	rm -rf htmlcov/
	rm -f coverage.xml coverage.json .coverage
	rm -rf .pytest_cache/
	rm -rf .mypy_cache/
	rm -rf src/*.egg-info/
	rm -rf build/ dist/
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true

install:
	pip install -e ".[dev,polars]"

lint:
	mypy src/structframe
	ruff check src/ tests/

format:
	ruff format src/ tests/
