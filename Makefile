# Makefile for MuzaQuest Mini App

.PHONY: help install install-dev test lint format clean build docker-build docker-run docs

# Default target
help:
	@echo "Available targets:"
	@echo "  install      - Install production dependencies"
	@echo "  install-dev  - Install development dependencies"
	@echo "  test         - Run tests"
	@echo "  test-cov     - Run tests with coverage"
	@echo "  lint         - Run linting checks"
	@echo "  format       - Format code"
	@echo "  clean        - Clean build artifacts"
	@echo "  build        - Build package"
	@echo "  docker-build - Build Docker image"
	@echo "  docker-run   - Run Docker container"
	@echo "  docs         - Build documentation"
	@echo "  serve-docs   - Serve documentation locally"
	@echo "  security     - Run security checks"
	@echo "  pre-commit   - Run pre-commit hooks"

# Python and pip
PYTHON := python3
PIP := pip3

# Installation
install:
	$(PIP) install -r requirements.txt

install-dev:
	$(PIP) install -r requirements-dev.txt
	$(PIP) install -e .
	pre-commit install

# Testing
test:
	pytest tests/ -v

test-cov:
	pytest tests/ -v --cov=. --cov-report=html --cov-report=term-missing

test-watch:
	pytest-watch tests/ -v

# Code quality
lint:
	flake8 .
	mypy . --ignore-missing-imports
	bandit -r . -f json -o bandit-report.json

format:
	black .
	isort .

format-check:
	black --check .
	isort --check-only .

# Security
security:
	bandit -r . -f json -o bandit-report.json
	safety check

# Pre-commit
pre-commit:
	pre-commit run --all-files

# Build
clean:
	rm -rf build/
	rm -rf dist/
	rm -rf *.egg-info/
	rm -rf .pytest_cache/
	rm -rf .coverage
	rm -rf htmlcov/
	rm -rf .mypy_cache/
	find . -type d -name __pycache__ -exec rm -rf {} +
	find . -type f -name "*.pyc" -delete

build: clean
	$(PYTHON) -m build

# Docker
docker-build:
	docker build -t muzaquest-mini-app .

docker-run:
	docker run -p 8000:8000 muzaquest-mini-app

docker-compose-up:
	docker-compose up -d

docker-compose-down:
	docker-compose down

# Documentation
docs:
	cd docs && make html

serve-docs:
	cd docs/_build/html && python -m http.server 8080

# Development
dev-server:
	uvicorn main:app --reload --host 0.0.0.0 --port 8000

# Database
db-migrate:
	alembic upgrade head

db-revision:
	alembic revision --autogenerate -m "$(message)"

# Monitoring
profile:
	python -m cProfile -o profile.stats main.py

# Release
release-patch:
	bumpversion patch

release-minor:
	bumpversion minor

release-major:
	bumpversion major

# CI/CD helpers
ci-install:
	$(PIP) install -r requirements.txt -r requirements-dev.txt

ci-test:
	pytest tests/ -v --cov=. --cov-report=xml --junitxml=test-results.xml

ci-lint:
	flake8 . --format=junit-xml --output-file=flake8-results.xml
	mypy . --ignore-missing-imports --junit-xml=mypy-results.xml

# All checks (useful for CI)
check: format-check lint test security

# Quick development setup
setup: install-dev
	@echo "Development environment setup complete!"
	@echo "Run 'make test' to verify everything is working."