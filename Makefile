.PHONY: help setup install start stop clean test lint format validate all

# Default target
.DEFAULT_GOAL := help

# Variables
DOCKER_COMPOSE := docker-compose
PYTHON := python3
PIP := pip3
PROJECT_ROOT := $(shell pwd)

# Colors for output
BLUE := \033[0;34m
GREEN := \033[0;32m
YELLOW := \033[1;33m
RED := \033[0;31m
NC := \033[0m # No Color

help: ## Show this help message
	@echo "$(BLUE)Streaming Pipeline - Makefile Commands$(NC)"
	@echo ""
	@echo "$(GREEN)Setup & Installation:$(NC)"
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  $(YELLOW)%-20s$(NC) %s\n", $$1, $$2}'

setup: ## One-command setup: install dependencies and prepare environment
	@echo "$(BLUE)========================================$(NC)"
	@echo "$(BLUE)Setting up Streaming Pipeline$(NC)"
	@echo "$(BLUE)========================================$(NC)"
	@echo ""
	@echo "$(GREEN)Step 1: Installing Python dependencies...$(NC)"
	@if [ -f "ingestion/producer/requirements.txt" ]; then $(PIP) install -r ingestion/producer/requirements.txt; fi
	@if [ -f "processing/test_requirements.txt" ]; then $(PIP) install -r processing/test_requirements.txt; fi
	@echo ""
	@echo "$(GREEN)Step 2: Making scripts executable...$(NC)"
	@chmod +x scripts/*.sh
	@echo ""
	@echo "$(GREEN)Step 3: Checking Docker...$(NC)"
	@command -v docker >/dev/null 2>&1 || { echo "$(RED)Error: Docker is not installed$(NC)"; exit 1; }
	@command -v docker-compose >/dev/null 2>&1 || { echo "$(RED)Error: docker-compose is not installed$(NC)"; exit 1; }
	@echo "$(GREEN)✓ Docker is available$(NC)"
	@echo ""
	@echo "$(GREEN)Step 4: Building Docker images...$(NC)"
	@$(DOCKER_COMPOSE) build
	@echo ""
	@echo "$(GREEN)✓ Setup completed!$(NC)"
	@echo ""
	@echo "$(YELLOW)Next steps:$(NC)"
	@echo "  make start    - Start all services"
	@echo "  make test     - Run tests"
	@echo "  make help     - Show all commands"

install: ## Install Python dependencies
	@echo "$(GREEN)Installing dependencies...$(NC)"
	@if [ -f "ingestion/producer/requirements.txt" ]; then $(PIP) install -r ingestion/producer/requirements.txt; fi
	@if [ -f "processing/test_requirements.txt" ]; then $(PIP) install -r processing/test_requirements.txt; fi
	@echo "$(GREEN)✓ Dependencies installed$(NC)"

build: ## Build Docker images
	@echo "$(GREEN)Building Docker images...$(NC)"
	@$(DOCKER_COMPOSE) build
	@echo "$(GREEN)✓ Docker images built$(NC)"

start: ## Start all services
	@echo "$(GREEN)Starting all services...$(NC)"
	@./scripts/run_all.sh

stop: ## Stop all services
	@echo "$(YELLOW)Stopping all services...$(NC)"
	@./scripts/kill_all.sh

restart: stop start ## Restart all services

status: ## Check status of all services
	@./scripts/check_services.sh

# Producer commands
producer: ## Start Kafka producer
	@./scripts/start_producer.sh

# Streaming commands
streaming: ## Start Spark streaming job
	@./scripts/start_streaming.sh

# Query commands
query: ## Run percentile query (requires S3_BUCKET or INPUT_PATH)
	@if [ -z "$(S3_BUCKET)" ] && [ -z "$(INPUT_PATH)" ]; then \
		echo "$(RED)Error: S3_BUCKET or INPUT_PATH must be set$(NC)"; \
		echo "Usage: make query S3_BUCKET=my-bucket"; \
		exit 1; \
	fi
	@./scripts/run_percentile_query.sh

# Validation commands
validate-output: ## Validate query output
	@./scripts/validate_output.sh

validate-s3: ## Validate S3 data (requires S3_BUCKET)
	@if [ -z "$(S3_BUCKET)" ]; then \
		echo "$(RED)Error: S3_BUCKET must be set$(NC)"; \
		exit 1; \
	fi
	@./scripts/validate_s3_data.sh

# Storage setup
setup-s3: ## Set up AWS S3 bucket
	@./scripts/setup_s3_bucket.sh

setup-minio: ## Set up MinIO bucket (default, local S3-compatible storage)
	@./storage/setup_minio_bucket.sh

# Testing
test: ## Run all tests
	@echo "$(GREEN)Running tests...$(NC)"
	@cd processing && $(PYTHON) -m pytest test_spark_streaming_job.py -v -m "not integration"

test-unit: ## Run unit tests only
	@cd processing && $(PYTHON) -m pytest test_spark_streaming_job.py -v -m "unit"

test-integration: ## Run integration tests
	@cd processing && $(PYTHON) -m pytest test_integration.py -v -m "integration"

test-coverage: ## Run tests with coverage
	@cd processing && $(PYTHON) -m pytest --cov=spark_streaming_job --cov-report=html test_spark_streaming_job.py
	@echo "$(GREEN)Coverage report generated in processing/htmlcov/$(NC)"

# Linting and formatting
lint: ## Run linting checks
	@echo "$(GREEN)Running linting...$(NC)"
	@command -v pylint >/dev/null 2>&1 || { echo "$(YELLOW)Warning: pylint not installed$(NC)"; exit 0; }
	@find . -name "*.py" -not -path "./venv/*" -not -path "./.venv/*" -exec pylint {} \;

format: ## Format Python code
	@echo "$(GREEN)Formatting code...$(NC)"
	@command -v black >/dev/null 2>&1 || { echo "$(YELLOW)Warning: black not installed$(NC)"; exit 0; }
	@find . -name "*.py" -not -path "./venv/*" -not -path "./.venv/*" -exec black {} \;

# Cleanup
clean: ## Clean temporary files and caches
	@echo "$(YELLOW)Cleaning temporary files...$(NC)"
	@find . -type d -name "__pycache__" -exec rm -r {} + 2>/dev/null || true
	@find . -type f -name "*.pyc" -delete
	@find . -type f -name "*.pyo" -delete
	@find . -type d -name "*.egg-info" -exec rm -r {} + 2>/dev/null || true
	@find . -type d -name ".pytest_cache" -exec rm -r {} + 2>/dev/null || true
	@rm -rf .coverage htmlcov/ .tox/ dist/ build/
	@echo "$(GREEN)✓ Cleanup completed$(NC)"

clean-docker: ## Clean Docker containers and volumes
	@echo "$(YELLOW)Cleaning Docker resources...$(NC)"
	@$(DOCKER_COMPOSE) down -v
	@docker system prune -f
	@echo "$(GREEN)✓ Docker cleanup completed$(NC)"

clean-all: clean clean-docker ## Clean everything (files and Docker)

# Logs
logs: ## Show logs from all services
	@$(DOCKER_COMPOSE) logs -f

logs-streaming: ## Show streaming job logs
	@$(DOCKER_COMPOSE) logs -f spark-streaming

logs-kafka: ## Show Kafka logs
	@$(DOCKER_COMPOSE) logs -f kafka

logs-spark: ## Show Spark logs
	@$(DOCKER_COMPOSE) logs -f spark-master spark-worker-1 spark-worker-2

# Monitoring
monitor: ## Open monitoring dashboards
	@echo "$(GREEN)Monitoring URLs:$(NC)"
	@echo "  Spark UI: http://localhost:8080"
	@echo "  Prometheus: http://localhost:9090"
	@echo "  Grafana: http://localhost:3000 (admin/admin)"
	@echo ""
	@echo "$(YELLOW)Opening in browser...$(NC)"
	@command -v open >/dev/null 2>&1 && open http://localhost:8080 || true
	@command -v xdg-open >/dev/null 2>&1 && xdg-open http://localhost:8080 || true

# Full pipeline
pipeline: setup start producer ## Complete pipeline: setup, start services, and producer

# Development
dev-setup: setup ## Development setup with all tools
	@echo "$(GREEN)Installing development tools...$(NC)"
	@$(PIP) install black pylint pytest pytest-cov mypy || true
	@echo "$(GREEN)✓ Development setup completed$(NC)"

# Quick commands
quick-start: ## Quick start: build and start services
	@$(DOCKER_COMPOSE) build
	@./scripts/run_all.sh

quick-stop: ## Quick stop: stop all services
	@./scripts/kill_all.sh

# All-in-one
all: setup build start ## Complete setup: install, build, and start everything
