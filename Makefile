# Define variables
DOCKER_COMPOSE_FILE=docker-compose.yml

# Default target when `make` is run without arguments
.DEFAULT_GOAL := help

.PHONY: help
help:  ## Show this help message
	@echo ""
	@echo "Usage: make [option]"
	@echo ""
	@echo "Options:"
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-20s\033[0m %s\n", $$1, $$2}'
	@echo ""

.PHONY: build
build:  ## Build docker services
	docker-compose -f $(DOCKER_COMPOSE_FILE) build

.PHONY: start
start:  ## Start docker services (detached mode)
	docker-compose -f $(DOCKER_COMPOSE_FILE) up -d

.PHONY: stop
stop:  ## Stop docker services
	docker-compose -f $(DOCKER_COMPOSE_FILE) stop

.PHONY: dbt-limit
dbt-test:  ## Run dbt with LIMIT 100
	cd data_modelling && dbt build

.PHONY: dbt-full
dbt-full:  ## Run dbt with full data
	cd data_modelling && dbt build --vars '{"is_dev_run": false}'

