# (c) 2020, Ava Labs, Inc. All rights reserved.
# See the file LICENSE for licensing terms.

##
## Help
##
.DEFAULT_GOAL := help
.PHONY: help
help:
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}'

##
## Developer environment
##
.PHONY: dev_env_run dev_env_rm test_env_run test_env_rm standalone_run standalone_rm standalone_build

dev_env_run: ## Run backing services in dev mode
	@docker-compose -f docker/dev_env/docker-compose.yml up --remove-orphans

dev_env_start: ## Start up backing services in dev mode
	@docker-compose -f docker/dev_env/docker-compose.yml up --remove-orphans -d

dev_env_rm: ## Stop and remove all backing services in dev mode
	@docker-compose -f docker/dev_env/docker-compose.yml down --remove-orphans --volumes

test_env_run: ## Run backing services in test mode
	@docker-compose -f docker/test_env/docker-compose.yml up --remove-orphans

test_env_start: ## Start up backing services in test mode
	@docker-compose -f docker/test_env/docker-compose.yml up --remove-orphans -d

test_env_rm: ## Stop and remove all test mode services
	@docker-compose -f docker/standalone/docker-compose.yml down --remove-orphans --volumes

standalone_run: ## Run standalone mode
	@docker-compose -f docker/standalone/docker-compose.yml up --remove-orphans

standalone_start: ## Start up standalone mode
	@docker-compose -f docker/standalone/docker-compose.yml up --remove-orphans -d

standalone_rm: ## Stop and remove all standalone mode services
	@docker-compose -f docker/standalone/docker-compose.yml down --remove-orphans --volumes

standalone_build: ## Build or rebuild all standalone mode images
	@docker-compose -f docker/standalone/docker-compose.yml build

production_start: ## Start production mode
	@docker-compose -f docker/production/docker-compose.yml up -d --remove-orphans

production_stop: ## Stop production mode
	@docker-compose -f docker/production/docker-compose.yml stop

production_rm: ## Remove production mode
	@docker-compose -f docker/production/docker-compose.yml down

production_logs: ## Logs for production mode
	@docker-compose -f docker/production/docker-compose.yml logs -f

##
## Testing
##
.PHONY: tests profile_tests check_binaries

tests: ## Run tests
	go test -race -timeout="120s" -coverprofile="coverage.out" -covermode="atomic" ./...

tests_profile: ## Run tests with coverage profiling
	go test -v -coverprofile=coverage.out -coverpkg=./... ./...
	go tool cover -html=./coverage.out

check_binaries: ## Ensure the binaries build
	@(go build -o /dev/null ./api/b in/main.go 2>&1 >/dev/null && \
	go build -o /dev/null ./client/bin/main.go 2>&1 >/dev/null && \
	echo "Builds successful") || \
	(echo "Builds failed" && exit 1)

##
## Database
##
.PHONY: db_migrate_up db_migrate_down

db_migrate_up: ## Migrate the database up
	DSN="${DSN:-mysql://root:password@tcp(127.0.0.1:3306)/ortelius_dev}"
	${GOPATH}/bin/migrate -source file://services/db/migrations -database "${DSN}" up

db_migrate_down: ## Migrate the database down
	DSN="${DSN:-mysql://root:password@tcp(127.0.0.1:3306)/ortelius_dev}"
	${GOPATH}/bin/migrate -source file://services/db/migrations -database "${DSN}" down

##
## Build
##
.PHONY: image image_publish

GIT_HASH = $(shell git rev-parse --short HEAD)

DOCKER_REPO ?= avaplatform/ortelius
DOCKER_TAG ?= $(GIT_HASH)
DOCKER_IMAGE_NAME ?= ${DOCKER_REPO}:${DOCKER_TAG}

image: ## Build the Docker image
	docker build -t ${DOCKER_IMAGE_NAME} .

image_push: ## Push the Docker image to the registry
	docker push ${DOCKER_IMAGE_NAME}
