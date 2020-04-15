# (c) 2020, Ava Labs, Inc. All rights reserved.
# See the file LICENSE for licensing terms.

.DEFAULT_GOAL := help
.PHONY: help
help:
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}'

##
## Testing
##
check_binaries: ## Ensure the binaries build
	@(go build -o /dev/null ./api/bin/main.go 2>&1 >/dev/null && \
	go build -o /dev/null ./client/bin/main.go 2>&1 >/dev/null && \
	echo "Builds successful") || \
	(echo "Builds failed" && exit 1)

##
## Developer environment
##
.PHONY: dev_env_run test_env_run

dev_env_run: ## Start up backing services in dev mode
	@(cd docker/dev_env && docker-compose up --remove-orphans)

test_env_run: ## Start up backing services in test mode
	@(cd docker/test_env && docker-compose up --remove-orphans)

##
## Testing
##
.PHONY: tests profile_tests

tests: ## Run tests
	go test -i ./...
	go test -v -cover ./...

tests_profile: ## Run tests with coverage profiling
	go test -i ./...
	go test -v -coverprofile=coverage.out -coverpkg=./... ./...
	go tool cover -html=./coverage.out

##
## Database
##
.PHONY: db_install_migrate db_migrate_up db_migrate_down

db_install_migrate: ## Install the migration tool
	@which migrate || go get -tags 'mysql' github.com/golang-migrate/migrate/cmd/migrate

db_migrate_up: db_install_migrate ## Migrate the database up
	DSN="${DSN:-mysql://root:password@tcp(127.0.0.1:3306)/ortelius_dev}"
	${GOPATH}/bin/migrate -source file://services/db/migrations -database "${DSN}" up

db_migrate_down: db_install_migrate ## Migrate the downbase down
	DSN="${DSN:-mysql://root:password@tcp(127.0.0.1:3306)/ortelius_dev}"
	${GOPATH}/bin/migrate -source file://services/db/migrations -database "${DSN}" down
