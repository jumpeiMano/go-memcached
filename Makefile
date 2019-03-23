PROGRAM=go-memcached
WORKING_DIRECTORY=/go/src/github.com/jumpeiMano/${PROGRAM}
CIRCLE_TAG=dummy
.DEFAULT_GOAL := help

.PHONY: help debug
help:
	grep -E '^[a-z0-9A-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}'

mod-download: ## download packages via go mod
	docker-compose run --rm --workdir ${WORKING_DIRECTORY} --no-deps \
	--entrypoint go app mod download

lint: ## lint
	docker-compose run --rm lint

test: ## test
	docker-compose run --rm --workdir ${WORKING_DIRECTORY} --entrypoint go \
	app test -v -race ./...
