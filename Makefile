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
	go test -v -race -cover ./...

dockerize:
	docker-compose run --rm --no-deps --entrypoint dockerize \
	app -wait tcp://memcached_1:11211 -timeout 1m
	docker-compose run --rm --no-deps --entrypoint dockerize \
	app -wait tcp://memcached_2:11211 -timeout 1m
	docker-compose run --rm --no-deps --entrypoint dockerize \
	app -wait tcp://memcached_3:11211 -timeout 1m
	docker-compose run --rm --no-deps --entrypoint dockerize \
	app -wait tcp://memcached_4:11211 -timeout 1m
