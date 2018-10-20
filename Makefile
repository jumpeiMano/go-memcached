PROGRAM=go-memcached
CIRCLE_TAG=dummy
.DEFAULT_GOAL := help

.PHONY: help debug
help:
	grep -E '^[a-z0-9A-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}'

dep-ensure: ## update packages via dep
	glide update

lint: ## lint
	gometalinter --config=linter_config.json ./...

test: ## test
	go test -v -race ./...
