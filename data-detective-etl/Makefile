GRAPH_CMD=docker exec -it data-detective-etl_app_1  ./docker/generate_dags_graph.sh
LINT_CMD=docker-compose -f docker-compose.linter.yml up --build --exit-code-from=linter
TESTS_CMD=docker-compose -f docker-compose.tests.yml up --build --exit-code-from=tests

.PHONY: help
help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}'

.PHONY: graph
graph: ## Generate image with tasks for all DAGs
	$(GRAPH_CMD) $(DAG) $(IMG)

.PHONY: lint
lint: ## Run linters with docker-compose.tests.yml
	$(LINT_CMD)

.PHONY: tests
tests: ## Run pytest with docker-compose.tests.yml
	$(TESTS_CMD)
