.DEFAULT_GOAL := help
PYTHONPATH = PYTHONPATH:./
TEST=docker-compose -f docker-compose.tests.yml up --build --exit-code-from=tests

CODE = mg_af

.PHONY: help
help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}'

.PHONY: test
test: ## Runs pytest in docker-compose with coverage
	$(TEST)

.PHONY: lint
lint: ## Lint code
	#flake8 --jobs 4 --statistics --show-source $(CODE)
	#pylint --rcfile=setup.cfg $(CODE)
	mypy $(CODE)
	#black --line-length 120 --target-version py39 --skip-string-normalization --check $(CODE)
	#pytest --dead-fixtures --dup-fixtures
	#safety --help
	#safety check --full-report $(CODE)

.PHONY: format
format: ## Formats all files
	#autoflake --in-place --remove-all-unused-imports $(CODE)
	isort --src mg_af $(CODE)
	black --line-length 120 --target-version py39 --skip-string-normalization $(CODE)
	unify --in-place --recursive $(CODE)

.PHONY: check
check: format lint test ## Format and lint code then run tests
