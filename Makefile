PYTHON = python3.12
APP_NAME = DataHandler
STACK_NAME = DataHandler
FN_NAME_DEFAULT = data_handler
LINT_PATHS_CDK = cdk_app.py config.py ./stacks
LINT_PATHS = $(LINT_PATHS_CDK) ./functions

#set defaults
ifeq ($(FN_NAME),)
FN_NAME := $(FN_NAME_DEFAULT)
endif


VENV = . .venv/bin/activate
FORMAT_SM_STATUS = jq '(if has("input") then .input|=fromjson else . end) | (if has("output") then .output|=fromjson else . end)'
GET_PAYLOAD = if [ -f ./events/${FN_NAME}.${EVENT}.json ]; then payload='--payload fileb://events/${FN_NAME}.${EVENT}.json'; elif [ -f ./events/${FN_NAME}.json ]; then payload='--payload fileb://events/${FN_NAME}.json'; else payload=''; fi
.DEFAULT_GOAL:=help
# Always execute make targets to remove need to maintain list of PHONY targets
MAKEFLAGS += --always-make
CURRENT_DIR = $(shell pwd)


##@ HELP
help: ##display this help
	@awk -v FN_NAME_DEFAULT=$(FN_NAME_DEFAULT) 'BEGIN {FS = ":.*##"; \
	printf "\nFor detailed usage please refer to README, wiki, or relevant procedure.\n\n"; \
	printf "\033[1mUSAGE\n\033[0m  make \033[36m<TARGET>\033[0m\n"} \
	/^[a-zA-Z0-9_-]+:.*?##/ {printf "  \033[36m%-20s\033[0m %s\n", $$1, $$2} \
	/^###/ {sub("FN_NAME_DEFAULT", FN_NAME_DEFAULT, $$0); printf "\033[0m%s\033[0m\n", substr($$0, 4)} \
	/^##@/ {printf "\n\033[1m%s\033[0m\n", substr($$0, 5)}' $(MAKEFILE_LIST)


##@ SETUP
venv: ##checks if .venv exists and runs install if it doesn't
	@test -d .venv || $(MAKE) install

install: ##setup .venv then install/upgrade required modules
	@test -d .venv || ${PYTHON} -m venv .venv --prompt ${APP_NAME}
	${VENV}; pip install -q --upgrade pip
	${VENV}; pip install -q --upgrade -r requirements.txt
	${VENV}; pip install -q --upgrade -r requirements-dev.txt
	${VENV}; find ./functions -maxdepth 2 -name 'requirements.txt' -exec pip install -q --upgrade -r {} \;

build_config: venv ##Generates .config_cache.json from config.py for use by CLI commands
	@echo "Generating .config_cache.json from config.py..."
	@${VENV}; python config.py > .config_cache.json

get_config: venv ##Generates .config_cache.json from config.py when the json file is missing or is older than config.py
	@test -e .config_cache.json -a .config_cache.json -nt config.py || $(MAKE) build_config

##@ LINT & FORMAT
lint: venv pylint black ##lint the python files with pylint, black, and isort
	${VENV}; isort --diff ${LINT_PATHS_CDK}
	@${VENV}; find ./functions -mindepth 1 -maxdepth 1 -type d -exec echo "${VENV}; isort --diff {}" \; -exec isort --diff {} \;

pylint: venv ##lint the python files with pylint
	${VENV}; pylint ${LINT_PATHS_CDK}
	@${VENV}; find ./functions -mindepth 1 -maxdepth 1 -type d -exec echo "${VENV}; pylint {} --recursive=y" \; -exec pylint {} --recursive=y \;

black: venv ##lint the python files with black
	${VENV}; black --diff --color ${LINT_PATHS}

format: venv ##format python files with isort & black
	${VENV}; isort ${LINT_PATHS_CDK}
	@${VENV}; find ./functions -mindepth 1 -maxdepth 1 -type d -exec echo "${VENV}; isort {}" \; -exec isort {} \;
	${VENV}; black ${LINT_PATHS}
