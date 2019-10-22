#!/bin/bash

set -e
set -x

pipenv run mypy .
pipenv run black .
pipenv run flake8 .
pipenv run pytest --cov-fail-under=100
