#!/bin/bash

cat > pre-commit.sh <<EOF
#!/bin/bash

set -e
set -x

pipenv run mypy .
pipenv run black .
pipenv run flake8 .
pipenv run pytest --cov-fail-under=100
EOF

pipenv --python=python3.8 install --pre pytest pytest-cov black mypy flake8

cat > setup.cfg <<EOF
[flake8]
ignore = E203, E266, E501, W503
max-line-length = 80
max-complexity = 18
select = B,C,E,F,W,T4,B9

[tool:pytest]
addopts = --cov=. --cov-report=term-missing --no-cov-on-fail

[mypy]
ignore_missing_imports = True

[mypy-unittests]
ignore_errors = True
EOF