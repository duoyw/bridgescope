#!/bin/bash
set -e

# Remove all files in the dist directory
rm -rf dist/*

# Build the project
python -m build

# Upload to PyPI
# You can add --repository-url to specify test or production PyPI
# For example: twine upload --repository-url https://test.pypi.org/legacy/ dist/*
twine upload dist/*

uvx --refresh mcp-proxy-exec
