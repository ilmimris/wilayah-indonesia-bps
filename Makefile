# Makefile for BPS Wilayah Data Fetcher

# Variables
PYTHON = .venv/bin/python
VENV_DIR = .venv
COOKIE ?= ""

.PHONY: all install fetch clean

all: install fetch

install: $(VENV_DIR)/pyvenv.cfg

$(VENV_DIR)/pyvenv.cfg: requirements.txt
	@command -v uv >/dev/null 2>&1 || (echo "Error: uv is not installed. Please install it from https://github.com/astral-sh/uv" && exit 1)
	@echo "Creating virtual environment..."
	uv venv
	@echo "Installing dependencies..."
	uv pip install -r requirements.txt

fetch: install
	@if [ -z "$(COOKIE)" ]; then \
		echo "Error: COOKIE environment variable is not set."; \
		echo "Usage: make fetch COOKIE='your_cookie_string_here'"; \
		exit 1; \
	fi
	@echo "Fetching BPS data..."
	$(PYTHON) fetch_bps_wilayah.py --cookie "$(COOKIE)"

clean:
	@echo "Cleaning generated data..."
	rm -rf data/processed/* data/raw/* data/sql/*
