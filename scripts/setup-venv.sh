#!/bin/bash

set -euo pipefail

source scripts/logger.sh

# List of python module subdirectories in the project
SUBDIRS=(
  "dbt"
  "api"
  "ingestor"
  "transformer"
  "retl"
  "scripts"
)

setup_global_tools() {
  logger "INFO" "🌍 Setting up global Python tools: pre-commit, sqlglot"
  if command -v pipx &>/dev/null; then
    pipx install --force pre-commit
    pipx install --force sqlglot
  else
    python3 -m pip install --user --upgrade pip
    python3 -m pip install --user --upgrade pre-commit sqlglot
  fi
  logger "INFO" "✅ Global tools installed"

  # Ensure pre-commit is set up as a git hook if config exists
  if [ -f ".pre-commit-config.yaml" ]; then
    logger "INFO" "🔗 Installing pre-commit git hook"
    if command -v pre-commit &>/dev/null; then
      pre-commit install
      logger "INFO" "✅ pre-commit hook installed"
    else
      logger "ERROR" "pre-commit not found in PATH after installation"
      exit 1
    fi
  else
    logger "WARNING" "No .pre-commit-config.yaml found, skipping pre-commit hook installation"
  fi
}

create_pyvenv() {
  local dir="$1"
  if [ ! -d "$dir/venv" ]; then
    logger "INFO" "🧪 Creating virtual environment in $dir/venv"
    if ! command -v python3 &>/dev/null; then
      logger "ERROR" "python3 not found. Please install Python 3."
      exit 1
    fi
    python3 -m venv "$dir/venv" \
      || { logger "ERROR" "Failed to create venv in $dir"; exit 1; }
  else
    logger "INFO" "✅ Virtual environment already exists in $dir/venv"
  fi
}

install_requirements() {
  local dir="$1"
  if [ -f "$dir/requirements.txt" ]; then
    logger "INFO" "📦 Installing requirements for $dir"
    pushd "$dir" > /dev/null
    source "venv/bin/activate"
    pip install --upgrade pip
    pip install -r "requirements.txt" \
      || { logger "ERROR" "pip install failed in $dir"; deactivate; popd > /dev/null; exit 1; }
    pip install pytest==8.4.0
    deactivate
    popd > /dev/null
  else
    logger "WARNING" "⚠️  requirements.txt not found in $dir – skipping pip install"
  fi
}

logger "INFO" "🔧 Setting up Python environments"

#setup_global_tools

for dir in "${SUBDIRS[@]}"; do
  if [ -d "$dir" ]; then
    logger "INFO" "👉 Found $dir"
    create_pyvenv "$dir"
    install_requirements "$dir"
  else
    logger "WARNING" "❌ Directory $dir does not exist – skipping"
  fi
done
logger "INFO" "✅ Finished setting up Python environments"
