#!/bin/bash

source scripts/logger.sh

# List of python module subdirectories
SUBDIRS=(
  "dbt"
  "api"
  "ingestor"
  "transformer"
)

create_pyvenv() {
  if [ ! -d "$dir/venv" ]; then
      logger "INFO" "🧪 Creating virtual environment in $dir/venv"
      python3 -m venv "$dir/venv"
  else
    logger "INFO" "✅ Virtual environment already exists in $dir/venv"
  fi
}


install_requirements() {
  if [ -f "$dir/requirements.txt" ]; then
      logger "INFO" "📦 Installing requirements for $dir"
      source "$dir/venv/bin/activate"
      pip install --upgrade pip
      pip install -r "$dir/requirements.txt"
      deactivate
  else
    logger "WARNING" "⚠️  requirements.txt not found in $dir – skipping pip install"
  fi
}


logger "INFO" "🔧 Setting up Python environments"
for dir in "${SUBDIRS[@]}"; do
  if [ -d "$dir" ]; then
    logger "INFO" "👉 Found $dir"
    create_pyvenv
    install_requirements
  else
    logger "WARNING" "❌ Directory $dir does not exist – skipping"
  fi
done
logger "INFO" "✅ Finished setting up Python environments"
