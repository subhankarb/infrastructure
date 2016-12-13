#!/usr/bin/env bash

set -e

VENV_DIR="$PWD/venv"

if [ ! -d "$VENV_DIR" ]; then
    virtualenv --python=python3 $VENV_DIR
    echo "Virtualenv created at $VENV_DIR"
fi

source $VENV_DIR/bin/activate
pip install --upgrade pip
pip install awscli
deactivate
