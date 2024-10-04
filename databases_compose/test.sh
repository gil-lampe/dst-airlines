#! /bin/bash

CURRENT_DIR=$(dirname "$(realpath "$0")")
PROJECT_DIR=$(dirname "$CURRENT_DIR")

source $PROJECT_DIR/env/private.env


echo $PROJECT_DIR
echo $TEST