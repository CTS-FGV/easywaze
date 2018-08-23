#!/bin/bash

# Enable virtualenv
. $USER_HOME/.pyenv/versions/venv/bin/activate

# Run Python command each 120 seconds
while true; do
    cd /app && python main.py

    sleep 119
done
