#!/bin/bash

docker run --env-file ../env/private.env -p 8000:8000 glampe/dst_airlines_fastapi:latest