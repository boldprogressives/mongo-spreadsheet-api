#!/bin/bash
./bin/gunicorn -b 0.0.0.0:8000 server.app:app -k gevent
