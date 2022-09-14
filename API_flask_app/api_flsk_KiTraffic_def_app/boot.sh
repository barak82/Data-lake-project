#!/bin/bash
source venv/bin/activate
exec gunicorn -b :6060 --access-logfile - --error-logfile - API_client:server