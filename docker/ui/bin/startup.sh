#!/bin/bash
# startup.sh - startup script for the UI docker image

echo "Starting Conductor UI"

# Start the UI
cd /app/ui/dist
if [ -z "$WF_SERVER" ];
  then
    export WF_SERVER=http://localhost:8080/api/
  else
    echo "using Conductor API server from '$WF_SERVER'"
fi

secrets=/app/config/secrets.env
if [ -f $secrets ]; then
    echo Loading environments from $secrets
    secrets=$(cat $secrets | grep =)
    export $secrets
fi


# Run server.js in the foreground, sending messages to stdout
exec node server.js