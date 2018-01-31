#!/bin/sh
# startup.sh - startup script for the server docker image

echo "Starting Conductor server"

# Start the server
cd /app/libs
echo "Property file: $CONFIG_PROP"
echo $CONFIG_PROP
export config_file=

if [ -z "$CONFIG_PROP" ];
  then
    export config_file=/app/config/config-local.properties
  else
    export config_file=/app/config/$CONFIG_PROP
fi

secrets=/app/config/secrets.properties
if [ -f $secrets ]; then
    echo Loading environments from $secrets
    secrets=$(cat $secrets | grep =)
    export $secrets
fi

# Log the configuration settings as defaults
echo "Starting conductor server with the following defaults: $(cat $config_file | grep = | grep -v '#' | sed ':a;N;$!ba;s/\n/ /g')"

# Run java in the foreground and stream messages directly to stdout
java -jar conductor-server-*-all.jar $config_file
