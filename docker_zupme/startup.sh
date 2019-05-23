#!/bin/sh
# startup.sh - startup script for the server docker image

echo "Starting Conductor server"

# Start the server
cd /app/libs
echo "Property file: $CONFIG_PROP"
echo $CONFIG_PROP
export config_file=

echo "Log4j property file: $LOG4J_PROP"
export log4j_file=$LOG4J_PROP

if [ -z "$CONFIG_PROP" ];
  then
    echo "Using an in-memory instance of conductor";
    export config_file=/app/config/config.properties
  else
    echo "Using '$CONFIG_PROP'";
    export config_file=$CONFIG_PROP
fi

export MEM_PARAMS=$(/param_tuning.sh ${HEAP_SIZE:-512})

java ${JAVA_AGENT} ${JAVA_OPTS} ${MEM_PARAMS} -jar conductor-server-*-all.jar $config_file $log4j_file
