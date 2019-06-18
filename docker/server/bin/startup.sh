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

secrets=/app/config/secrets.env
if [ -f $secrets ]; then
    echo Loading environments from $secrets
    secrets=$(cat $secrets | grep =)
    export $secrets
fi

if [[ "$workflow_elasticsearch_url" != "" ]]; then
    config_url=$workflow_elasticsearch_url/conductor.metadata.${STACK}.config/_search?size=1000
    for e in $(curl --silent --fail $config_url | jq -c '.hits.hits[]');
    do
        name=$(echo $e | jq '._id' | sed 's/"//g')
        value=$(echo $e | jq '._source.value' | sed 's/"//g')
        export $(echo $name"="$value)
    done
fi

if [[ "$log4j_aurora_appender" == "true" ]]; then
    echo "log4j.rootLogger=ALL, CN, DB" > /app/config/log4j.properties

    echo "log4j.appender.CN=org.apache.log4j.ConsoleAppender" >> /app/config/log4j.properties
    echo "log4j.appender.CN.threshold=INFO" >> /app/config/log4j.properties
    echo "log4j.appender.CN.layout=com.netflix.conductor.log4j.KeyValueLayout" >> /app/config/log4j.properties

    echo "log4j.appender.DB=com.netflix.conductor.aurora.log4j.DeluxeAuroraAppender" >> /app/config/log4j.properties
    echo "log4j.appender.DB.url=jdbc:postgresql://"${aurora_host}":"${aurora_port}"/"${aurora_db} >> /app/config/log4j.properties
    echo "log4j.appender.DB.user="${aurora_user} >> /app/config/log4j.properties
    echo "log4j.appender.DB.password="${aurora_password} >> /app/config/log4j.properties
    echo "log4j.appender.DB.threshold=TRACE" >> /app/config/log4j.properties
fi


# Log the configuration settings as defaults
echo "Starting conductor server with the following defaults: $(cat $config_file | grep = | grep -v '#' | sed ':a;N;$!ba;s/\n/ /g')"

# Add loggers
for logger in $(env | grep log4j_logger | sed 's/_/./g');
do
    echo ${logger} >> /app/config/log4j.properties
done

# Run java in the foreground and stream messages directly to stdout
exec java $JAVA_OPTS -Dlog4j.configuration="file:/app/config/log4j.properties" -Dlog4j.configurationFile="file:/app/config/log4j.properties" -jar conductor-server-*-all.jar $config_file
