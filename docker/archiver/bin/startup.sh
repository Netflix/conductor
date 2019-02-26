#!/bin/sh

cd /app

if [ -f /app/config/secrets.env ]; then
    secrets=$(cat /app/config/secrets.env | grep =)
    export $secrets
fi

echo "source="${workflow_elasticsearch_url} > /app/archiver.properties
echo "env="${env_type} >> /app/archiver.properties

# $1 - environment name
# $2 - property name in the file
# $3 - default value in the property file if $1 is not defined
addParam() {
    if [[ "$1" != "" ]]; then
        echo $2"="$1 >> /app/archiver.properties
    else
        echo $2"="$3 >> /app/archiver.properties
    fi
}

addParam "${archiver_keep_days}" "keep_days" "30"
addParam "${archiver_batch_size}" "batch_size" "5000"
addParam "${archiver_queue_workers}" "queue_workers" "100"

addParam "${archiver_bucket_name}" "bucket_name" "conductor-initializer-shared-owf-dev-us-west-2"
addParam "${archiver_region}" "region" "us-west-2"
addParam "${archiver_access_key}" "access_key" ""
addParam "${archiver_access_secret}" "access_secret" ""

addParam "${log4j_aurora_appender}" "log4j_aurora_appender" "false"
addParam "${aurora_host}" "aurora_host" ""
addParam "${aurora_port}" "aurora_port" ""
addParam "${aurora_db}" "aurora_db" ""
addParam "${aurora_user}" "aurora_user" ""
addParam "${aurora_password}" "aurora_password" ""

exec java $JAVA_OPTS -jar conductor-archiver-*-all.jar