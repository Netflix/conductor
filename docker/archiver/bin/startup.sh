#!/bin/sh

cd /app

if [ -f /app/config/secrets.env ]; then
    secrets=$(cat /app/config/secrets.env | grep =)
    export $secrets
fi

# Init the file
echo "" > /app/archiver.properties

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
addParam "${archiver_queue_workers}" "queue_workers" "100"

addParam "${aurora_host}" "aurora_host" ""
addParam "${aurora_port}" "aurora_port" ""
addParam "${aurora_db}" "aurora_db" ""
addParam "${aurora_user}" "aurora_user" ""
addParam "${aurora_password}" "aurora_password" ""

exec java $JAVA_OPTS -jar conductor-archiver-*-all.jar