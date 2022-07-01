#!/bin/sh
#
#  Copyright 2021 Netflix, Inc.
#  <p>
#  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
#  the License. You may obtain a copy of the License at
#  <p>
#  http://www.apache.org/licenses/LICENSE-2.0
#  <p>
#  Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
#  an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
#  specific language governing permissions and limitations under the License.
#

# startup.sh - startup script for the server docker image

echo "Starting Conductor server"
set -a

# Start the server
cd /app/libs
echo "Property file: $CONFIG_PROP"
echo $CONFIG_PROP
export config_file=

if [ -z "$CONFIG_PROP" ];
  then
    echo "Using an in-memory instance of conductor";
    export config_file=/app/config/config-local.properties
  else
    echo "Using '$CONFIG_PROP'";
    export config_file=/app/config/$CONFIG_PROP
fi

if [ ! -z "${LOG4J_PROP}" ]; then
  LOG4J_CONF="-Dlog4j2.configurationFile=${LOG4J_PROP}"
fi

echo "Using java options config: $JAVA_OPTS"

if [ -f "/set_env_secrets.sh" ]; then
. /set_env_secrets.sh
fi

java ${JAVA_OPTS} -jar ${LOG4J_CONF:-} -DCONDUCTOR_CONFIG_FILE=$config_file conductor-server-*-boot.jar
