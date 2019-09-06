#!/usr/bin/env bash

# owf-dev, owf-int, owf-live
TLD=owf-dev

# DNS Service Name
SERVICE=conductor-server.service.${TLD}

# Get the SRV record from the dns
ENDPOINT=`dig +short $SERVICE SRV | head -1 | sed 's/\.[^.]*$//' | awk '{print "http://" $4 ":" $3}'`

# Conductor endpoint
echo "Using conductor endpoint $ENDPOINT"

# Auth token. Obtain a fresh
TOKEN="<<token>>>"

# cancel workflows
for id in $(strings -n 0 workflows.txt)
do
    RESPONSE=$(curl -i -s -X POST -H "Accept: application/json" -H "Authorization: Bearer $TOKEN" $ENDPOINT/api/workflow/$id/cancel?reason=Post-deployment-cleanup)
    HTTP=$(echo "$RESPONSE" | head -n 1 | awk '{print $2}')
    BODY=$(echo "$RESPONSE" | tail -n 1)
    if [[ $HTTP = 200 ]]; then
        echo "$id CANCELLED"
    else
        echo "$id FAILED(HTTP Response $HTTP) ${BODY}"
    fi
done

echo "Done"

