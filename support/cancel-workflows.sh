#!/usr/bin/env bash

# owf-dev, owf-int, owf-live
TLD=owf-dev

# DNS Service Name
SERVICE=sherlock.service.${TLD}

# Get the SRV record from the dns
ENDPOINT=`dig +short $SERVICE SRV | head -1 | sed 's/\.[^.]*$//' | awk '{print "http://" $4 ":" $3}'`

# sherlock endpoint
echo "Using sherlock endpoint $ENDPOINT"

# cancel workflows
for id in $(strings -n 0 workflows.txt)
do
    RESPONSE=$(curl -i -s -X POST -H 'Accept: application/json' $ENDPOINT/v1/cancel/$id)
    HTTP=$(echo "$RESPONSE" | head -n 1 | awk '{print $2}')
    BODY=$(echo "$RESPONSE" | tail -n 1)
    if [[ $HTTP = 200 ]]; then
        echo "$id CANCELLED"
    else
        echo "$id FAILED(HTTP Response $HTTP) ${BODY}"
    fi
done

echo "Done"

