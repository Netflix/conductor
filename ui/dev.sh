#!/bin/bash
set -ex

#Disable browser auto opening in CRA since it will be the wrong port
BROWSER=none yarn start