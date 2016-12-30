#!/bin/bash

cd /netflix/conductor/test-harness
../gradlew server &

cd /netflix/conductor/ui
npm install
npm run watch
