#!/bin/bash

rm `find . -name *.lock`

cp -R /cache /home/.gradle

./gradlew generateLock updateLock saveLock

./gradlew clean build