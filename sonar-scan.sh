#!/bin/bash

if [ -z "${SONAR_ENABLED}" ]; then
    SONAR_ENABLED=0
fi

if [ "${SONAR_ENABLED}" -eq "1" ]; then
  ./gradlew sonarqube -x test -Dsonar.projectKey=conductor -Dsonar.branch.name="${SONAR_BRANCH}" -Dsonar.host.url="${SONAR_HOST}" -Dsonar.login="${SONAR_LOGIN}"
else
  echo "SONAR_ENABLED: ${SONAR_ENABLED}. Analysis skipped."
fi