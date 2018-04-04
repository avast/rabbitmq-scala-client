#!/usr/bin/env bash

./gradlew test &&
  if $(test ${TRAVIS_REPO_SLUG} == "avast/rabbitmq-scala-client" && test ${TRAVIS_PULL_REQUEST} == "false" && test "$TRAVIS_TAG" != ""); then
    ./gradlew bintrayUpload -Pversion="$TRAVIS_TAG" --info
  else
    exit 0 # skipping publish, it's regular build
  fi