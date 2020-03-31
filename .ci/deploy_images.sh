#!/bin/bash -e

# (c) 2020, Ava Labs, Inc. All rights reserved.
# See the file LICENSE for licensing terms.

# tag_and_push takes in an app name and a tag, tags the ortelius image for that
# app with the tag, and pushes it to the registry
tag_and_push () {
  TAG="$DOCKER_REGISTRY/ortelius-$1:$2"
  docker tag $1 $TAG
  docker push $TAG
}

# build_and_deploy is the workhorse of this script. It builds an image, decides
# which tags it should have, and pushes them to the registry
build_and_deploy() {
  # The name of the app we're building; e.g. "api" or "client"
  APP=${1}

  docker build -t $APP -f ./docker/Dockerfile.$APP .

  PATH=$PATH:$HOME/.local/bin aws ecr get-login-password --region=$AWS_REGION | docker login --username $DOCKER_USER --password-stdin $DOCKER_REGISTRY/ortelius-$APP

  tag_and_push $1 "dev-$(git rev-parse --short HEAD)"
  if [ "$TRAVIS_TAG" != "" ] ; then tag_and_push $1 $TRAVIS_TAG; fi
  if [ "$TRAVIS_BRANCH" == "master" ] ; then tag_and_push $1 "latest"; fi
  if [ "$TRAVIS_BRANCH" == "staging" ] ; then tag_and_push $1 "staging"; fi
}

# Install AWSCLI if necessary
which aws || pip install --user awscli

# Build and deploy images for each app
build_and_deploy api
build_and_deploy client
