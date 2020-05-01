#!/bin/bash -e

# (c) 2020, Ava Labs, Inc. All rights reserved.
# See the file LICENSE for licensing terms.

# Build the image
IMAGE_NAME="$DOCKER_REGISTRY/ortelius:dev-$(git rev-parse --short HEAD)"
DOCKER_IMAGE_NAME=IMAGE_NAME make image

# Install AWS CLI and login to ECR
which aws || pip install --user awscli
PATH=$PATH:$HOME/.local/bin aws ecr get-login-password --region=$AWS_REGION | docker login --username $DOCKER_USER --password-stdin $DOCKER_REGISTRY/ortelius

# Create and publish all the appropriate tags
tag_and_push () {
  docker tag $IMAGE_NAME "$DOCKER_REGISTRY/ortelius:$1"
  docker push "$DOCKER_REGISTRY/ortelius:$1"
}

if [ "$TRAVIS_TAG" != "" ] ; then tag_and_push $TRAVIS_TAG; fi
if [ "$TRAVIS_BRANCH" == "master" ] ; then tag_and_push "latest"; fi
if [ "$TRAVIS_BRANCH" == "staging" ] ; then tag_and_push "staging"; fi

