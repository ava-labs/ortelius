#!/bin/bash -e

# (c) 2020, Ava Labs, Inc. All rights reserved.
# See the file LICENSE for licensing terms.

# Build the image with a canonical commit-based name
IMAGE_NAME="$DOCKER_REGISTRY/ortelius:dev-$(git rev-parse --short HEAD)"
DOCKER_IMAGE_NAME="$IMAGE_NAME" make image

# Login to docker registry
echo "$DOCKER_PASSWORD" | docker login --username $DOCKER_USER --password-stdin

# Create and publish all the appropriate tags
# Every commit gets a commit-based tag. Git tags, master branch, and staging
# branch all get special tags
tag_and_push () {
  docker tag $IMAGE_NAME "$DOCKER_REGISTRY/ortelius:$1"
  docker push "$DOCKER_REGISTRY/ortelius:$1"
}

docker push "$IMAGE_NAME"
if [ "$TRAVIS_TAG" != "" ] ; then tag_and_push $TRAVIS_TAG; fi
if [ "$TRAVIS_BRANCH" == "master" ] ; then tag_and_push "latest"; fi
if [ "$TRAVIS_BRANCH" == "staging" ] ; then tag_and_push "staging"; fi

