#!/bin/bash -e

# (c) 2020, Ava Labs, Inc. All rights reserved.
# See the file LICENSE for licensing terms.

# Decrypt and export secrets
openssl aes-256-cbc -K $encrypted_c4bd738ed480_key -iv $encrypted_c4bd738ed480_iv -in .ci/set_env_secrets.sh.enc -out .ci/set_env_secrets.sh -d
source .ci/set_env_secrets.sh

# Build and deploy images
.ci/deploy_images.sh
