#!/bin/bash -e

# (c) 2020, Ava Labs, Inc. All rights reserved.
# See the file LICENSE for licensing terms.

travis encrypt-file .ci/set_env_secrets.sh .ci/set_env_secrets.sh.enc
