#!/bin/bash
set -e

curl -fsSL https://get.pulumi.com | sh

echo 'export PATH="$PATH:/root/.pulumi/bin"' >> /root/.bashrc