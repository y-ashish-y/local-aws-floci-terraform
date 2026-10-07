#!/usr/bin/env bash
# Installs toolchain. Terraform is not in Homebrew (license) so it is
# fetched from HashiCorp releases; everything else comes from brew.
set -euo pipefail

brew install floci-io/floci/floci kubectl helm kind
# shellcheck disable=SC1091
eval "$(brew shellenv)" 2>/dev/null || true

if ! command -v terraform >/dev/null; then
  TF_VER=1.9.8
  curl -fsSL -o /tmp/terraform.zip \
    "https://releases.hashicorp.com/terraform/${TF_VER}/terraform_${TF_VER}_linux_amd64.zip"
  unzip -o /tmp/terraform.zip -d /tmp
  mkdir -p ~/.local/bin
  mv /tmp/terraform ~/.local/bin/
fi

export PATH="$HOME/.local/bin:$PATH"
floci --version && terraform version && kubectl version --client && helm version && kind version
