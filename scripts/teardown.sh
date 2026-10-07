#!/usr/bin/env bash
set -euo pipefail
kind delete cluster --name lakehouse || true
floci stop || true
