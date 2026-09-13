#!/bin/bash
# ==============================================================================
# Fetch secrets from CyberArk Conjur
# ==============================================================================
#
# This script retrieves secrets from Conjur and exports them as environment
# variables. It is sourced by env.sh before application startup.
#
# Prerequisites:
#   - Conjur CLI (conjur) installed and in PATH
#   - Host identity configured (~/.conjurrc or CONJUR_* env vars)
#   - Service account has access to the required secrets
#
# Usage:
#   source fetch_secrets.sh
#
# ==============================================================================

set -euo pipefail

# Conjur secret paths - customize per environment
CONJUR_ACCOUNT="${CONJUR_ACCOUNT:-company}"
CONJUR_MQ_SECRET_PATH="${CONJUR_MQ_SECRET_PATH:-}"
CONJUR_HDFS_SECRET_PATH="${CONJUR_HDFS_SECRET_PATH:-}"

# Check if Conjur is configured
conjur_enabled() {
    [[ -n "${CONJUR_MQ_SECRET_PATH:-}" ]] && command -v conjur &>/dev/null
}

# Fetch a secret from Conjur
# Usage: fetch_secret "path/to/secret"
fetch_secret() {
    local secret_path="$1"
    conjur variable get -i "$secret_path" 2>/dev/null || {
        echo "ERROR: Failed to fetch secret: $secret_path" >&2
        return 1
    }
}

# Fetch secrets from Conjur and export as environment variables
fetch_conjur_secrets() {
    echo "Fetching secrets from CyberArk Conjur..."

    # MQ Credentials
    if [[ -n "${CONJUR_MQ_SECRET_PATH:-}" ]]; then
        export IBM_MQ_USER=$(fetch_secret "${CONJUR_MQ_SECRET_PATH}/username")
        export IBM_MQ_PASSWORD=$(fetch_secret "${CONJUR_MQ_SECRET_PATH}/password")
        echo "  MQ credentials loaded from Conjur"
    fi

    # HDFS/Kerberos Credentials (if using password-based auth)
    if [[ -n "${CONJUR_HDFS_SECRET_PATH:-}" ]]; then
        export KRB5_PASSWORD=$(fetch_secret "${CONJUR_HDFS_SECRET_PATH}/password")
        echo "  HDFS credentials loaded from Conjur"
    fi

    echo "Secrets loaded successfully"
}

# Alternative: Use Conjur Summon for secret injection
# If using summon, secrets are injected directly into the process environment
# See: https://cyberark.github.io/summon/
use_summon() {
    if command -v summon &>/dev/null && [[ -f "secrets.yml" ]]; then
        echo "Using Summon for secret injection"
        return 0
    fi
    return 1
}

# Main
if conjur_enabled; then
    fetch_conjur_secrets
elif use_summon; then
    echo "Summon will inject secrets at runtime"
else
    echo "Conjur not configured - using static credentials from env.sh"
fi
