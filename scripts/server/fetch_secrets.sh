#!/bin/bash
# ==============================================================================
# Fetch secrets from CyberArk Conjur
# ==============================================================================
#
# This script retrieves MQ and HDFS credentials from CyberArk Conjur
# and exports them as environment variables for the application.
#
# Prerequisites:
#   - Conjur CLI installed: /opt/conjur/bin/conjur
#   - OR curl available for REST API fallback
#   - Host identity file or API key configured
#
# Required Environment Variables:
#   CONJUR_APPLIANCE_URL  - Conjur server URL
#   CONJUR_ACCOUNT        - Conjur account name
#   CONJUR_AUTHN_LOGIN    - Host identity (e.g., host/mq-intake/rms)
#   CONJUR_MQ_SECRET_PATH - Path to MQ secrets in Conjur
#
# Optional:
#   CONJUR_AUTHN_API_KEY  - API key (if not using host identity file)
#   CONJUR_CERT_FILE      - Path to Conjur SSL certificate
#   CONJUR_HDFS_SECRET_PATH - Path to HDFS secrets (if password-based auth)
#
# ==============================================================================

set -euo pipefail

# Conjur CLI path
CONJUR_CLI="${CONJUR_CLI:-/opt/conjur/bin/conjur}"

# Log function
log() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] CONJUR: $*"
}

log_error() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] CONJUR ERROR: $*" >&2
}

# ==============================================================================
# Check if Conjur is configured
# ==============================================================================
conjur_enabled() {
    if [[ -z "${CONJUR_APPLIANCE_URL:-}" ]]; then
        return 1
    fi
    if [[ -z "${CONJUR_MQ_SECRET_PATH:-}" ]]; then
        return 1
    fi
    return 0
}

# ==============================================================================
# Authenticate with Conjur and get access token
# ==============================================================================
conjur_authenticate() {
    local api_key="${CONJUR_AUTHN_API_KEY:-}"
    local token=""

    # If no API key provided, try to get it from host identity file
    if [[ -z "$api_key" ]]; then
        local host_identity_file="${CONJUR_HOST_IDENTITY_FILE:-/etc/conjur/identity}"
        if [[ -f "$host_identity_file" ]]; then
            api_key=$(grep -E "^api_key:" "$host_identity_file" | cut -d: -f2 | tr -d ' ')
        fi
    fi

    if [[ -z "$api_key" ]]; then
        log_error "No API key found. Set CONJUR_AUTHN_API_KEY or configure host identity file."
        return 1
    fi

    # Build curl options
    local curl_opts=(-s -X POST)
    if [[ -n "${CONJUR_CERT_FILE:-}" ]]; then
        curl_opts+=(--cacert "$CONJUR_CERT_FILE")
    fi

    # URL encode the login
    local encoded_login
    encoded_login=$(echo -n "${CONJUR_AUTHN_LOGIN}" | sed 's|/|%2F|g')

    # Authenticate and get token
    token=$(curl "${curl_opts[@]}" \
        -d "$api_key" \
        "${CONJUR_APPLIANCE_URL}/authn/${CONJUR_ACCOUNT}/${encoded_login}/authenticate")

    if [[ -z "$token" ]]; then
        log_error "Authentication failed - empty token"
        return 1
    fi

    # Token is returned as data, need to base64 encode for API calls
    CONJUR_ACCESS_TOKEN=$(echo -n "$token" | base64 | tr -d '\n')
    export CONJUR_ACCESS_TOKEN
    log "Authentication successful"
}

# ==============================================================================
# Fetch a secret value from Conjur
# ==============================================================================
conjur_get_secret() {
    local secret_path="$1"
    local secret_value=""

    # URL encode the path
    local encoded_path
    encoded_path=$(echo -n "$secret_path" | sed 's|/|%2F|g')

    # Build curl options
    local curl_opts=(-s -H "Authorization: Token token=\"${CONJUR_ACCESS_TOKEN}\"")
    if [[ -n "${CONJUR_CERT_FILE:-}" ]]; then
        curl_opts+=(--cacert "$CONJUR_CERT_FILE")
    fi

    # Fetch the secret
    secret_value=$(curl "${curl_opts[@]}" \
        "${CONJUR_APPLIANCE_URL}/secrets/${CONJUR_ACCOUNT}/variable/${encoded_path}")

    if [[ -z "$secret_value" ]]; then
        log_error "Failed to fetch secret: $secret_path"
        return 1
    fi

    echo "$secret_value"
}

# ==============================================================================
# Alternative: Use Conjur CLI if available
# ==============================================================================
conjur_cli_get_secret() {
    local secret_path="$1"

    if [[ ! -x "$CONJUR_CLI" ]]; then
        return 1
    fi

    "$CONJUR_CLI" variable get -i "$secret_path" 2>/dev/null
}

# ==============================================================================
# Fetch all required secrets
# ==============================================================================
fetch_mq_secrets() {
    log "Fetching MQ credentials from: ${CONJUR_MQ_SECRET_PATH}"

    local username password

    # Try CLI first, fall back to REST API
    if [[ -x "$CONJUR_CLI" ]]; then
        log "Using Conjur CLI"
        username=$(conjur_cli_get_secret "${CONJUR_MQ_SECRET_PATH}/username") || return 1
        password=$(conjur_cli_get_secret "${CONJUR_MQ_SECRET_PATH}/password") || return 1
    else
        log "Using Conjur REST API"
        conjur_authenticate || return 1
        username=$(conjur_get_secret "${CONJUR_MQ_SECRET_PATH}/username") || return 1
        password=$(conjur_get_secret "${CONJUR_MQ_SECRET_PATH}/password") || return 1
    fi

    export IBM_MQ_USER="$username"
    export IBM_MQ_PASSWORD="$password"

    log "MQ credentials loaded successfully (user: $username)"
}

fetch_hdfs_secrets() {
    if [[ -z "${CONJUR_HDFS_SECRET_PATH:-}" ]]; then
        return 0
    fi

    log "Fetching HDFS credentials from: ${CONJUR_HDFS_SECRET_PATH}"

    local password

    if [[ -x "$CONJUR_CLI" ]]; then
        password=$(conjur_cli_get_secret "${CONJUR_HDFS_SECRET_PATH}/password") || return 1
    else
        password=$(conjur_get_secret "${CONJUR_HDFS_SECRET_PATH}/password") || return 1
    fi

    export KRB5_PASSWORD="$password"
    log "HDFS credentials loaded successfully"
}

# ==============================================================================
# Main
# ==============================================================================
main() {
    if ! conjur_enabled; then
        log "Conjur not configured - using static credentials from env.sh"
        return 0
    fi

    log "CyberArk Conjur integration enabled"
    log "Server: ${CONJUR_APPLIANCE_URL}"
    log "Account: ${CONJUR_ACCOUNT}"
    log "Identity: ${CONJUR_AUTHN_LOGIN}"

    # Fetch MQ secrets (required)
    if ! fetch_mq_secrets; then
        log_error "Failed to fetch MQ secrets from Conjur"
        return 1
    fi

    # Fetch HDFS secrets (optional)
    if ! fetch_hdfs_secrets; then
        log_error "Failed to fetch HDFS secrets from Conjur"
        return 1
    fi

    log "All secrets loaded from CyberArk Conjur"
}

# Run main
main "$@"
