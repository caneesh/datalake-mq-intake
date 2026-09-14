#!/bin/bash
# ==============================================================================
# Fetch MQ credentials from CyberArk Conjur
# ==============================================================================
#
# Sourced by intake.sh (start and preflight only) AFTER env.sh. Exports the
# same variables the application reads for static credentials:
#
#   MQ_USER, MQ_PASSWORD, and MQ_CREDENTIAL_REF="env:MQ_USER,MQ_PASSWORD"
#
# Enabled when both of these are set (normally in env.sh):
#   CONJUR_APPLIANCE_URL  - Conjur server URL
#   CONJUR_MQ_SECRET_PATH - variable path prefix; "<path>/username" and
#                           "<path>/password" are fetched
#
# Also read:
#   CONJUR_ACCOUNT           - Conjur account (REST API)
#   CONJUR_AUTHN_LOGIN       - host identity, e.g. host/mq-intake/rms (REST API)
#   CONJUR_AUTHN_API_KEY     - API key; else read from CONJUR_HOST_IDENTITY_FILE
#   CONJUR_HOST_IDENTITY_FILE - default /etc/conjur/identity
#   CONJUR_CERT_FILE         - CA certificate for the appliance
#   CONJUR_CLI               - CLI binary; used instead of REST when executable
#                              (default /opt/conjur/bin/conjur)
#
# When Conjur is enabled, MQ_USER / MQ_PASSWORD must NOT also be set in
# env.sh: a static value there would silently win over the vault, so it is
# refused rather than merged.
#
# ==============================================================================

CONJUR_CLI="${CONJUR_CLI:-/opt/conjur/bin/conjur}"

conjur_log() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] CONJUR: $*"
}

conjur_log_error() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] CONJUR ERROR: $*" >&2
}

conjur_enabled() {
    [[ -n "${CONJUR_APPLIANCE_URL:-}" && -n "${CONJUR_MQ_SECRET_PATH:-}" ]]
}

conjur_curl_opts() {
    CURL_OPTS=(-s --fail --max-time 15)
    if [[ -n "${CONJUR_CERT_FILE:-}" ]]; then
        CURL_OPTS+=(--cacert "$CONJUR_CERT_FILE")
    fi
}

conjur_url_encode() {
    echo -n "$1" | sed 's|/|%2F|g'
}

conjur_authenticate() {
    local api_key="${CONJUR_AUTHN_API_KEY:-}"

    if [[ -z "$api_key" ]]; then
        local identity_file="${CONJUR_HOST_IDENTITY_FILE:-/etc/conjur/identity}"
        if [[ -f "$identity_file" ]]; then
            api_key=$(grep -E "^api_key:" "$identity_file" | cut -d: -f2- | tr -d ' ')
        fi
    fi

    if [[ -z "$api_key" ]]; then
        conjur_log_error "No API key: set CONJUR_AUTHN_API_KEY or provide the host identity file"
        return 1
    fi
    if [[ -z "${CONJUR_ACCOUNT:-}" || -z "${CONJUR_AUTHN_LOGIN:-}" ]]; then
        conjur_log_error "REST authentication needs CONJUR_ACCOUNT and CONJUR_AUTHN_LOGIN"
        return 1
    fi

    conjur_curl_opts
    local token
    token=$(curl "${CURL_OPTS[@]}" -X POST -d "$api_key" \
        "${CONJUR_APPLIANCE_URL}/authn/${CONJUR_ACCOUNT}/$(conjur_url_encode "$CONJUR_AUTHN_LOGIN")/authenticate") \
        || { conjur_log_error "Authentication request failed"; return 1; }

    if [[ -z "$token" ]]; then
        conjur_log_error "Authentication returned an empty token"
        return 1
    fi

    CONJUR_ACCESS_TOKEN=$(echo -n "$token" | base64 | tr -d '\n')
    conjur_log "Authenticated as ${CONJUR_AUTHN_LOGIN}"
}

conjur_rest_get_secret() {
    conjur_curl_opts
    curl "${CURL_OPTS[@]}" -H "Authorization: Token token=\"${CONJUR_ACCESS_TOKEN}\"" \
        "${CONJUR_APPLIANCE_URL}/secrets/${CONJUR_ACCOUNT}/variable/$(conjur_url_encode "$1")"
}

conjur_cli_get_secret() {
    "$CONJUR_CLI" variable get -i "$1" 2>/dev/null
}

conjur_get_secret() {
    local value
    if [[ -x "$CONJUR_CLI" ]]; then
        value=$(conjur_cli_get_secret "$1") || { conjur_log_error "CLI could not read $1"; return 1; }
    else
        value=$(conjur_rest_get_secret "$1") || { conjur_log_error "REST could not read $1"; return 1; }
    fi
    if [[ -z "$value" ]]; then
        conjur_log_error "Secret $1 is empty"
        return 1
    fi
    printf '%s' "$value"
}

conjur_fetch_mq_credentials() {
    if ! conjur_enabled; then
        return 0
    fi

    if [[ -n "${MQ_USER:-}" || -n "${MQ_PASSWORD:-}" ]]; then
        conjur_log_error "Conjur is enabled but MQ_USER/MQ_PASSWORD are also set in env.sh."
        conjur_log_error "Remove the static values — they would override the vault silently."
        return 1
    fi

    conjur_log "Fetching MQ credentials from ${CONJUR_APPLIANCE_URL} path ${CONJUR_MQ_SECRET_PATH}"
    if [[ -x "$CONJUR_CLI" ]]; then
        conjur_log "Using Conjur CLI ${CONJUR_CLI}"
    else
        conjur_log "Using Conjur REST API"
        conjur_authenticate || return 1
    fi

    local username password
    username=$(conjur_get_secret "${CONJUR_MQ_SECRET_PATH}/username") || return 1
    password=$(conjur_get_secret "${CONJUR_MQ_SECRET_PATH}/password") || return 1

    export MQ_USER="$username"
    export MQ_PASSWORD="$password"
    export MQ_CREDENTIAL_REF="env:MQ_USER,MQ_PASSWORD"
    unset CONJUR_ACCESS_TOKEN

    conjur_log "MQ credentials loaded (user: ${MQ_USER})"
}

conjur_fetch_mq_credentials
