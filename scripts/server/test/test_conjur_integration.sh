#!/bin/bash
# Exercises fetch_secrets.sh against the mock Conjur CLI. No real Conjur needed.
#
#   ./test_conjur_integration.sh

set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
MOCK_CONJUR="$SCRIPT_DIR/mock_conjur.sh"
FETCH_SECRETS="$SCRIPT_DIR/../fetch_secrets.sh"
chmod +x "$MOCK_CONJUR"

GREEN='\033[0;32m'; RED='\033[0;31m'; YELLOW='\033[1;33m'; NC='\033[0m'
failures=0
pass() { echo -e "${GREEN}PASS${NC}: $1"; }
fail() { echo -e "${RED}FAIL${NC}: $1"; failures=$((failures + 1)); }
info() { echo -e "${YELLOW}----${NC} $1"; }

reset_env() {
    unset CONJUR_APPLIANCE_URL CONJUR_ACCOUNT CONJUR_AUTHN_LOGIN CONJUR_MQ_SECRET_PATH
    unset MQ_USER MQ_PASSWORD MQ_CREDENTIAL_REF CONJUR_ACCESS_TOKEN
    export CONJUR_CLI="$MOCK_CONJUR"
}

enable_conjur() {
    export CONJUR_APPLIANCE_URL="https://mock.conjur.local"
    export CONJUR_ACCOUNT="test"
    export CONJUR_AUTHN_LOGIN="host/mq-intake/rms"
    export CONJUR_MQ_SECRET_PATH="$1"
}

info "1: mock CLI answers"
out=$("$MOCK_CONJUR" variable get -i apps/mq-intake/rms/mq/username)
[[ "$out" == "test_mq_user" ]] && pass "mock returns username" || fail "mock returned '$out'"

info "2: Conjur not configured leaves credentials untouched"
reset_env
source "$FETCH_SECRETS"; rc=$?
[[ $rc -eq 0 && -z "${MQ_USER:-}" ]] && pass "no-op, exit 0" || fail "rc=$rc MQ_USER='${MQ_USER:-}'"

info "3: RMS credentials exported under the names the app reads"
reset_env; enable_conjur apps/mq-intake/rms/mq
source "$FETCH_SECRETS"; rc=$?
[[ $rc -eq 0 ]] && pass "exit 0" || fail "rc=$rc"
[[ "${MQ_USER:-}" == "test_mq_user" ]] && pass "MQ_USER=$MQ_USER" || fail "MQ_USER='${MQ_USER:-}'"
[[ "${MQ_PASSWORD:-}" == "test_mq_password_123" ]] && pass "MQ_PASSWORD set" || fail "MQ_PASSWORD wrong"
[[ "${MQ_CREDENTIAL_REF:-}" == "env:MQ_USER,MQ_PASSWORD" ]] && pass "MQ_CREDENTIAL_REF=$MQ_CREDENTIAL_REF" \
    || fail "MQ_CREDENTIAL_REF='${MQ_CREDENTIAL_REF:-}'"

info "4: Claims path"
reset_env; enable_conjur apps/mq-intake/claims/mq
source "$FETCH_SECRETS" > /dev/null
[[ "${MQ_USER:-}" == "test_claims_user" ]] && pass "MQ_USER=$MQ_USER" || fail "MQ_USER='${MQ_USER:-}'"

info "5: static MQ_USER alongside Conjur is refused"
reset_env; enable_conjur apps/mq-intake/rms/mq
export MQ_USER=static_user MQ_PASSWORD=static_pw
source "$FETCH_SECRETS" > /dev/null 2>&1; rc=$?
[[ $rc -ne 0 ]] && pass "refused (rc=$rc)" || fail "accepted static override"
[[ "${MQ_USER}" == "static_user" ]] && pass "static value left as-is for the error message" || fail "MQ_USER mutated"

info "6: missing secret fails"
reset_env; enable_conjur apps/mq-intake/nothere/mq
source "$FETCH_SECRETS" > /dev/null 2>&1; rc=$?
[[ $rc -ne 0 && -z "${MQ_USER:-}" ]] && pass "failed without exporting partial credentials" \
    || fail "rc=$rc MQ_USER='${MQ_USER:-}'"

echo
if (( failures == 0 )); then
    echo -e "${GREEN}All tests passed.${NC}"
else
    echo -e "${RED}${failures} test(s) failed.${NC}"
    exit 1
fi
