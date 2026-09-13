#!/bin/bash
# ==============================================================================
# Test Conjur Integration
# ==============================================================================
#
# Tests the fetch_secrets.sh with mock Conjur CLI.
#
# Usage:
#   ./test_conjur_integration.sh
#
# ==============================================================================

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
MOCK_CONJUR="$SCRIPT_DIR/mock_conjur.sh"
FETCH_SECRETS="$SCRIPT_DIR/../fetch_secrets.sh"

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

pass() { echo -e "${GREEN}PASS${NC}: $1"; }
fail() { echo -e "${RED}FAIL${NC}: $1"; exit 1; }
info() { echo -e "${YELLOW}INFO${NC}: $1"; }

echo "=============================================="
echo "Testing CyberArk Conjur Integration"
echo "=============================================="
echo ""

# Make mock executable
chmod +x "$MOCK_CONJUR"

# ==============================================================================
# Test 1: Mock Conjur CLI works
# ==============================================================================
info "Test 1: Verify mock Conjur CLI"

RESULT=$("$MOCK_CONJUR" variable get -i "apps/mq-intake/rms/mq/username")
if [[ "$RESULT" == "test_mq_user" ]]; then
    pass "Mock Conjur CLI returns expected value"
else
    fail "Mock Conjur CLI failed: got '$RESULT'"
fi

# ==============================================================================
# Test 2: fetch_secrets.sh without Conjur config
# ==============================================================================
info "Test 2: fetch_secrets.sh without Conjur config (should skip)"

unset CONJUR_APPLIANCE_URL
unset CONJUR_MQ_SECRET_PATH
unset IBM_MQ_USER
unset IBM_MQ_PASSWORD

source "$FETCH_SECRETS"

if [[ -z "${IBM_MQ_USER:-}" ]]; then
    pass "No credentials set when Conjur not configured"
else
    fail "Credentials should not be set: IBM_MQ_USER=$IBM_MQ_USER"
fi

# ==============================================================================
# Test 3: fetch_secrets.sh with Conjur config
# ==============================================================================
info "Test 3: fetch_secrets.sh with mock Conjur"

export CONJUR_CLI="$MOCK_CONJUR"
export CONJUR_APPLIANCE_URL="https://mock.conjur.local"
export CONJUR_ACCOUNT="test"
export CONJUR_AUTHN_LOGIN="host/mq-intake/rms"
export CONJUR_MQ_SECRET_PATH="apps/mq-intake/rms/mq"

unset IBM_MQ_USER
unset IBM_MQ_PASSWORD

source "$FETCH_SECRETS"

if [[ "${IBM_MQ_USER:-}" == "test_mq_user" ]]; then
    pass "IBM_MQ_USER set correctly: $IBM_MQ_USER"
else
    fail "IBM_MQ_USER not set correctly: got '${IBM_MQ_USER:-}'"
fi

if [[ "${IBM_MQ_PASSWORD:-}" == "test_mq_password_123" ]]; then
    pass "IBM_MQ_PASSWORD set correctly: ***"
else
    fail "IBM_MQ_PASSWORD not set correctly"
fi

# ==============================================================================
# Test 4: Claims secrets
# ==============================================================================
info "Test 4: Claims secrets"

export CONJUR_MQ_SECRET_PATH="apps/mq-intake/claims/mq"

unset IBM_MQ_USER
unset IBM_MQ_PASSWORD

source "$FETCH_SECRETS"

if [[ "${IBM_MQ_USER:-}" == "test_claims_user" ]]; then
    pass "Claims IBM_MQ_USER set correctly: $IBM_MQ_USER"
else
    fail "Claims IBM_MQ_USER not set correctly: got '${IBM_MQ_USER:-}'"
fi

# ==============================================================================
# Test 5: HDFS secrets
# ==============================================================================
info "Test 5: HDFS secrets (optional)"

export CONJUR_MQ_SECRET_PATH="apps/mq-intake/rms/mq"
export CONJUR_HDFS_SECRET_PATH="apps/mq-intake/rms/hdfs"

unset IBM_MQ_USER
unset IBM_MQ_PASSWORD
unset KRB5_PASSWORD

source "$FETCH_SECRETS"

if [[ "${KRB5_PASSWORD:-}" == "test_hdfs_password" ]]; then
    pass "KRB5_PASSWORD set correctly: ***"
else
    fail "KRB5_PASSWORD not set correctly"
fi

# ==============================================================================
# Summary
# ==============================================================================
echo ""
echo "=============================================="
echo -e "${GREEN}All tests passed!${NC}"
echo "=============================================="
echo ""
echo "The Conjur integration is working correctly."
echo "To use with real Conjur, update env.sh with:"
echo "  CONJUR_APPLIANCE_URL=https://your-conjur-server"
echo "  CONJUR_ACCOUNT=your-account"
echo "  CONJUR_AUTHN_LOGIN=host/your-host-identity"
echo "  CONJUR_MQ_SECRET_PATH=path/to/mq/secrets"
