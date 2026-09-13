#!/bin/bash
# ==============================================================================
# Mock Conjur CLI for testing
# ==============================================================================
#
# Simulates the Conjur CLI for local testing without a real Conjur server.
#
# Setup:
#   chmod +x mock_conjur.sh
#   export CONJUR_CLI=/path/to/mock_conjur.sh
#
# Usage:
#   mock_conjur.sh variable get -i <secret_path>
#
# ==============================================================================

# Mock secrets - customize for testing
declare -A MOCK_SECRETS=(
    ["apps/mq-intake/rms/mq/username"]="test_mq_user"
    ["apps/mq-intake/rms/mq/password"]="test_mq_password_123"
    ["apps/mq-intake/claims/mq/username"]="test_claims_user"
    ["apps/mq-intake/claims/mq/password"]="test_claims_password_456"
    ["apps/mq-intake/rms/hdfs/password"]="test_hdfs_password"
)

# Parse arguments
COMMAND="${1:-}"
SUBCOMMAND="${2:-}"

case "$COMMAND" in
    variable)
        case "$SUBCOMMAND" in
            get)
                # Extract path from -i flag
                shift 2
                while [[ $# -gt 0 ]]; do
                    case "$1" in
                        -i)
                            SECRET_PATH="$2"
                            shift 2
                            ;;
                        *)
                            SECRET_PATH="$1"
                            shift
                            ;;
                    esac
                done

                if [[ -n "${MOCK_SECRETS[$SECRET_PATH]:-}" ]]; then
                    echo "${MOCK_SECRETS[$SECRET_PATH]}"
                    exit 0
                else
                    echo "error: variable not found: $SECRET_PATH" >&2
                    exit 1
                fi
                ;;
            *)
                echo "Unknown subcommand: $SUBCOMMAND" >&2
                exit 1
                ;;
        esac
        ;;
    *)
        echo "Mock Conjur CLI"
        echo "Usage: $0 variable get -i <secret_path>"
        exit 1
        ;;
esac
