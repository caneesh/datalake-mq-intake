#!/bin/bash
#
# Deploy MQ Intake artifacts to office directory structure.
#
# Transforms the unified source layout to the office deployment structure:
#   src/java/membership/riskstrat/[rms|claims]/
#   src/scripts/membership/riskstrat/[rms|claims|common]/
#   params/membership/riskstrat/[rms|claims]/
#
# Usage:
#   ./deploy-to-office.sh <source_dir> <dest_base> [module]
#
# Arguments:
#   source_dir  Path to the built project (contains rms/, claims/, scripts/)
#   dest_base   Base path for office structure deployment
#   module      Optional: 'rms', 'claims', or 'all' (default: all)
#
# Examples:
#   ./deploy-to-office.sh . /path/to/office/base
#   ./deploy-to-office.sh . /path/to/office/base rms
#

set -euo pipefail

SOURCE_DIR="${1:-.}"
DEST_BASE="${2:?Usage: $0 <source_dir> <dest_base> [module]}"
MODULE="${3:-all}"

# Resolve absolute paths
SOURCE_DIR="$(cd "$SOURCE_DIR" && pwd)"

echo "=============================================="
echo "MQ Intake Office Deployment"
echo "=============================================="
echo "Source:      $SOURCE_DIR"
echo "Destination: $DEST_BASE"
echo "Module:      $MODULE"
echo "Timestamp:   $(date '+%Y-%m-%d %H:%M:%S')"
echo ""

# Validate source
if [[ ! -f "$SOURCE_DIR/pom.xml" ]]; then
    echo "ERROR: $SOURCE_DIR does not appear to be the project root (no pom.xml)"
    exit 1
fi

# Check if built
check_artifact() {
    local artifact="$1"
    if [[ ! -f "$artifact" ]]; then
        echo "ERROR: Artifact not found: $artifact"
        echo "Run 'mvn clean package' first"
        exit 1
    fi
}

# Deploy a module
deploy_module() {
    local module="$1"
    local module_upper="${module^^}"

    echo "--- Deploying $module_upper ---"

    # Paths
    local java_dest="$DEST_BASE/src/java/membership/riskstrat/$module"
    local scripts_dest="$DEST_BASE/src/scripts/membership/riskstrat/$module"
    local params_dest="$DEST_BASE/params/membership/riskstrat/$module"

    # Check artifact exists
    check_artifact "$SOURCE_DIR/$module/target/app.jar"

    # Create directories
    mkdir -p "$java_dest"
    mkdir -p "$scripts_dest"
    mkdir -p "$params_dest"

    # Deploy Java artifact
    echo "  Java artifact -> $java_dest/"
    cp "$SOURCE_DIR/$module/target/app.jar" "$java_dest/"

    # Deploy server scripts
    if [[ -d "$SOURCE_DIR/scripts/server" ]]; then
        echo "  Server scripts -> $scripts_dest/"
        cp "$SOURCE_DIR/scripts/server/"*.sh "$scripts_dest/" 2>/dev/null || true
    fi

    # Deploy configuration template
    local config_source="$SOURCE_DIR/$module/src/main/resources/application.yml"
    if [[ -f "$config_source" ]]; then
        echo "  Config template -> $params_dest/"
        cp "$config_source" "$params_dest/application.yml.template"
    fi

    echo "  Done: $module_upper"
    echo ""
}

# Deploy shared resources
deploy_common() {
    echo "--- Deploying COMMON resources ---"

    local common_dest="$DEST_BASE/src/scripts/membership/riskstrat/common"

    # Control-M scripts
    if [[ -d "$SOURCE_DIR/scripts/controlm" ]]; then
        mkdir -p "$common_dest/controlm"
        echo "  Control-M scripts -> $common_dest/controlm/"
        cp -r "$SOURCE_DIR/scripts/controlm/"* "$common_dest/controlm/"
    fi

    # Documentation
    if [[ -d "$SOURCE_DIR/docs" ]]; then
        local docs_dest="$DEST_BASE/docs/membership/riskstrat/mq-intake"
        mkdir -p "$docs_dest"
        echo "  Documentation -> $docs_dest/"
        cp "$SOURCE_DIR/docs/"*.md "$docs_dest/" 2>/dev/null || true
    fi

    echo "  Done: COMMON"
    echo ""
}

# Main deployment logic
case "$MODULE" in
    rms)
        deploy_module "rms"
        deploy_common
        ;;
    claims)
        deploy_module "claims"
        deploy_common
        ;;
    all)
        deploy_module "rms"
        deploy_module "claims"
        deploy_common
        ;;
    *)
        echo "ERROR: Unknown module: $MODULE"
        echo "Valid options: rms, claims, all"
        exit 1
        ;;
esac

echo "=============================================="
echo "Deployment complete"
echo "=============================================="
echo ""
echo "Deployed structure:"
find "$DEST_BASE" -type f -name "*.jar" -o -name "*.sh" -o -name "*.yml*" -o -name "*.md" 2>/dev/null | sort | head -30
echo ""
echo "Next steps:"
echo "1. Copy env.sh to each module's deployment directory"
echo "2. Update environment-specific configuration"
echo "3. Run preflight checks before starting"
