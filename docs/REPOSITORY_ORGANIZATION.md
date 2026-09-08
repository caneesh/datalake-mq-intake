# Repository Organization Strategy

This document analyzes two approaches to organizing the MQ Intake codebase in Git and provides recommendations for source structure vs. deployment structure.

## Table of Contents

- [Background](#background)
- [Option A: Unified Project Structure](#option-a-unified-project-structure)
- [Option B: Match Office Deployment Structure](#option-b-match-office-deployment-structure)
- [Comparison](#comparison)
- [Recommendation](#recommendation)
- [Implementation](#implementation)
- [When to Use Each Approach](#when-to-use-each-approach)

---

## Background

### The Question

The office deployment structure organizes files as:

```
<base>/src/java/membership/riskstrat/[rms|claims]/
<base>/src/scripts/membership/riskstrat/[rms|claims]/
<base>/params/membership/riskstrat/[rms|claims]/
```

Should the Git repository:
- **Option A**: Use a unified project structure and transform during deployment?
- **Option B**: Mirror the office deployment structure directly?

---

## Option A: Unified Project Structure

### Git Layout

```
datalake-mq-intake/
├── core/
│   ├── pom.xml
│   └── src/main/java/com/hcsc/datalake/mqintake/core/
├── rms/
│   ├── pom.xml
│   └── src/main/java/com/hcsc/datalake/mqintake/rms/
├── claims/
│   ├── pom.xml
│   └── src/main/java/com/hcsc/datalake/mqintake/claims/
├── scripts/
│   ├── controlm/
│   └── server/
├── docs/
└── pom.xml (parent)
```

### Deployment Transformation

```
Git Repository                    →    Office Deployment Structure
─────────────────────────────────      ────────────────────────────────────
core/target/core.jar              →    (embedded in app.jar)
rms/target/app.jar                →    src/java/membership/riskstrat/rms/
claims/target/app.jar             →    src/java/membership/riskstrat/claims/
scripts/server/intake.sh          →    src/scripts/membership/riskstrat/rms/
scripts/controlm/*.sh             →    src/scripts/membership/riskstrat/common/
rms/src/main/resources/*.yml      →    params/membership/riskstrat/rms/
claims/src/main/resources/*.yml   →    params/membership/riskstrat/claims/
```

### Characteristics

| Aspect | Description |
|--------|-------------|
| Source organization | Standard Maven/Gradle multi-module project |
| Module dependencies | Natural: `rms` depends on `core`, `claims` depends on `core` |
| Build process | Standard `mvn clean install` |
| Deployment | Requires transformation script |

---

## Option B: Match Office Deployment Structure

### Git Layout

```
<base>/
├── src/
│   ├── java/
│   │   └── membership/
│   │       └── riskstrat/
│   │           ├── core/
│   │           ├── rms/
│   │           └── claims/
│   └── scripts/
│       └── membership/
│           └── riskstrat/
│               ├── common/
│               ├── rms/
│               └── claims/
└── params/
    └── membership/
        └── riskstrat/
            ├── rms/
            └── claims/
```

### Deployment

```
Git Repository                    →    Office Deployment Structure
─────────────────────────────────      ────────────────────────────────────
(direct copy, no transformation)
```

### Characteristics

| Aspect | Description |
|--------|-------------|
| Source organization | Mirrors deployment paths |
| Module dependencies | Requires custom build configuration |
| Build process | Non-standard, requires custom setup |
| Deployment | Direct copy, no transformation needed |

---

## Comparison

### Developer Experience

| Criterion | Option A (Unified) | Option B (Match Office) |
|-----------|-------------------|------------------------|
| **Project navigation** | ✅ Shallow, logical paths | ❌ Deep nested paths |
| **IDE support** | ✅ Single project root | ⚠️ May need multiple roots |
| **Code search** | ✅ All code together | ⚠️ Scattered across tree |
| **Onboarding** | ✅ Standard structure | ❌ Enterprise-specific |

### Build and Dependencies

| Criterion | Option A (Unified) | Option B (Match Office) |
|-----------|-------------------|------------------------|
| **Maven/Gradle** | ✅ Standard multi-module | ❌ Non-standard layout |
| **Shared code (core)** | ✅ Natural dependency | ❌ Cross-path references |
| **Dependency management** | ✅ Parent POM inheritance | ⚠️ Complex configuration |
| **Build reproducibility** | ✅ Standard tooling | ⚠️ Custom scripts needed |

### Development Workflow

| Criterion | Option A (Unified) | Option B (Match Office) |
|-----------|-------------------|------------------------|
| **Code reviews** | ✅ Related changes together | ❌ Changes scattered |
| **Refactoring** | ✅ IDE refactoring works | ❌ Cross-directory issues |
| **Testing** | ✅ Standard test execution | ⚠️ Test path configuration |
| **Local development** | ✅ Standard workflow | ⚠️ Path setup needed |

### Deployment and Operations

| Criterion | Option A (Unified) | Option B (Match Office) |
|-----------|-------------------|------------------------|
| **Deployment complexity** | ⚠️ Transformation needed | ✅ Direct copy |
| **Path mapping** | ⚠️ Must be documented | ✅ Self-evident |
| **Troubleshooting** | ⚠️ Mental mapping needed | ✅ Source = production |
| **Compliance/audit** | ⚠️ Mapping documentation | ✅ Direct correspondence |

### CI/CD

| Criterion | Option A (Unified) | Option B (Match Office) |
|-----------|-------------------|------------------------|
| **Pipeline setup** | ✅ Standard templates | ⚠️ Custom configuration |
| **Artifact handling** | ✅ Standard conventions | ⚠️ Path-aware scripts |
| **Multi-environment** | ✅ Standard parameterization | ✅ Direct mapping |

---

## Recommendation

### Primary Recommendation: Option A (Unified Project)

**Use a unified project structure in Git with deployment transformation.**

### Rationale

1. **Build tools expect standard layouts**

   Maven and Gradle are designed for:
   ```
   module/src/main/java/...
   module/src/main/resources/...
   module/src/test/java/...
   ```
   
   Not:
   ```
   src/java/membership/riskstrat/module/...
   ```
   
   Fighting standard conventions creates ongoing friction.

2. **Shared code requires natural dependencies**

   The `core` module contains shared functionality used by both `rms` and `claims`:
   - Transaction management
   - Batch processing
   - HDFS writing
   - Metrics and health
   
   In Option A, this is a natural Maven dependency:
   ```xml
   <dependency>
       <groupId>com.hcsc.datalake</groupId>
       <artifactId>mq-intake-core</artifactId>
   </dependency>
   ```
   
   In Option B, `core` doesn't fit the path pattern and requires workarounds.

3. **Source organization ≠ deployment organization**

   These optimize for different goals:
   
   | Organization | Optimizes For |
   |--------------|---------------|
   | Source | Development, building, testing, reviewing |
   | Deployment | Operations, compliance, corporate standards |
   
   Conflating them compromises both.

4. **Transformation is cheap; bad structure is expensive**

   - Deploy script that maps files: ~20 lines, written once
   - Non-standard source layout: ongoing friction in every development task

5. **Industry standard practice**

   Most projects use clean source structures and transform at deployment:
   - Spring Boot → Docker image
   - Node.js → Lambda package
   - Java → WAR/EAR with specific structure

---

## Implementation

### Deployment Script

Create a deployment script that transforms source layout to office structure:

```bash
#!/bin/bash
# scripts/deploy-to-office.sh

set -euo pipefail

SOURCE_DIR="${1:-.}"
DEST_BASE="${2:?Usage: $0 <source_dir> <dest_base>}"

echo "Deploying from $SOURCE_DIR to $DEST_BASE"

# Create destination structure
mkdir -p "$DEST_BASE/src/java/membership/riskstrat/rms"
mkdir -p "$DEST_BASE/src/java/membership/riskstrat/claims"
mkdir -p "$DEST_BASE/src/scripts/membership/riskstrat/common/controlm"
mkdir -p "$DEST_BASE/src/scripts/membership/riskstrat/rms"
mkdir -p "$DEST_BASE/src/scripts/membership/riskstrat/claims"
mkdir -p "$DEST_BASE/params/membership/riskstrat/rms"
mkdir -p "$DEST_BASE/params/membership/riskstrat/claims"

# Java artifacts
cp "$SOURCE_DIR/rms/target/app.jar" \
   "$DEST_BASE/src/java/membership/riskstrat/rms/"
cp "$SOURCE_DIR/claims/target/app.jar" \
   "$DEST_BASE/src/java/membership/riskstrat/claims/"

# Server scripts
cp "$SOURCE_DIR/scripts/server/"*.sh \
   "$DEST_BASE/src/scripts/membership/riskstrat/rms/"
cp "$SOURCE_DIR/scripts/server/"*.sh \
   "$DEST_BASE/src/scripts/membership/riskstrat/claims/"

# Control-M scripts (shared)
cp -r "$SOURCE_DIR/scripts/controlm/"* \
   "$DEST_BASE/src/scripts/membership/riskstrat/common/controlm/"

# Configuration templates
cp "$SOURCE_DIR/rms/src/main/resources/application.yml" \
   "$DEST_BASE/params/membership/riskstrat/rms/"
cp "$SOURCE_DIR/claims/src/main/resources/application.yml" \
   "$DEST_BASE/params/membership/riskstrat/claims/"

echo "Deployment complete"
```

### Deployment Manifest

For more complex deployments, use a manifest file:

```yaml
# deploy-manifest.yml
#
# Maps source paths to office deployment paths.
# Used by the deployment pipeline.

version: "1.0"
description: "MQ Intake deployment mapping"

mappings:
  # Java artifacts
  - source: rms/target/app.jar
    dest: src/java/membership/riskstrat/rms/app.jar
    type: file
    
  - source: claims/target/app.jar
    dest: src/java/membership/riskstrat/claims/app.jar
    type: file

  # Server scripts
  - source: scripts/server/
    dest: src/scripts/membership/riskstrat/rms/
    type: directory
    include: "*.sh"
    
  - source: scripts/server/
    dest: src/scripts/membership/riskstrat/claims/
    type: directory
    include: "*.sh"

  # Control-M scripts (shared)
  - source: scripts/controlm/
    dest: src/scripts/membership/riskstrat/common/controlm/
    type: directory

  # Configuration
  - source: rms/src/main/resources/application.yml
    dest: params/membership/riskstrat/rms/application.yml
    type: file
    
  - source: claims/src/main/resources/application.yml
    dest: params/membership/riskstrat/claims/application.yml
    type: file

  # Documentation
  - source: docs/
    dest: docs/membership/riskstrat/mq-intake/
    type: directory
```

### CI/CD Integration

```yaml
# .github/workflows/deploy.yml (example)
jobs:
  deploy:
    steps:
      - name: Build
        run: mvn clean package -DskipTests
        
      - name: Transform to office structure
        run: ./scripts/deploy-to-office.sh . ${{ env.DEPLOY_STAGING }}
        
      - name: Deploy to target
        run: rsync -av ${{ env.DEPLOY_STAGING }}/ ${{ env.DEPLOY_TARGET }}/
```

---

## When to Use Each Approach

### Use Option A (Unified) When:

| Scenario | Reason |
|----------|--------|
| Multi-module project with shared code | Natural dependency management |
| Standard build tools (Maven/Gradle) | Leverage standard conventions |
| Active development | Optimize for developer experience |
| Multiple contributors | Standard structure aids onboarding |
| CI/CD pipelines | Standard tooling integration |

### Use Option B (Match Office) When:

| Scenario | Reason |
|----------|--------|
| Scripts-only repository | No build tool constraints |
| No shared code between modules | No dependency problem |
| Regulatory requirement | Audit needs source = production |
| Legacy migration | Code already in that structure |
| Separate team ownership | Different teams own different paths |
| Configuration-only repository | Direct deployment simplifies ops |

---

## Summary

| Aspect | Recommendation |
|--------|----------------|
| **Git structure** | Unified project (Option A) |
| **Build** | Standard Maven multi-module |
| **Shared code** | `core` module as dependency |
| **Deployment** | Transform via script/manifest |
| **Documentation** | Manifest file documents mapping |

The office path structure is a **deployment artifact**, not a source organization principle. Keep source optimized for development; transform at deployment time.

---

## Related Documents

- [DEPLOYMENT.md](DEPLOYMENT.md) — Deployment procedures
- [CONTROLM_SETUP.md](CONTROLM_SETUP.md) — Control-M job setup
- [CLAUDE.md](../CLAUDE.md) — Project conventions
