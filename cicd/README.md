
# Microsoft Fabric CI/CD Guide

This document describes the CI/CD process for publishing Microsoft Fabric artifacts from a Git repository into Fabric workspaces using GitHub Actions and the `fabric-cicd` tooling. It is written for customers and operators responsible for configuring, running, and maintaining the deployment pipeline.

## Overview

- Purpose: Source-controlled deployment of Fabric artifacts (notebooks, pipelines, lakehouses, semantic models) across DEV → UAT → PROD.
- Scope: repository layout, prerequisites, parameterization, GitHub Actions workflow

Refer to the detailed checklist and design plan for operational tasks:

- [cicd/docs/checklist.md](cicd/docs/checklist.md#L1)
- [cicd/docs/plan.md](cicd/docs/plan.md#L1)

## Quick Prerequisites

- Azure: App Registration + Service Principal (Tenant ID, Client ID, Client Secret).
- Fabric tenant: enable "Service Principals can use Fabric APIs" and grant the SP access to workspaces.
- GitHub: repository created, Actions enabled, branch protection for `main`, and Environments for `fabric-test` and `fabric-prod`.
- Secrets: Store `TENANT_ID`, `CLIENT_ID`, `CLIENT_SECRET` at repository level; environment-specific secrets (workspace, lakehouse, connection IDs) at environment level.

## Repository Layout (recommended)

Standard structure expected by the deploy scripts and `fabric-cicd`:

```
repository-root/
├── .github/workflows/
│   └── fabric-deploy.yml
├── fabric/                # source artifacts (notebooks, pipelines, lakehouses, semanticModels)
├── scripts/               # deploy helpers (deploy_pipeline.py, deploy_powerbi.py)
```

Place all Fabric workspace items under the `fabric/` folder. Do not edit UAT/Prod workspaces directly — make all changes through Git and CI.


## GitHub Actions: CI/CD flow

Typical job flow (high level):

1. Pull Request: run validation (lint, item schema checks, optional unit tests).
2. Merge to `main`: trigger CD jobs.
3. Deploy to `fabric uat` environment: requires environment approval.
4. After verification, promote to `fabric production` environment: requires approval.

## Secrets and Environments
- Repository secrets (global): `TENANT_ID`, `CLIENT_ID`, `CLIENT_SECRET`.
- Environment secrets (per GitHub Environment): `WORKSPACE_ID`, `LAKEHOUSE_ID`, `CONNECTION_ID`, etc.
- Configure `UAT` and `PROD` environments with required approvers and reviewers.

## Validation and Smoke Tests

After deployment, run smoke tests to ensure:

- Notebooks can attach to the expected lakehouse.
- Pipelines are valid and runnable (basic compile/parse validation).
- Connections resolve and credentials work.

Add smoke test steps to the CD job or as a separate job that runs immediately after publish.

## Rollback Strategy

- Use Git to revert commits that updated `fabric/` and re-run the CD pipeline.
- Maintain concise runbook in `cicd/docs/` describing the revert steps and contacts.

## Operational Checklist

Before enabling automated production deployments, ensure the following are complete (see full checklist in `cicd/docs/checklist.md`):

- Service Principal and secrets are created and validated.
- Workspace, lakehouse, and connection IDs collected for all environments.
- GitHub Environments created and approvers configured for UAT & Prod.

## How to use (operator commands)

Local test (dry-run):

```bash
python cicd/scripts/github-action/deploy.py --workspaceid <ws-id> --aztenantid <aztenantid> --azclientid <azclientid> --azspsecret <azspsecret> --target_env <ENV> 

```

Trigger full deployment (CI): merge to `dev` and approve the `fabric UAT` environment; after validation, promote to `fabric production`.

## Support and maintenance

Store runbook information, on-call contacts, and rollback procedures in `cicd/docs/`.

---

Last updated: 2026-06-21
