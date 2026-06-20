# Fabric CI/CD Solution with GitHub Actions

## 1. Context

Source code is managed on GitHub. In the repository, all Fabric
artifacts are organized in the `/fabric` folder.

------------------------------------------------------------------------

# 2. Repository Structure

Recommended source structure:

    repository-root/

    ├── .github/
    │   └── workflows/
    │       └── fabric-deploy.yml
    │
    ├── fabric/
    │   │
    │   ├── notebooks/
    │   │
    │   ├── pipelines/
    │   │
    │   ├── lakehouses/
    │   │
    │   ├── semanticModels/
    │   │
    │   └── other Fabric items
    │
    ├── scripts/
    │   └── deploy_*.py
    │
    ├── parameter.yml
    │
    └── requirements.txt

Folder:

    /fabric

is the folder that contains all Fabric workspace artifacts managed in Git.

Mapping:

    GitHub Repository
            |
            |
            v
    /fabric folder
            |
            |
            v
    Fabric Workspace

------------------------------------------------------------------------

# 3. Proposed Solution

Build a CI/CD pipeline based on:

-   GitHub Repository:
    -   Source control for Fabric artifacts.
-   GitHub Actions:
    -   Automation pipeline.
-   fabric-cicd:
    -   Publish Fabric items from the `/fabric` folder.
-   GitHub Environment:
    -   Approval workflows for Test/Prod.
-   GitHub Secrets:
    -   Credentials management.

------------------------------------------------------------------------

# 4. Architecture

    Feature Branch

          |
          |
    Pull Request

          |
          v

    GitHub Actions CI
    (validate)

          |
          |
    Merge main

          |
          v

    GitHub Actions CD

          |
          v

    fabric-cicd publish_all_items()

          |
          |
          v

     Workspace
------------------------------------------------------------------------

# 5. Components

  Component           Role
  -------------------- -----------------------------------------------
  `/fabric` folder     Contains all Fabric artifacts
  fabric-cicd          Deploys artifacts from Git to Fabric workspace
  parameter.yml        Maps GUIDs between environments
  GitHub Actions       CI/CD workflows
  GitHub Environment   Approval gate
  GitHub Secrets       Credential management
  Azure Key Vault      Optional secret management

------------------------------------------------------------------------

# 6. GitHub Actions Deployment Flow

Workflow:

    Checkout Repository

            |

    Read /fabric folder

            |

    Install fabric-cicd

            |

    Authenticate Azure

            |

    Publish Fabric Items

            |

    Deploy Workspace

Example:

`.github/workflows/fabric-deploy.yml`

``` yaml
name: Fabric Deployment

on:

  push:
    branches:
      - main


jobs:

  deploy-test:

    runs-on: ubuntu-latest

    environment:
      name: fabric-test


    steps:

    - name: Checkout
      uses: actions/checkout@v4


    - name: Setup Python

      uses: actions/setup-python@v5

      with:
        python-version: '3.11'


    - name: Install dependencies

      run: |

        pip install fabric-cicd


    - name: Deploy Fabric

      run: |

        python scripts/deploy_*.py
```

------------------------------------------------------------------------

# 7. Deploy Script

`scripts/deploy_*.py`

Flow:

    /fabric
        |
        |
    fabric-cicd
        |
        |
    publish_all_items()
        |
        |
    Fabric Workspace

The script will:

1.  Authenticate with Fabric.
2.  Load source from `/fabric`.
3.  Apply parameter replacements.
4.  Publish items.

------------------------------------------------------------------------

# 8. Environment Mapping

Example:

    GitHub Repository

           |

           |

    fabric folder


           |

           |

    Fabric Workspace DEV

Promotion:

    DEV

     |

     |

    Test Workspace

     |

     |

    Prod Workspace

------------------------------------------------------------------------

# 9. Hardcoded GUID Replacement

Example Notebook:

``` json
{
 "defaultLakehouse": {

    "id":"DEV-LAKEHOUSE-ID",

    "workspaceId":"DEV-WORKSPACE-ID"

 }
}
```

`parameter.yml`

``` yaml
find_replace:


- find: DEV-WORKSPACE-ID

  replace_with:

    TEST: TEST-WORKSPACE-ID

    PROD: PROD-WORKSPACE-ID



- find: DEV-LAKEHOUSE-ID

  replace_with:

    TEST: TEST-LAKEHOUSE-ID

    PROD: PROD-LAKEHOUSE-ID
```

------------------------------------------------------------------------

# 10. Approval Workflow

GitHub Environments:

    fabric-test

    fabric-prod

Production flow:

    Merge main

         |

    Deploy Test

         |

    Approval Required

         |

    Deploy Production

------------------------------------------------------------------------

# 11. Secret Management

## Option 1 - GitHub Secrets

    Repository Settings

            |

    Secrets

            |

    CLIENT_ID

    CLIENT_SECRET

    TENANT_ID

------------------------------------------------------------------------

# 12. Implementation Plan

## Phase 1 - Foundation

-   Setup GitHub repository.
-   Create `/fabric` folder structure.
-   Enable GitHub Actions.
-   Create Service Principal.
-   Grant Fabric workspace permission.
-   Setup GitHub Secrets.
-   Install fabric-cicd.
-   Create deploy_pipeline.py and deploy_powerbi.py.
-   Create parameter.yml.

Result:

    git push main

          |

    GitHub Action

          |

    Deploy /fabric

          |

    Test Workspace

------------------------------------------------------------------------

## Phase 2 - Promotion Flow

-   Create environments:

    fabric-test

    fabric-prod

-   Configure reviewers.
-   Add production deployment.
-   Validate DEV → TEST → PROD flow.

------------------------------------------------------------------------

## Phase 3 - Hardening

-   Add deployment validation.
-   Add smoke test.
-   Add rollback strategy.
-   Control item deployment scope.

Example:

``` yaml
items_in_scope:

- notebooks

- pipelines

- lakehouses
```

------------------------------------------------------------------------

# 13. Before vs After

  Criteria                 Current           After
  ------------------------ ----------------- ------------------------
  Source control           Manual            GitHub
  Fabric source location   Not standardized  `/fabric` folder
  Deployment               Manual            GitHub Actions
  Approval                 None              GitHub Environment
  GUID replacement         Manual            parameter.yml
  Audit                    None              GitHub Actions history
  Rollback                 Difficult         Git history

------------------------------------------------------------------------

# 14. Technical Note

`fabric-cicd` deploys based on the `/fabric` folder.

Do not edit Test/Prod Fabric workspaces directly.

Standard flow:

    Developer

       |

    GitHub

       |

    /fabric

       |

    GitHub Actions

       |

    Fabric Workspace

Check compatibility of artifacts before promoting to production:

-   Notebook
-   Data Pipeline
-   Lakehouse
-   Semantic Model

