# Fabric CI/CD Solution with GitHub Actions

## 1. Context

Source code được quản lý trên GitHub. Trong repository, toàn bộ Fabric
artifacts được tổ chức trong folder `/fabric`

------------------------------------------------------------------------

# 2. Repository Structure

Cấu trúc source code đề xuất:

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
    │   └── deploy.py
    │
    ├── parameter.yml
    │
    └── requirements.txt

Folder:

    /fabric

là folder chứa toàn bộ Fabric workspace artifacts được quản lý bằng Git.

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

# 3. Giải pháp đề xuất

Xây dựng CI/CD pipeline dựa trên:

-   GitHub Repository:
    -   Source control cho Fabric artifacts.
-   GitHub Actions:
    -   Automation pipeline.
-   fabric-cicd:
    -   Publish Fabric items từ folder `/fabric`.
-   GitHub Environment:
    -   Approval workflow Test/Prod.
-   GitHub Secrets:
    -   Quản lý credential.

------------------------------------------------------------------------

# 4. Kiến trúc tổng quan

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

    /fabric folder

          |
          |
    parameter.yml replacement

          |
          v

    Test Workspace

          |
          |
    Manual Approval

          |
          v

    Prod Workspace

------------------------------------------------------------------------

# 5. Thành phần kiến trúc

  Thành phần           Vai trò
  -------------------- -----------------------------------------------
  `/fabric` folder     Chứa toàn bộ Fabric artifacts
  fabric-cicd          Deploy artifacts từ Git sang Fabric workspace
  parameter.yml        Mapping GUID giữa môi trường
  GitHub Actions       CI/CD workflow
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

Ví dụ:

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

        python scripts/deploy.py
```

------------------------------------------------------------------------

# 7. Deploy Script

`scripts/deploy.py`

Luồng:

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

Script sẽ:

1.  Authenticate với Fabric.
2.  Load source từ `/fabric`.
3.  Apply parameter replacement.
4.  Publish items.

------------------------------------------------------------------------

# 8. Environment Mapping

Ví dụ:

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

Ví dụ Notebook:

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

GitHub Environment:

    fabric-test

    fabric-prod

Production:

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
-   Create deploy.py.
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

```{=html}

```
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

# 13. So sánh trước và sau

  Tiêu chí                 Hiện tại          Sau
  ------------------------ ----------------- ------------------------
  Source control           Manual            GitHub
  Fabric source location   Không chuẩn hóa   `/fabric` folder
  Deployment               Manual            GitHub Actions
  Approval                 Không             GitHub Environment
  GUID replacement         Manual            parameter.yml
  Audit                    Không             GitHub Actions history
  Rollback                 Khó               Git history

------------------------------------------------------------------------

# 14. Technical Note

`fabric-cicd` deploy dựa trên folder `/fabric`.

Không nên chỉnh sửa trực tiếp trên Fabric workspace Test/Prod.

Luồng chuẩn:

    Developer

       |

    GitHub

       |

    /fabric

       |

    GitHub Actions

       |

    Fabric Workspace

Cần kiểm tra compatibility của các artifact:

-   Notebook
-   Data Pipeline
-   Lakehouse
-   Semantic Model

trước khi đưa production.
