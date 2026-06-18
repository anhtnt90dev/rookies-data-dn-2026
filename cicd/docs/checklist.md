# Microsoft Fabric CI/CD Prerequisites Checklist

## 1. Azure / Entra ID Setup

### Service Principal

* [ ] Create Azure App Registration
* [ ] Create Service Principal
* [ ] Generate Client Secret
* [ ] Record Tenant ID
* [ ] Record Client ID
* [ ] Store Client Secret securely

### Required Values

| Parameter     | Status | Value |
| ------------- | ------ | ----- |
| TENANT_ID     | [ ]    |       |
| CLIENT_ID     | [ ]    |       |
| CLIENT_SECRET | [ ]    |       |

---

## 2. Fabric Tenant Configuration

### Fabric Admin Portal

* [ ] Enable **Service Principals can use Fabric APIs**
* [ ] Enable Service Principal access to required workspaces
* [ ] Validate Service Principal authentication

---

## 3. Workspace Inventory

### Development Workspace

* [ ] Workspace Name
* [ ] Workspace ID

### Test Workspace

* [ ] Workspace Name
* [ ] Workspace ID

### Production Workspace

* [ ] Workspace Name
* [ ] Workspace ID

| Environment | Workspace Name | Workspace ID |
| ----------- | -------------- | ------------ |
| DEV         |                |              |
| TEST        |                |              |
| PROD        |                |              |

---

## 4. Lakehouse Inventory

| Environment | Lakehouse Name | Lakehouse ID |
| ----------- | -------------- | ------------ |
| DEV         |                |              |
| TEST        |                |              |
| PROD        |                |              |

Checklist:

* [ ] Collect DEV Lakehouse ID
* [ ] Collect TEST Lakehouse ID
* [ ] Collect PROD Lakehouse ID

---

## 5. Warehouse Inventory

| Environment | Warehouse Name | Warehouse ID |
| ----------- | -------------- | ------------ |
| DEV         |                |              |
| TEST        |                |              |
| PROD        |                |              |

Checklist:

* [ ] Collect DEV Warehouse ID
* [ ] Collect TEST Warehouse ID
* [ ] Collect PROD Warehouse ID

---

## 6. Connection Inventory

Connections are commonly environment-specific and usually require parameterization.

| Environment | Connection Name | Connection ID |
| ----------- | --------------- | ------------- |
| DEV         |                 |               |
| TEST        |                 |               |
| PROD        |                 |               |

Checklist:

* [ ] Azure SQL Connection IDs
* [ ] Dataflow Connection IDs
* [ ] Shortcut Connection IDs
* [ ] External Storage Connection IDs
* [ ] Warehouse Connection IDs

---

## 7. Variable Library Inventory

If Variable Libraries are used:

| Environment | Variable Library Name | Item ID |
| ----------- | --------------------- | ------- |
| DEV         |                       |         |
| TEST        |                       |         |
| PROD        |                       |         |

Checklist:

* [ ] Collect DEV Variable Library ID
* [ ] Collect TEST Variable Library ID
* [ ] Collect PROD Variable Library ID

---

## 8. Environment Item Inventory

If Fabric Environment items are used:

| Environment | Environment Name | Item ID |
| ----------- | ---------------- | ------- |
| DEV         |                  |         |
| TEST        |                  |         |
| PROD        |                  |         |

Checklist:

* [ ] Collect DEV Environment ID
* [ ] Collect TEST Environment ID
* [ ] Collect PROD Environment ID

---

## 9. GitHub Configuration

### Repository

* [ ] Create GitHub Repository
* [ ] Connect Fabric Workspace to Git
* [ ] Standardize `/fabric` folder structure
* [ ] Protect `main` branch
* [ ] Enable Pull Request review policy

### Repository Structure

```text
repository-root/

├── .github/
│   └── workflows/
│
├── fabric/
│
├── scripts/
│
├── parameter.yml
│
└── requirements.txt
```

---

## 10. GitHub Secrets

### Repository Secrets

* [ ] TENANT_ID
* [ ] CLIENT_ID
* [ ] CLIENT_SECRET

### Environment Secrets

#### fabric-test

* [ ] WORKSPACE_ID
* [ ] LAKEHOUSE_ID
* [ ] CONNECTION_ID

#### fabric-prod

* [ ] WORKSPACE_ID
* [ ] LAKEHOUSE_ID
* [ ] CONNECTION_ID

---

## 11. GitHub Environment Approval

### Test Environment

* [ ] Create `fabric-test`
* [ ] Configure reviewers

### Production Environment

* [ ] Create `fabric-prod`
* [ ] Configure required reviewers
* [ ] Configure deployment protection rules

---

## 12. Parameterization

### parameter.yml

* [ ] Workspace IDs mapped
* [ ] Lakehouse IDs mapped
* [ ] Connection IDs mapped
* [ ] Variable Library IDs mapped
* [ ] Environment IDs mapped

Example:

```yaml
find_replace:

- find: DEV_WORKSPACE_ID
  replace_with:
    TEST: TEST_WORKSPACE_ID
    PROD: PROD_WORKSPACE_ID

- find: DEV_LAKEHOUSE_ID
  replace_with:
    TEST: TEST_LAKEHOUSE_ID
    PROD: PROD_LAKEHOUSE_ID
```

---

## 13. Service Principal Permissions

### DEV Workspace

* [ ] Member or Admin

### TEST Workspace

* [ ] Member or Admin

### PROD Workspace

* [ ] Member or Admin

Recommended:

```text
DEV  = Admin
TEST = Admin
PROD = Admin
```

---

## 14. CI/CD Validation Checklist

### Pre-Go-Live

* [ ] GitHub Actions can authenticate to Fabric
* [ ] Deploy from `/fabric` succeeds
* [ ] GUID replacement works correctly
* [ ] Notebook lakehouse attachment validated
* [ ] Connections validated
* [ ] Test deployment successful
* [ ] Production deployment successful
* [ ] Approval workflow validated
* [ ] Rollback procedure documented
* [ ] Runbook completed

---

## Final  Review

### Infrastructure

* [ ] Azure App Registration completed
* [ ] Service Principal configured
* [ ] GitHub repository configured
* [ ] GitHub Actions configured

### Fabric

* [ ] DEV workspace inventoried
* [ ] TEST workspace inventoried
* [ ] PROD workspace inventoried

### Deployment

* [ ] parameter.yml completed
* [ ] CI pipeline validated
* [ ] CD pipeline validated
* [ ] Approval workflow validated


