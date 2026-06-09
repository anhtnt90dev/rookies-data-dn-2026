# Pull Request (PR) Process

All changes to `dev` and `main` must go through a Pull Request. **No direct pushes to protected branches are permitted.**

---

## 1. Pull Request (PR) Title Pattern

PR titles must follow the conventional commit format:

```text
type(scope): description
```

- **`type`**: One of `feat`, `fix`, `docs`, `refactor`, `chore`, `test`.
- **`scope`** *(Optional)*: The module or data layer affected (e.g. `bronze`, `silver`, `gold`, `dwh`).
- **`description`**: A short, imperative, present-tense summary.

*Example*: `feat(ingestion): add incremental load for claims`

---

## 2. PR Size Guidelines

Keep PRs focused to ensure quick turnarounds and high-quality reviews.

| PR Size | Guideline | Review Time |
| :--- | :--- | :--- |
| **Small (Ideal)** | Focuses on one specific feature, fix, or schema change. | 15–30 mins |
| **Medium** | Covers multiple related changes (e.g., notebook and pipeline for same feature). | 30–60 mins |
| **Large** | Covers many unrelated changes. **Should be split into smaller, independent PRs.** | > 60 mins (Avoid) |

---

## 3. PR Description Template

Copy this template when opening a new PR:

```markdown
## What Does This PR Do?
[Brief plain-English summary]

## Changes Made
- [ ] Notebook: [describe change]
- [ ] Pipeline: [describe change]
- [ ] Lakehouse / Warehouse schema: [describe change]
- [ ] Config / Parameters: [describe change]

## Data Layer Affected
- [ ] Bronze (raw ingestion)
- [ ] Silver (transformation / cleansing)
- [ ] Gold (aggregation / serving)
- [ ] Warehouse

## Testing Done in Dev Workspace
- [ ] Notebook runs end-to-end without errors
- [ ] Pipeline triggered and completed successfully
- [ ] Output row counts verified (expected: ___, actual: ___)
- [ ] No nulls in required columns
- [ ] Checked for unexpected duplicates

## Screenshots / Proof of Run
[Attach screenshot of pipeline run result or notebook output summary]

## Breaking Changes?
- [ ] No breaking changes
- [ ] Yes — describe impact: [...]

## Related Ticket
Closes #[ticket number]
```

---

## 4. PR Reviewer Checklist

Use this checklist when reviewing a teammate's PR:

- [ ] **Logic Correctness**: The code or pipeline logic is correct for the target data layer.
- [ ] **Security**: No hardcoded credentials, storage paths, or environment-specific values.
- [ ] **Cleanliness**: Notebook outputs are cleared before commit.
- [ ] **Standards**: Commit messages and branch names follow the repository convention.
- [ ] **Scope**: The PR is strictly scoped to its description (no unexpected extra changes).
- [ ] **Compatibility**: Schema changes are documented and backward-compatible (or flagged if breaking).
- [ ] **Maintainability**: Parameters are used instead of hardcoded values.
- [ ] **Data Quality**: Data quality or validation checks are included where appropriate.

---

## 5. Approval Requirements

All Pull Requests (PRs) must undergo a structured review process before they can be merged:

1. **Stage 1: Peer Review**
   - The PR must first be reviewed and approved by at least **one peer** (any qualified team member).
   - The peer reviewer focuses on logic correctness, standards compliance, and testing validity using the [PR Reviewer Checklist](#4-pr-reviewer-checklist).
2. **Stage 2: Team Lead Review**
   - After receiving peer approval, the PR must be reviewed and approved by the **Team Lead** (or a Senior Engineer).
   - The Team Lead performs final technical verification, quality control, and architecture compliance checks.
3. **Stage 3: PO (Product Owner) Review (For `main` Only)**
   - For PRs targeting the production branch (`main`), final sign-off is required from the **Product Owner** (PO) to confirm business features, stakeholder alignment, and release readiness.

### Approval Requirements Matrix

| Target Branch | Required Approvals | Required Reviewers | Flow |
| :--- | :--- | :--- | :--- |
| `dev` | 2 Approvals | 1 Peer + 1 Team Lead | Peer review first, followed by Team Lead sign-off. |
| `main` | 3 Approvals | 1 Peer + 1 Team Lead + 1 PO | Peer review, then Team Lead review, and finally PO sign-off. |

---

## 6. Merge Methods and Clean Up

| Branch Transition | Merge Method | Reason |
| :--- | :--- | :--- |
| `feature/*` → `dev` | **Squash merge** | Combines commits to keep the development history clean. |
| `release/*` → `main` | **Merge commit** | Preserves the release point in history. |
| `hotfix/*` → `main` | **Merge commit** | Preserves the fix and its hotfix tag in history. |

> [!IMPORTANT]
> **Post-Merge Clean Up**: Immediately after a PR is successfully merged to `dev` or `main`, **delete the source branch** to keep the remote repository clean.
