#  Pull Request (PR) Process
All changes to `develop` and `main` must go through a Pull Request. **No direct pushes to protected branches — ever.**

###  Pull Request (PR) title

**Pattern**

`type(scope): description`

- **type**: One of `feat`, `fix`, `docs`, `refactor`, `chore`, `test`
- **scope** (optional): The module/component being affected
- **description**: Short, imperative, present-tense summary

Example: `feat(ingestion): add incremental load for claims`

###  PR Size Guideline

| PR Size | Guideline |
|---|---|
|  Small (ideal) | One feature, one fix, or one schema change. Easy to review in 15–30 mins. |
| Medium | Multiple related changes (e.g. notebook + pipeline for same feature). Acceptable. |
| Large | Many unrelated changes bundled together. Split into smaller PRs. |

###  PR Description Template

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

### 11.3 PR Reviewer Checklist

Use this when reviewing a teammate's PR:

- [ ] Logic is correct and makes sense for the data layer it targets
- [ ] No hardcoded credentials, storage paths, or environment-specific values
- [ ] Notebook outputs are cleared
- [ ] Commit messages follow the standard
- [ ] PR is scoped to what it claims — no surprise extra changes
- [ ] Schema changes are documented and non-breaking (or flagged if breaking)
- [ ] Parameters are used instead of hardcoded values where appropriate
- [ ] Data quality / validation logic is included or not needed for this change

### 11.4 Approval Requirements

| Target Branch | Required Approvals | Notes |
|---|---|---|
| `develop` | 1 reviewer | Any team member |
| `main` | 2 reviewers | Must include team lead or senior engineer |

### 11.5 Merge Method

| Branch Transition | Merge Method | Reason |
|---|---|---|
| `feature/*` → `develop` | **Squash merge** | Keeps develop history clean |
| `release/*` → `main` | **Merge commit** | Preserves the release point in history |
| `hotfix/*` → `main` | **Merge commit** | Preserves the fix in history |

After any merge to `develop` or `main`: **delete the source branch**.

---
