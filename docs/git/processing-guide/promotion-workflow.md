# Promotion Flow

```
[Feature Development]
feature/xxx ──PR──► develop
                       │
              [Dev Workspace tests pass]
                       │
                       ▼
              release/v1.2.0 ──► UAT Workspace connected to this branch
                       │
              [UAT sign-off from stakeholders]
                       │
                       ▼
              PR: release/v1.2.0 ──► main
                                        │
                                   [Prod Workspace]
                                   + Git tag v1.2.0
```
