# 12. Conflict Resolution
A conflict happens when two people changed the same part of the same file, and Git can't automatically decide which version to keep. Conflicts are normal — don't panic.

### 12.1 When Conflicts Happen

```bash
git merge develop

# Git output when a conflict exists:
# CONFLICT (content): Merge conflict in fabric/notebooks/transformation/orders_silver.ipynb
# Automatic merge failed; fix conflicts and then commit the result.
```

### 12.2 Resolving a Conflict Step by Step

```bash
# 1. See all conflicting files
git status

# 2. Open the conflicting file. Git marks conflicts like this:
  ← Your version of this code

# 3. Edit the file:
#    - Keep your version, or
#    - Keep the incoming version, or
#    - Combine both (most common for data transforms)
#    Then DELETE the conflict markers (<<<, ===, >>>)

# 4. Stage the resolved file
git add fabric/notebooks/transformation/orders_silver.ipynb

# 5. Commit the resolution
git commit -m "merge: resolve conflict in orders_silver transform notebook"

# 6. Push
git push origin feature/your-branch
```

### 12.3 Notebook Conflict Tips

Notebooks (`.ipynb`) are JSON files, which makes conflicts look messy. Practical tips:

- Open the file in **VS Code with the Jupyter extension** — it renders the cells more readably than a raw text editor
- If the conflict is only in **output cells** (printed data, charts), simply accept the incoming version — you'll re-run the notebook anyway
- If the conflict is in **code cells**, compare both versions carefully and keep the logically correct one — or combine both if each has useful changes
- Use `nbstripout` to auto-clear outputs before every commit — this is the single most effective way to avoid notebook conflicts

```bash
pip install nbstripout
nbstripout --install   # One-time setup — runs automatically on every commit
```

### 12.4 Prevention: The Best Conflict Strategy

- Pull `develop` into your branch **at least every 2 days** during active work
- If two team members are working on the same notebook, coordinate offline — split the work into separate functions or cells, or take turns
- Keep feature branches short-lived — open your PR within a few days of creating the branch; long-lived branches accumulate conflicts

---
