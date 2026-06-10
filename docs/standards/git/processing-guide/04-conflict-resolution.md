# Conflict Resolution

A merge conflict occurs when Git cannot automatically reconcile differences between two commits (usually when two developers modify the same lines of the same file). Conflicts are a normal part of development—do not panic.

---

## 1. When Conflicts Happen

When you attempt to merge or pull and Git detects overlapping changes, it will halt the merge process:

```bash
git merge dev

# Git output when a conflict exists:
# CONFLICT (content): Merge conflict in fabric/notebooks/transformation/orders_silver.ipynb
# Automatic merge failed; fix conflicts and then commit the result.
```

---

## 2. Resolving a Conflict Step by Step

1. **Identify Conflict Files**:
   Run `git status` to see a list of all files with conflicts (marked as "both modified").
   ```bash
   git status
   ```

2. **Open and Locate Conflict Markers**:
   Open the conflicting files. Git highlights conflicts using markers:
   ```text
   <<<<<<< HEAD
   your local changes
   =======
   incoming changes from the target branch
   >>>>>>> dev
   ```

3. **Resolve the Code**:
   - Keep your local version,
   - Keep the incoming version, or
   - Manually combine both (common for notebooks or pipeline scripts).
   - **Important**: Delete the conflict markers (`<<<<<<<`, `=======`, `>>>>>>>`) from the file before saving.

4. **Stage the File**:
   Stage the resolved file to mark it as resolved in Git.
   ```bash
   git add fabric/notebooks/transformation/orders_silver.ipynb
   ```

5. **Commit and Push**:
   Commit the resolution and push to your remote feature branch.
   ```bash
   git commit -m "merge: resolve conflict in orders_silver transform notebook"
   git push origin feature/your-branch
   ```

---

## 3. Jupyter Notebook Conflict Tips

Jupyter Notebooks (`.ipynb`) are serialized as JSON, making raw text conflicts difficult to read. Follow these best practices:

- **Use Visual Editors**: Open the conflicting notebook in **VS Code with the Jupyter extension**. It renders the conflict cell-by-cell, allowing you to select changes visually.
- **Output Conflicts**: If the conflict occurs only in output cells (rendered charts, cell run counters, printed statistics), accept the incoming version. The notebook cells will be re-run during validation.
- **Code Conflicts**: If the conflict is in a code cell, compare both versions carefully. Combine logic or choose the correct implementation based on requirements.
- **Prevent Output Bloat**: Use `nbstripout` to automatically clear cell outputs before committing notebooks. This prevents the majority of notebook conflicts.
  ```bash
  pip install nbstripout
  nbstripout --install   # Configures a Git filter to strip outputs on commit
  ```

---

## 4. Prevention: The Best Conflict Strategy

- **Keep Up to Date**: Merge the `dev` branch into your active feature branch **at least every 2 days** to keep your branch close to the integration branch.
- **Coordinate Offline**: If multiple team members are working on the same notebooks, coordinate who is working on what modules or cells to prevent concurrent changes to the same lines.
- **Keep Branches Short-Lived**: Open your Pull Request as soon as a task is complete. Long-lived feature branches accumulate conflicts over time.
