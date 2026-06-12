# 🔁 Rollback `main` Branch to a Commit SHA (GitHub Actions)

This workflow allows **manual rollback of the `main` branch** to a **known good commit SHA or tag** in production.

It includes **safety checks and a backup tag** to prevent mistakes.

---

## How it works

When triggered manually:

1. Checks out `origin/main` with full history using `actions/checkout@v4`
2. Validates the target commit SHA or tag exists
3. Ensures the commit is part of `main` history — either an ancestor (rollback) or descendant (restore)
4. Creates a **timestamped backup tag** of the current `main` state
5. Configures Git identity for commits
6. Resets `main` to the target commit and **force-pushes** to GitHub

Effectively, it runs:
```bash
git rev-parse <commit-sha>                               # validate exists

# either passes:
git merge-base --is-ancestor <commit-sha> origin/main   # rollback (going back)
git merge-base --is-ancestor origin/main <commit-sha>   # restore  (going forward)

git tag backup-before-rollback-<timestamp>
git push origin --tags
git reset --hard <commit-sha>
git push origin main --force
```

The backup tag allows recovery if the rollback was incorrect — simply re-run the workflow with the backup tag as the `target_ref`.

---

## How to use this workflow

1. Go to **GitHub → Actions**
2. Select **Rollback main To commit SHA**
3. Click **Run workflow**
4. Enter the **commit SHA or tag** of the target state (e.g. `abcde123` or `backup-before-rollback-1775738874`)
5. Run the workflow
6. Verify `main` is correct in GitHub and locally

---

## ✅ Do's

- Use commit SHAs or tags from `main` history only
- Roll back only to stable, previously deployed commits
- Keep the backup tag until rollback is confirmed
- Notify the team before and after rollback
- After rollback, sync your local repo:
  ```bash
  git fetch origin
  git reset --hard origin/main
  ```
- Restrict workflow access to senior engineers / SREs

---

## ❌ Don'ts

- Don't rollback to commits from other branches
- Don't run this during an active deployment
- Don't use for normal development fixes
- Don't re-push the broken commit after rollback
- Don't allow unrestricted access to this workflow

---

## ⚠️ Important Notes

- This workflow rewrites Git history with a **force push**
- The backup tag serves as an **undo point** — re-run the workflow with the backup tag to restore a previous state
- Tags can be left in the repo for audit or deleted once rollback is verified

---

## Optional Cleanup

After confirming the rollback is correct, you may delete the backup tag to keep the repo tidy:

```bash
git push origin :backup-before-rollback-<timestamp>  # delete remote tag
git tag -d backup-before-rollback-<timestamp>        # delete local tag
```
