# Engineer-bot + reviewer-bot — operator setup

How to activate the two bots added by this PR. Until these are done they stay
inert (the workflows are present but every run no-ops / the PR is draft).

The bots run the generic **`databricks-bot-engine`** (hosted in
`eric-wang-1990/databricks-bot-engine`, **private for now**). Both workflows are
**self-contained**: they `pip install` the engine and run it inline.

## 1. Two GitHub Apps (the bot identities)

Create two GitHub Apps in the org and **install both on `adbc-drivers/databricks`**.
They are only used to mint short-lived tokens (`actions/create-github-app-token`)
for API calls — no webhook subscriptions are needed.

| App | Repository permissions | Used for |
|---|---|---|
| **engineer-bot** | Contents: **Read & write**, Pull requests: **Read & write**, Issues: **Read & write**, Metadata: Read | push the fix branch, open the fix PR, comment the outcome on the issue |
| **reviewer-bot** | Pull requests: **Read & write**, Contents: **Read & write**, Metadata: Read | post inline review findings + thread replies, and **resolve threads** — the `resolveReviewThread` mutation is gated behind **Contents:write**, NOT Pull-requests:write (GitHub gotcha; only needed because the reviewer *follow-up* resolves threads) |

For each App, generate a private key and store the App ID + key as secrets (below).

## 2. Repository secrets (Settings → Secrets and variables → Actions)

| Secret | Value |
|---|---|
| `ENGINEER_BOT_APP_ID` / `ENGINEER_BOT_APP_PRIVATE_KEY` | the engineer-bot App's id + private key |
| `REVIEW_BOT_APP_ID` / `REVIEW_BOT_APP_PRIVATE_KEY` | the reviewer-bot App's id + private key |
| `BOT_ENGINE_PAT` | a **fine-grained PAT** with **Contents: Read-only** scoped to **just `eric-wang-1990/databricks-bot-engine`** (private for now). Set an expiry + rotation. **Drop it entirely once that repo is public** (the install goes anonymous). |
| `DATABRICKS_HOST` / `DATABRICKS_TOKEN` | **already present** in adbc CI (the e2e/benchmark workflows use them). The bots reuse them — **no new model secrets needed**: the endpoint is built as `https://$DATABRICKS_HOST/serving-endpoints/databricks-claude-opus-4-8/invocations` and `DATABRICKS_TOKEN` authenticates it (Bearer). |

**Model credential.** The bots reuse the `DATABRICKS_HOST` + `DATABRICKS_TOKEN` adbc
already stores for CI — no new model secret is introduced, and the model serving
endpoint lives on the same workspace. If you'd rather the token not be reachable from
workflow runs at all, run the jobs on the protected **`peco-driver`** self-hosted
runner (source creds from the runner env; change `runs-on:`) or use **OIDC** (the
workflows already request `id-token: write`).

`BOT_ENGINE_PAT` is interim — once `eric-wang-1990/databricks-bot-engine` is made
public (or published to PyPI), drop it and switch the install lines to anonymous git / PyPI.

## 3. Two labels (`engineer-bot` and `review-bot`)

Create **both** labels and keep them **maintainer-only** (applying a label already
requires triage/write). Nothing the bots do happens on an unlabeled PR.

**`engineer-bot`** — invites the *engineer*:
- On an **issue** → triggers the bug-fix bot (`engineer-bot.yaml` opens a fix PR).
  The issue body is untrusted input; the label is the authorization.
- On a **PR** → opts it into the engineer **follow-up** (respond to review comments,
  push fix commits). The bug-fix bot's own fix PRs get it automatically (publish
  applies it); a maintainer applies it to a human PR to invite the bot in.

**`review-bot`** — invites the *reviewer*:
- On a **PR** → reviewer-bot reviews it (and its follow-up resolves threads). Without
  it the reviewer stays silent, so the bot doesn't comment on every PR in the repo.

**How they interact:** the reviewer runs on a PR labeled **`review-bot` _or_
`engineer-bot`**. The OR is deliberate — the bug-fix bot's fix PRs only carry
`engineer-bot`, so this keeps them auto-reviewed and the loop closed without anyone
hand-labeling each fix PR. So: `engineer-bot` → both bots engage; `review-bot` →
reviewer only. No cross-fire: issue-labels fire `issues` events (only the fixer
listens), PR-labels fire `pull_request` events, and publish's auto-label is a
Bot-sender event the engineer follow-up gate skips — so no fix→PR→fix loop.

## 4. Engine ref

Both workflows pin the engine to **`eric-wang-1990/databricks-bot-engine@main`**
(the dedicated engine repo, currently private). Repin to a release tag
once the engine repo cuts one.

## 5. Activate + test

1. Mark this PR **ready for review** and merge it.
2. **Reviewer-bot**: open a test PR, add the **`review-bot`** label, and confirm it posts findings.
3. **Bug-fix bot**: open an issue with a small, reproducible bug (clear symptom + expected vs actual), add the **`engineer-bot`** label, and confirm it opens a fix PR.
4. **The loop**: confirm the fix PR carries the **`engineer-bot`** label, that reviewer-bot reviews it, that engineer-bot-followup addresses the findings (a fix commit + thread replies), and that reviewer-bot-followup then re-checks and resolves the threads.

## How they compose — the four workflows

| Workflow | Trigger | Identity | What it does |
|---|---|---|---|
| `engineer-bot.yaml` | issue labeled `engineer-bot` | engineer-bot | opens a fix PR for the bug |
| `reviewer-bot.yml` | `review-bot`- or `engineer-bot`-labeled PR (opened/synchronize/labeled/…) | reviewer-bot | posts inline review findings — incl. on the bug-fix bot's own PRs |
| `engineer-bot-followup.yml` | review comment on / `engineer-bot`-labeled PR | engineer-bot | pushes fix commits + replies, addressing the findings |
| `reviewer-bot-followup.yml` | reply on a reviewer thread / synchronize, on a `review-bot`/`engineer-bot` PR | reviewer-bot | verifies the fix against the diff, then resolves or pushes back |

Together: **issue → fix PR → review → engineer addresses → reviewer verifies/resolves** —
the fully closed reviewer ⇄ engineer loop. Loop-prevention is marker-based
(`<!-- engineer-bot-csharp-bugfix:v1 …>` / `<!-- pr-review-bot:v1 …>`), so the bots
never react to their own comments.
