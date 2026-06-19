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
| **reviewer-bot** | Pull requests: **Read & write**, Contents: **Read**, Metadata: Read | post inline review findings |

For each App, generate a private key and store the App ID + key as secrets (below).

## 2. Repository secrets (Settings → Secrets and variables → Actions)

| Secret | Value |
|---|---|
| `ENGINEER_BOT_APP_ID` / `ENGINEER_BOT_APP_PRIVATE_KEY` | the engineer-bot App's id + private key |
| `REVIEW_BOT_APP_ID` / `REVIEW_BOT_APP_PRIVATE_KEY` | the reviewer-bot App's id + private key |
| `BOT_ENGINE_PAT` | a **fine-grained PAT** with **Contents: Read-only** scoped to **just `eric-wang-1990/databricks-bot-engine`** (private for now). Set an expiry + rotation. **Drop it entirely once that repo is public** (the install goes anonymous). |
| `MODEL_ENDPOINT` / `MODEL_TOKEN` | your LLM serving endpoint + token. **⚠️ This is a Databricks credential in a public repo** — prefer one of the alternatives below over a stored token. |

**Model credential — prefer not to store it.** Two cleaner options than a repo secret:
- Run the jobs on the protected **`peco-driver`** self-hosted runner and source the
  model creds from the runner environment (change `runs-on:` in both workflows).
- Use **OIDC / workload-identity federation** (both workflows already request
  `id-token: write`) if your endpoint accepts a federated short-lived token.

`BOT_ENGINE_PAT` is interim — once `eric-wang-1990/databricks-bot-engine` is made
public (or published to PyPI), drop it and switch the install lines to anonymous git / PyPI.

## 3. The `bot-fix` label

Create a label named **`bot-fix`**. Applying it to an issue is what triggers the
bug-fix bot — so it must be **applied only by maintainers** (label application
already requires triage/write permission). The issue body is untrusted input; the
label is the authorization.

## 4. Engine ref

Both workflows pin the engine to **`eric-wang-1990/databricks-bot-engine@main`**
(the dedicated engine repo, currently private). Repin to a release tag
once the engine repo cuts one.

## 5. Activate + test

1. Mark this PR **ready for review** and merge it.
2. **Reviewer-bot** then runs on every new PR automatically — confirm it posts findings on a test PR.
3. **Bug-fix bot**: open an issue with a small, reproducible bug (clear symptom + expected vs actual), add the **`bot-fix`** label, and confirm it opens a fix PR that the reviewer-bot then reviews (the closed loop).

## How they compose
- Reviewer-bot reviews every PR — including the bug-fix bot's own fix PRs.
- (Not yet wired) an `engineer-bot-followup` workflow would let the engineer-bot
  *respond* to the reviewer's findings on its fix PRs, fully closing the loop. Add
  it as a follow-on once these two are working.
