<!--
  Copyright (c) 2025 ADBC Drivers Contributors

  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at

          http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
-->

# Orchestration Spec — E2E Agent

The E2E Agent is the final task of every sprint whose spec directory contains
an `e2e-test-spec.md`. It runs after all implementation tasks are complete.

It reads only the design doc and the e2e spec — it has no knowledge of
individual task specs or implementation details.

---

## Inputs

| Input | Purpose |
|---|---|
| Design doc (e.g. `cloudfetch-pipeline-redesign.md`) | What the feature does — used to understand intent when writing tests |
| `e2e-test-spec.md` | Connection parameters, test scenarios, and run instructions |

---

## Protocol

```
After all impl tasks complete:
  │
  └─ spawn E2E Agent
       │
       ├─ Phase 1: Write or locate tests
       │     Read e2e-test-spec.md and design doc
       │     For each scenario in e2e-test-spec.md:
       │       If not covered by an existing test → write it
       │     Run all e2e tests per instructions in e2e-test-spec.md
       │
       ├─ if all tests pass → DONE
       │
       └─ if any test fails → Phase 2: Fix the stack
             │
             ├─ Identify the offending commit
             │     Read git log, match commits to feature descriptions
             │     Use git bisect if the regression is not obvious
             │
             ├─ Checkout the branch containing the bad commit
             │
             ├─ Fix the code (amend the commit)
             │
             ├─ Rebase the entire stack forward
             │     Rebase every downstream branch in dependency order
             │
             ├─ Return to the tip branch
             │
             └─ Re-run all e2e tests → repeat until all pass
```

---

## Exit Criteria

All scenarios in `e2e-test-spec.md` pass. If the agent cannot resolve
failures, it escalates to the human with:
- Which test(s) failed
- Which commit was identified as the source
- What fix was attempted and why it did not resolve the failure

---

## Prompt Template

```
You are the E2E Agent. All implementation tasks are complete.
Your job is to validate the feature end-to-end and fix any regressions.

## Inputs
- Design doc: <path>
- E2E spec: <path to e2e-test-spec.md>

## Phase 1 — Write or locate tests
Read the e2e spec. For each scenario:
1. Check if an existing test covers it.
2. If not, write the test using the connection parameters in the e2e spec.
3. Run all e2e tests using the run instructions in the e2e spec.

## Phase 2 — Fix regressions (if any tests fail)
1. Run `git log --oneline` to see the commit stack.
2. Match the failing behavior to the most likely responsible commit.
   Use git bisect if it is not obvious.
3. Checkout that branch/commit.
4. Fix the code and amend the commit.
5. Rebase all downstream branches in order.
6. Return to the tip branch and re-run all e2e tests.
7. Repeat until all tests pass.

## Exit criteria
Every scenario in the e2e spec passes.
Report DONE when complete, or ESCALATE with a summary if you cannot resolve.
```

---

## File Locations

| File | Purpose |
|---|---|
| `rust/spec/orchestration_spec.md` | This file |
| `rust/spec/e2e-test-spec.md` | Connection parameters, scenarios, and run instructions |
| `rust/spec/sprint-plan-*.md` | Source for task generation; triggers E2E task when e2e-test-spec.md exists |
