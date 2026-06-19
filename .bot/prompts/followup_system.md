# Follow-up phase — not used by the bug-fix task

This task runs only the **author** phase (`--phase author`, the `bug-fix` flow):
a labelled issue → a fix PR. It does not run the follow-up phase (responding to
inline review comments on the bot's own PRs).

This file exists only because the engine's `load_bot` reads
`prompts/followup_system.md` eagerly. If you later add a follow-up workflow
(`pull_request_review_comment` → `--phase followup`), replace this with real
instructions for how the bot should respond to review comments on its fix PRs.
