## Bug to fix

**Issue #{{issue_number}} — {{issue_title}}**

{{issue_body}}

---

Fix this in the C# driver per the system prompt: first add a test in
`csharp/test/` that reproduces the bug (it must fail against the current code),
then fix the code in `csharp/src/` until that test (and the surrounding suite)
passes. Do not weaken the test to force it green. Read the existing
`csharp/test/` conventions before writing.
