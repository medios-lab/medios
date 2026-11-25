---
name: medios-file-issue
description: Help the user report a Medios preview failure on GitHub with the diagnostics the team needs to reproduce it. Use when a step fails and no troubleshooting entry fixes it.
---

# Filing an issue

The user files the issue from their own GitHub account; you prepare it. Don't
post anything without the user's yes.

1. Run `bin/medios-try diagnose`. It writes one archive under `state/` with the
   machine facts, the bundle revision, `release.json`, the controller and node
   logs and the Medios build record. It holds no credentials, but the logs do
   contain this machine's hostname and paths; tell the user so before they
   attach it.
2. Draft the issue for https://github.com/medios-lab/medios/issues with:
   - title: `[preview] <step that failed>: <error in a few words>`
   - the coding agent and model you are
   - the bundle revision (`bundle_revision` in the diagnose JSON)
   - the machine facts from the diagnose JSON
   - the failing command and its full JSON output
   - what you already tried from `skills/troubleshoot/SKILL.md`
3. Show the draft to the user. If they have `gh` installed and signed in, offer
   `gh issue create -R medios-lab/medios --title ... --body-file ...` and run it
   only when they say yes; otherwise give them the text to paste. GitHub's web
   form takes the archive as an attachment; `gh` doesn't, so mention that.
