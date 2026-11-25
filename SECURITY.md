# Security policy

## Reporting a vulnerability

Report a vulnerability through GitHub's private vulnerability reporting: open the repository's **Security** tab and choose **Report a vulnerability**. The report stays visible only to the maintainers until a fix is published.

Don't open a public issue, pull request or discussion for a security problem.

Include the Medios version or commit, the component (controller, node, executor, containerizer, an SDK), how to reproduce the problem, and what an attacker gains.

## Supported versions

Fixes go into the latest release. Older releases don't receive security fixes.

## Scope

Medios has no authentication or authorization enabled by default, and its controller and node HTTP APIs assume a trusted network. Running them on an untrusted network without enabling authentication is a deployment choice, not a vulnerability. A way to bypass authentication or authorization when it is enabled is in scope.
