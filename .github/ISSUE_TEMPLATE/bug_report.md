---
name: Bug report
about: Report a defect in CSFS
title: ""
labels: bug
assignees: ""
---

**Describe the bug**

A clear and concise description of what went wrong.

**To reproduce**

```bash
# Exact command(s) or minimal Python snippet
csfs fetch -p usgs --lookback 24
```

**Expected behavior**

What you expected to happen.

**Actual behavior**

What actually happened. Include the full traceback or error output if there is one.

**Environment**

- CSFS version (`pip show csfs` or `csfs --version`):
- Python version:
- OS:
- Install method (PyPI / source):

**Provider-related?**

If the bug involves a specific connector, name the provider slug (e.g. `usgs`,
`france_hubeau`). Note that live-provider commands can fail because the
*upstream* agency API is down — please re-run once before filing, and say
whether the failure is reproducible.

**Additional context**

Anything else that helps (config file excerpt with secrets removed, database
size, scheduler tier, etc.).
