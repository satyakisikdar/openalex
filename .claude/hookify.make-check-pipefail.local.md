---
name: make-check-pipefail
enabled: true
event: bash
action: warn
conditions:
  - field: command
    operator: regex_match
    pattern: make\s+check([^;&|\n]|>&)*\|(?!\|)
  - field: command
    operator: not_contains
    pattern: pipefail
---
`make check` is piped without `set -o pipefail`: the pipeline's exit status is the last command's (`tail`), so a
failing check looks like success and `&&` keeps going. This happened on 2026-10-29 and synced unverified code to
Yoda. Add `set -o pipefail;` first, or don't pipe it.
