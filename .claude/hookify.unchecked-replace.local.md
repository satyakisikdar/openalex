---
name: unchecked-replace
enabled: true
event: bash
action: warn
conditions:
  - field: command
    operator: regex_match
    pattern: \.replace\(
  - field: command
    operator: contains
    pattern: write_text
  - field: command
    operator: not_contains
    pattern: assert
---
Script edit with `str.replace` and no `assert`: if the anchor text is missing, `replace` silently does nothing
(or, with a wrong replacement, deletes the anchor). This lost two `notes.md` sections on 2026-10-29. Assert
`s.count(old) == 1` before each replace, or use the Edit tool.
