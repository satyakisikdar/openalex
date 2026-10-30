---
name: data-scripts-on-yoda
enabled: true
event: bash
action: block
conditions:
  - field: command
    operator: regex_match
    pattern: (python[0-9.]*\s+(-m\s+preprocessing\.|preprocessing/)|uv\s+run\s+(python\s+)?(-m\s+)?preprocessing[./])(optimize_parquet|flatten_openalex_files|filter_dataset)
  - field: command
    operator: not_contains
    pattern: ssh
---
Data scripts run only on Yoda (CLAUDE.md): nothing data-related runs on the Mac. `make sync`, then run it with
`ssh yoda 'cd /home/ssikdar/PycharmProjects/openalex && nohup uv run --no-sync python -m preprocessing.<script> ...'`.
