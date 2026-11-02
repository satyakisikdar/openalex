check:
	uv run ruff check .
	uv run ruff format --check .
	uv run ty check
	uv run pytest -q

setup:
	uv sync
	git config core.hooksPath .githooks
	git remote get-url yoda >/dev/null 2>&1 || git remote add yoda yoda:/home/ssikdar/PycharmProjects/openalex

# Push committed main to Yoda's checkout. Git refuses if Yoda has uncommitted edits (see .claude/docs/data.md).
sync:
	git push yoda main

# Show uncommitted edits on Yoda.
yoda-diff:
	ssh yoda 'cd /home/ssikdar/PycharmProjects/openalex && git status --short && git diff'

.PHONY: check setup sync yoda-diff
