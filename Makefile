check:
	uv run ruff check .
	uv run ruff format --check .
	uv run ty check
	uv run pytest -q

setup:
	uv sync
	git config core.hooksPath .githooks

# Push code to Yoda (no --delete: Yoda holds outputs we must never remove).
sync:
	rsync -avz --exclude-from=.rsync-exclude ./ yoda:/home/ssikdar/PycharmProjects/openalex/

.PHONY: check setup sync
