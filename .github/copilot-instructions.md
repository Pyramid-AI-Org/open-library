# Copilot instructions (Open Library)

## Big picture

- This repo is a set of scheduled Python web crawlers that emit small URL metadata records.
- Code lives on `main`; crawl outputs are published to a separate `data` branch via a git worktree (see .github/workflows/crawl.yml).
- Output layout (relative to the data root):
  - `latest/urls.jsonl` (overwritten each run)
  - `archive/YYYY/MM/DD/urls.jsonl` (previous `latest` is moved here)
- A static viewer in `docs/` (GitHub Pages) reads JSONL from the `data` branch via `raw.githubusercontent.com` and uses `docs/viewer-config.json` to decide which `meta.*` fields to show.

## Key entrypoints & data contracts

- CLI entrypoint: `main.py`
  - `python main.py --settings config/settings.yaml --out ./local-data`
  - `python main.py --crawler devb_press_releases --settings config/settings.yaml --out ./local-data`
- Crawler contract: `crawlers/base.py`
  - Each module exports `class Crawler` with `name` and `crawl(ctx: RunContext) -> list[UrlRecord]`.
  - `RunContext.settings` is the parsed YAML from `config/settings.yaml` (via `utils/settings.py`).
- Output schema written by `main.py`:
  - `url` (string), `name` (string|null), `discovered_at_utc` (ISO-8601 string), `source` (crawler name), `meta` (object)
- Determinism matters (data branch diffs): records are sorted before writing and JSON is emitted with sorted keys (`utils/jsonio.py`).

## Project-specific conventions

- Adding a crawler is intentionally allowlisted: update the explicit `crawler_names = [...]` list in `main.py`.
- Crawler config goes under `crawlers.<name>` in `config/settings.yaml` and typically includes:
  - request pacing (`request_delay_seconds`, `request_jitter_seconds`)
  - retry/backoff knobs (`max_total_records`, `backoff_*`, etc.)
- HTTP is done with `requests` (see `crawlers/devb_press_releases.py`, `crawlers/tel_directory.py`):
  - reuse a `requests.Session` when crawling many pages
  - honor `http.timeout_seconds` and `http.user_agent` from settings
  - handle 429/5xx with retries + exponential backoff
- HTML parsing is mostly stdlib (`html.parser.HTMLParser`) and the helper in `utils/html_links.py` (see `crawlers/link_extract.py`).

## Data publishing workflow (CI)

- Daily crawl: `.github/workflows/crawl.yml`
  - uses Python 3.11 (via `actions/setup-python`)
  - checks out code, creates/updates a `data` branch worktree at `./data-worktree`
  - archives previous latest via `utils/data_rotation.archive_previous_latest`
  - runs `main.py` with `--out data-worktree/data`
  - generates viewer artifacts:
    - stats: `python -m scripts.data_stats --in data-worktree/data/latest/urls.jsonl --out-json data-worktree/data/latest/stats.json > .../info.txt || true`
    - archive index: `python -m scripts.build_archive_index --data-root data-worktree/data`
- Pages deploy: `.github/workflows/pages.yml` publishes `docs/` as a static site.

## Useful local commands

- Setup:
  - `python3 -m venv .venv && source .venv/bin/activate && pip install -r crawlers/requirements.txt`
- Run + inspect outputs:
  - `python main.py --settings config/settings.yaml --out ./local-data`
  - `python -m scripts.data_stats --in local-data/latest/urls.jsonl --out-json local-data/latest/stats.json > local-data/latest/info.txt`
  - `python -m scripts.build_archive_index --data-root local-data`

## When changing the viewer

- The viewer loads `docs/viewer-config.json` at runtime (`docs/app.js: loadViewerConfig()`). If you add/rename `meta` fields in crawlers, consider updating viewer-config so details dialogs show the new fields.
- Archives are normally read from `archive/index.json`, but the viewer can fall back to enumerating the `data` branch git tree via the GitHub API (`docs/app.js: tryBuildArchiveIndexFromGitTree()`).
