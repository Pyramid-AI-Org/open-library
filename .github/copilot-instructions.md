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
  - `python main.py --crawler devb.devb_press_releases --settings config/settings.yaml --out ./local-data`
  - `python main.py --crawler hksar_press_releases --settings config/settings.yaml --out ./local-data` (short name also works)
  - note: `--debug` is a flag (do not pass `true/false` values)
- Crawler contract: `crawlers/base.py`
  - Each module exports `class Crawler` with `name` and `crawl(ctx: RunContext) -> list[UrlRecord]`.
  - `RunContext` provides:
    - `ctx.get_crawler_config(crawler_name)` - returns merged source-level + page-level config
    - `ctx.get_http_config()` - returns HTTP settings (timeout, user_agent, etc.)
    - `ctx.make_record(url, name, discovered_at_utc, source, meta)` - creates UrlRecord with source_id/source_label auto-populated
  - Shared crawler helpers live in `crawlers/base.py` and should be reused where possible:
    - `clean_text()`, `sleep_seconds()`, `compute_backoff_seconds()`
    - `get_with_retries()`
    - `canonicalize_url()`, `path_ext()`, `infer_name_from_link()`
- Output schema written by `main.py`:
  - `url` (string), `name` (string|null), `discovered_at_utc` (ISO-8601 string)
  - `source` (crawler name, e.g., "devb_press_releases")
  - `source_id` (folder name, e.g., "devb")
  - `source_label` (human-readable, e.g., "The Development Bureau")
  - `meta` (object)
- Determinism matters (data branch diffs): records are sorted before writing and JSON is emitted with sorted keys (`utils/jsonio.py`).

## Configuration structure

Settings are organized by **source** (folder name) with nested **pages** (crawlers):

```yaml
crawlers:
  devb:                                    # source_id (folder name)
    label: "The Development Bureau"        # human-readable label
    base_url: "https://www.devb.gov.hk"    # shared across pages
    request_delay_seconds: 0.5             # shared default
    pages:
      devb_press_releases:                 # crawler name
        years_back: 10                     # page-specific override
      devb_speeches_and_presentations:
        years_back: 10
```

- Source-level settings (e.g., `base_url`, `request_delay_seconds`) are inherited by all pages.
- Page-level settings override source-level defaults.
- Use `ctx.get_crawler_config(self.name)` to get the merged config.

## Project-specific conventions

- **Adding a new crawler**: Just add an entry under `crawlers.<source>.pages.<crawler_name>` in `config/settings.yaml`. No changes to `main.py` needed - crawlers are discovered automatically from settings.
- **Adding a new source**: Create a folder under `crawlers/`, add the source to `config/settings.yaml` with a `label`, and update `docs/viewer-config.json` sourceGroups.
- HTTP is done with `requests` (see `crawlers/devb/devb_press_releases.py`, `crawlers/directory/tel_directory.py`):
  - reuse a `requests.Session` when crawling many pages
  - use `ctx.get_http_config()` for timeout/user_agent settings
  - prefer shared `get_with_retries()` for 429/5xx + backoff behavior
- HTML parsing is mostly stdlib (`html.parser.HTMLParser`) and the helper in `utils/html_links.py` (see `crawlers/link_extract.py`).

### URL canonicalization and crawler-specific wrappers

- Use shared `canonicalize_url()` defaults unless a crawler requires special behavior.
- Common wrappers in crawlers are acceptable for preserving existing behavior, e.g.:
  - `encode_spaces=True` where sources contain literal spaces in hrefs.
  - response charset post-processing hooks (DevB press releases).
  - host-restricted canonicalization in domain-specific crawlers (e.g., telephone directory).

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
  - `python main.py --crawler devb.devb_press_releases --settings config/settings.yaml --out ./local-data`
  - `python -m scripts.data_stats --in local-data/latest/urls.jsonl --out-json local-data/latest/stats.json > local-data/latest/info.txt`
  - `python -m scripts.build_archive_index --data-root local-data`

## When changing the viewer

- The viewer loads `docs/viewer-config.json` at runtime (`docs/app.js: loadViewerConfig()`). If you add/rename `meta` fields in crawlers, consider updating viewer-config so details dialogs show the new fields.
- Archives are normally read from `archive/index.json`, but the viewer can fall back to enumerating the `data` branch git tree via the GitHub API (`docs/app.js: tryBuildArchiveIndexFromGitTree()`).
- Source groups in `viewer-config.json` should match the source IDs in `settings.yaml`.
