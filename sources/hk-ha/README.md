<!-- focus: PAI-1453 Housing Authority scope and rollout -->
<!-- use-when: maintaining the recurring HA Resources source -->

# Housing Authority Resources

`scope.yaml` records user-approved Option B: 27 PDF/Word documents from the
2026-10-23 survey, including four already indexed. The 24 sanitized test pages
cover the hub and 23 section pages. Initial acceptance URLs are test evidence,
not a fixed production download list. New links/sections are discovered within
the configured host, path, depth and crawl budgets. Failed/incomplete crawls
raise, keeping the previous source snapshot and failing the workflow's HA check.

## Editions and boundaries

BIM versions 2–4 are marked previous versions by the publisher. The 2022
Specification Library files are excluded by initial-scope policy in favour of
2026 for new contracts; this does not retire older project requirements.
Seminar PDFs remain included. Only HA-hosted PDF/DOC/DOCX files on configured
asset paths are eligible, including the measured asbestos-path exception.
HTML, Chinese mirrors, external publishers and CAD/media/model archives are out.

New editions and removed links change the feed but do not retire indexed chunks.
Edition linkage/retirement uses the existing pyramid-os maintenance process.
HA department/category filters remain separate work; unfiltered retrieval works.

## Rollout and stop

1. Merge this crawler PR first; HA is not yet in the ingestion allowlist.
2. Run `python main.py --crawler ha --out ./local-data` and verify summary,
   all 27 unique URLs and the four initial acceptance URLs in `scope.yaml`.
3. Deploy the paired pyramid-chat PR with HA dispatch held by default.
4. Run a production dry run targeting HA; verify the exact new-file manifest
   and page/credit estimate before releasing the hold. This PR authorizes no
   paid parsing, forced production refresh, merge or automatic retirement.
5. Same-URL updates reuse the existing rotating HEAD checks and SHA-256 gate.
   Unchanged bytes skip parsing; changed bytes can incur Agentic credits.
   Matching stale publisher validators may conceal updates indefinitely.

To stop ingestion use the paired dispatch hold or remove HA from its allowlist.
Disabling `schedule.enabled` stops crawling only: prior feed records are carried
forward. Disabling dispatch does not restore bytes/chunks already replaced;
metadata revert is not content recovery.

## Files

- `scope.yaml`: approved scope, measured counts, exclusions and acceptance URLs.
- `plan.md`: reviewed cross-repository plan and review log.
