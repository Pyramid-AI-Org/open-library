<!-- focus: PAI-1453 recurring Housing Authority Resources crawler, ingestion hold and rollout plan -->
<!-- use-when: reviewing, building or enabling the HA recurring crawler; deciding HA edition, scope or hold rules -->

# PAI-1453 — Housing Authority recurring Resources crawler

**Status:** Revision 4 (planner), 2026-10-23. Two formal Codex plan-review rounds completed; findings fixed; root checked. **Option B approved 2026-10-23; implementation in progress. Not deployed.**

Implementation survey correction: the executable hub walk finds 27 eligible documents (23 new), including direct hub links PSP Handbook and TPI omitted by the section-only survey. Earlier proposal counts of 25/21 below are historical; `scope.yaml` and the executable fixture inventory are the current acceptance set. Claude code review accepted this correction.
**Preset:** A · **Planner:** Claude · **Plan reviewer:** Codex · **Builder:** Codex · **Code reviewer:** Claude
**Branch (both repos, verbatim from Linear):** `pai-1453-openlibrary-new-content-request-housing-authority`
**Pinned bases:** open-library `50efc37097024070a36f619af3eb6430e4718e1f` (origin/main, Oct 20 pin); pyramid-chat `4e7d40b0ce40b7de6e39527d1f3dfd1d2828bbb9` (origin/main, Oct 23). Rebase on current main before build.
**Approach:** The existing 02:00 UTC crawl publishes an `ha` feed from three bounded SectionSpider modules. The existing orchestrator ingests it through the existing allowlist, behind a dispatch hold (default ON) checked at every dispatch seam. Release is an env change after human approval. No new scheduler, pipeline or backup system.

## Evidence (verified in cached files)

- Recursive inventory `crawler/inventory-recursive-2026-10-23.json`: 23 section pages fetched OK (plus the hub), **37 unique direct documents**, all on `www.housingauthority.gov.hk`, extensions pdf/docx only. Depth-1 inventory kept as history.
- BIM popup markup (`/tmp/pai-1453-cycle/building-information-modelling.html` lines 644–662): `javascript:popup800600Scroll('/en/common/pdf/…/HABIMSG v5.0-Vol.1-Quick Guide.docx')`. Headings: v5 current (641–647); "(Previous Version) … Version 4.0" (648); "(Previous Version) … Version 3.0" (654; **the unversioned `Volume 1 of 2 - …`, `Volume 2 of 2 - …`, `Annexes.docx` files**); "(Previous Version) … Version 2.0" (660). Sitemap `popupSingleWindow(…)` links are pages.
- Specification Library capture (`specification-library.html`): titles only. It states 2026 is for new contracts tendered from August 2026. It does **not** state that older 2022 projects may need old specs; that is a reasonable inference, not captured text. It does not state universal retirement.
- `acm.pdf` lives at `/en/common/pdf/about-us/publications-and-statistics/…`, outside `business-partnerships/resources`. The other 36 are inside it.
- Ingest: `ALLOWED_SOURCE_IDS` (`src/trigger/open-library-daily.ts:58-82`) has no `ha`. No per-source hold exists. `dryRun` returns before dispatch. `onlySourceIds` filters TODO (line 597) but not the revalidation classification (5b, lines 662–669). Targeted live runs skip revalidation (line 776).
- Processor (`open-library-processor.ts:545-563`): a forced refresh with identical bytes returns `content_unchanged` before parsing. Changed bytes are re-parsed.
- HEAD check (`open-library-daily.ts:372-413`): no stored or returned validators returns `changed`. Rotation is 500 per run (`:92-94`).
- Test isolation: `tests/test_lta_offline.py:79-80` and `tests/test_bca_offline.py:85-86` reassign `spider.get_with_retries` at import, permanently. `test_dsd_sewage_charging_offline.py` uses per-test monkeypatch.
- Licensing test asserts department-enum membership and `costOptimizer: true`. Unknown `ha` defaults to Agentic `costOptimizer: false` (`parse-tiers.ts:37`).
- `main.py`: exceptions keep the previous snapshot; skipped sources keep records; partial success replaces the source's records. The collapse check needs ≥10 prior records.
- Shared spider default `emit_page_records: true` would route HTML pages to web ingestion. Package scripts (verified): `type-check` = `NODE_OPTIONS=--max-old-space-size=6144 tsc --noEmit`; `test:unit` = `vitest run --project unit`. Lint `eslint .` and `format:check` `prettier --check .` exist in pyramid-chat (verified by root).
- Credit reference: 19,140 Agentic credits for four documents (page-count estimate; billed unknown).

## Scope decision S2 — human go required

Eligibility: **(a)** linked from a walked Resources page; **(b)** host `www.housingauthority.gov.hk`; **(c)** `.pdf`, `.docx` or `.doc`; **(d)** path under an allowed prefix; **(e)** not in the exact exclusion list. Exclusions are exact URLs in settings, copied from the recursive inventory JSON (no regex).

**Previous-edition manifest (12, excluded in both options):**
- BIM v4: `HABIMSG v4-Vol.1-Quick Guide.docx`, `HABIMSG v4-Vol.2-Detail Guide.docx`, `HABIMSG v4-Annexes.docx`
- BIM v3 (unversioned filenames; headings verified): `Volume 1 of 2 - Introduction and Quick Guide.docx`, `Volume 2 of 2 - Detail Guide.docx`, `Annexes.docx`
- BIM v2.0: `HA BIM Standards and Guidelines Version 2.0.docx`
- Specification Library 2022 set, excluded by initial-scope policy in favour of the 2026 edition for new contracts: `SL2022_052022_31052023_Orginal_OGCIO_V3.pdf`, `SL2022_042023_19062023_Batch01_OGCIO_V1.pdf`, `SL2022_042024_Batch02_OGCIO (rev 1).pdf`, `SL2022_052025_Batch03_OGCIO_final.pdf`, `SL2022_052026_Batch04_OGCIO_final.pdf`

**Option T additionally excludes these 7 (all under `construction-product-certification/`):** `OpeningSpeech.pdf`, `HKASSpeech.pdf`, `HKHASpeech.pdf`, `DrLOSpeech.pdf`, `FugroSpeech.pdf`, `GEOSpeech.pdf`, `PhotoGallery.pdf`. **The T manifest is fixed as these 7; `Using Production Certification in Construction Works_v1.pdf` stays in both options.**

These 7 are classified from parent-listing titles only; no PDF content was reviewed. Several titles read as technical seminar material, and `GEOSpeech` is technically specific, so the cut is a human content policy, not a fact.

| Option | Rule | In scope | New (not yet ingested) | Resources walker floor |
| --- | --- | ---: | ---: | ---: |
| **B (recommended)** | All current Resources PDF/Word except the 12 previous-edition items | 25 | 21 | `min_records: 17` |
| T | B minus the fixed 7-item manifest | 18 | 14 | `min_records: 12` |

Recommendation: **B**. It needs no content judgement beyond the edition exclusions. The four already-ingested documents are in both options and keep their stored names and URLs.

**S1 consequence (not retirement):** a newly published edition (for example BIM v6) enters as `current` beside v5. Old chunks stay until pyramid-os edition maintenance (human approval) links them. The crawler never retires a document. A document removed from a parent page remains searchable.

**Naming rule:** the four stored URLs keep curated `name` values from `processing/*.json`, via `document_names`. Other documents are named `Housing Authority — {page title} — {link text}`, with the filename stem added when link text repeats on the same page.

## Design

Three modules under source `ha`. Module names equal settings page keys, which are the source labels. `ha.building_information_modelling` and `ha.general_specification_for_building_works` are preserved for stored records; `ha.resources` covers all other documents.

| Module | Start | Path bound (pages) | Document path prefixes | Bounds and floors |
| --- | --- | --- | --- | --- |
| `building_information_modelling` | BIM index | `/en/business-partnerships/resources/building-information-modelling/` | `/en/common/pdf/business-partnerships/resources/building-information-modelling/`, `/common/pdf/business-partnerships/resources/building-information-modelling/` | depth 1; `min_records: 3` |
| `general_specification_for_building_works` | Spec index | `…/general-specification-for-building-works/` | `/common/pdf/business-partnerships/resources/general-specification-for-building-works/` | depth 1; `min_records: 1` |
| `resources` | Hub | `/en/business-partnerships/resources/`, excluding BIM and spec subtrees | `/en/common/pdf/business-partnerships/resources/`, `/common/pdf/business-partnerships/resources/`, `/en/common/pdf/about-us/publications-and-statistics/asbestos-containing-materials-in-public-housing-estates/` | depth 3; `max_pages: 80`; floor per S2 |

**All three sections set `emit_page_records: false`** (no HTML page records, no page-scraper tasks). `external_document_hosts` is not configured.

**Shared engine changes** (`crawlers/common/spider.py`, opt-in, defaults unchanged, covered by option tests; no review-queue flags, `main.py`, schema or backup changes):
1. `fail_on_page_error`: raise on start or followed-page fetch failure.
2. `fail_on_truncation`: raise when `max_pages`, `max_out_links_per_page` or `max_total_records` is reached.
3. `min_records`: raise below floor.
4. `exclude_document_urls`: exact canonical match.
5. `document_names`: URL → fixed name (the four).
6. `document_name_template`: the naming rule.
7. `popup_wrapper`: literal-only parser for `popup800600Scroll('<literal>')`; relative resolution, host and extension check; other `javascript:`/`onclick` ignored; a wrapper naming an allowed extension that fails to parse raises. No evaluation.

**No `required_urls` at runtime.** The four canonical URLs are initial acceptance checks (fixture and a one-time pre-release check of the live feed), not daily requirements. A publisher removal or replacement must not fail the crawl, so the previous feed is never carried forward for that reason.

## Discovery, refresh and cost behaviour

- **New link on a walked page:** found by the next 02:00 UTC crawl; ingested by the orchestrator triggered on publish (04:00 cron as fallback), subject to the hold.
- **Removed or replaced link:** dropped from the feed on the next crawl (O4). Its chunks stay until pyramid-os maintenance.
- **New section:** walked if within depth 3 and linked from a walked page; labelled `ha.resources`.
- **Same-URL refresh is automatic and required.** The orchestrator dispatches refresh with `forceRefresh: true` through normal revalidation. This is in scope.
- **Changed bytes behind an unchanged HEAD verdict:** detection waits for HEAD validators, checked up to 500 per run across all sources (~2.9k mutable URLs, so roughly six runs per URL at current size; budgeted, no hard SLA). A stale ETag or Last-Modified that still matches can hide changed bytes indefinitely. That is an existing mechanism limit; rotation does not bound detecting it, and no redesign is proposed here.
- **No validators, or a fingerprint change (name or meta):** a download happens every time. The processor parses only if bytes changed. **The no-parse claim holds only if bytes are unchanged**; changed bytes found this way are parsed and cost credits.

## Dispatch hold (the actual ingestion stop)

- New `lib/open-library/dispatch-hold.ts`: `heldSourceIds()` reads `OL_DISPATCH_HOLD_SOURCE_IDS`, **default `ha` when unset** (fail closed). Empty string releases.
- The orchestrator applies the hold at every non-dry dispatch seam: TODO files and pages, HEAD checks, fingerprint, baseline, rotation and refresh lists. Held records are counted as `held_dispatch_suppressed`.
- Dry run computes the same classification and reports would-dispatch counts with held records shown as suppressed.
- `.env.example` documents the variable name only.
- Old-version Trigger runs lack `ha` in their allowlist, so deploying C2 cannot race them. Runs triggered after deploy are held. The manual processor path (user-authorized imports) is unaffected.

## Failure detection

A fetch failure, truncation, popup parse failure or below-floor count raises. `main.py` keeps the previous `ha` records and lists the module in `crawlers_failed`. A scoped `crawl.yml` check fails the job on any `ha.` entry there.

## Tasks

Tick a task only when reviewed, verified and committed.

- [x] C1 — open-library crawler (one commit, repo `open-library`)
- [ ] C2 — pyramid-chat ingestion gate (one commit, repo `pyramid-chat`)

### C1 — open-library crawler (repo: open-library · browser check: no)

- **Files:** `crawlers/common/spider.py`; `crawlers/common/popup_links.py` (new); `crawlers/ha/__init__.py` (new, empty); `crawlers/ha/building_information_modelling.py`, `crawlers/ha/general_specification_for_building_works.py`, `crawlers/ha/resources.py` (new; each `class Crawler(SectionSpiderCrawler)`, `name` equal to file name); `config/settings.yaml` (new `ha:` block, `schedule.enabled: true`, `emit_page_records: false`); `docs/viewer-config.json` (`ha` group); `sources/hk-ha/scope.yaml` (`status: review`; the build writes `approved`, date and option only after the human go); `sources/hk-ha/README.md` (new); `tests/fixtures/ha/` (new, below); `tests/test_ha_resources_offline.py` (new); `.github/workflows/crawl.yml`.
- **Fixtures (no network):** one sanitized link/title extract per cached page, **24 files**: the 23 section pages plus the hub, from `/tmp/pai-1453-cycle/*.html`. Each extract keeps page URL, capture date and SHA-256 in `tests/fixtures/ha/MANIFEST.json`. Extract scope: anchors, popup wrappers, headings and "Previous Version" markers. BIM extract covers lines 641–664. This supports 25-URL parity from all 24 pages, not from four pages.
- **Produces / consumes:** produces `ha.*` document records in `latest/urls.jsonl`; consumes `SectionSpiderCrawler` and the `main.py` contract.
- **Steps:**
  1. Rebase on `main`; confirm pins. Build fixtures from the 24 cached pages.
  2. Add shared options 1–7 to `spider.py` (defaults unchanged); add `popup_links.py`.
  3. Add three modules, settings (`emit_page_records: false`, no `required_urls`), viewer group, scope.yaml, README.
  4. Add offline tests O1–O7, O11, including the replacement fixture below.
  5. Add the scoped `crawlers_failed` check to `crawl.yml`.
- **Replacement fixture (A):** a variant of the BIM and spec pages where the v5 trio is replaced by a v6 trio and the 2024 spec by a 2028 URL. Expected: new URLs discovered; old URLs absent from output; no failure while the floors are met.
- **Done when:** (a) `python -m pytest tests/test_ha_resources_offline.py -q` passes in its own process; each legacy offline file (`test_licensing_inventory.py`, `test_regression_guardrail.py`, `test_previous_snapshot.py`, `test_isomer.py`, `test_sweep_dedupe.py`, and the LTA/BCA/SG/payload/dsd files) passes in a separate process; (b) `python main.py --crawler ha --settings config/settings.yaml --out ./local-data` gives three `ha.` modules in `crawlers_succeeded`, none in `crawlers_failed`; (c) no `record_kind: page` or `file_ext: html` record; (d) `scope.yaml` remains `review` until the go.
- **Commit (one for this repo):** `feat(crawler): add Housing Authority Resources to recurring crawl (PAI-1453)`

### C2 — pyramid-chat ingestion gate (repo: pyramid-chat · browser check: no)

- **Files:** `lib/open-library/dispatch-hold.ts` (new); `src/trigger/open-library-daily.ts` (add `'ha'` to `ALLOWED_SOURCE_IDS`; hold at dispatch seams; dry-run classification honours `onlySourceIds`); `.env.example` (variable name); `tests/unit/open-library-ha-ingest.test.ts` (new: `isInScope`, `isFileRecord`, `resolveParseTier('ha','ha.resources')` → `{agentic, costOptimizer:false}`, hold); `tests/unit/open-library-refresh-path.test.ts` (new: processor cases, O10 i–iii); `tests/unit/open-library-orchestrator-refresh.test.ts` (new: O10 iv–v); `docs/tech/open-library/README.md` (hold and release runbook). If the processor cannot be imported under test without side effects, a narrow seam may be added in `lib/open-library/refresh-decision.ts` with no behaviour change. **Not changed:** `tests/unit/open-library-licensing-ingest.test.ts`; `scripts/open-library/lib/scope.ts`; department and category maps.
- **Steps:** 1. Hold module and env name. 2. Add `'ha'` to allowlist. 3. Apply hold at TODO, HEAD, fingerprint, baseline, rotation and refresh seams. 4. Dry-run classification honours `onlySourceIds`. 5. Mocked tests O9–O11. 6. Runbook.
- **Done when:** (a) `pnpm exec vitest run --project unit tests/unit/open-library-ha-ingest.test.ts tests/unit/open-library-refresh-path.test.ts tests/unit/open-library-orchestrator-refresh.test.ts tests/unit/open-library-licensing-ingest.test.ts` passes; (b) `pnpm type-check` passes; (c) `pnpm exec eslint <changed files>` and `pnpm exec prettier --check <changed files>` pass, with any repo-wide baseline failures from `eslint .` / `prettier --check .` on untouched main reported as pre-existing, not as new; (d) if any listed test cannot run, the build does not pass and the missing verification is reported.
- **Commit (one for this repo):** `feat(open-library): allowlist Housing Authority behind a dispatch hold (PAI-1453)`

## Verification oracles

- **O1 Fixture set:** eligibility (a–e) over the 24 fixtures yields exactly 25 URLs with Option B (18 with T). Excluded and manifest URLs are absent. No `.xls`, `.zip` or `.dwg`.
- **O2 Canonical identity:** the four URLs (`%20` form) emitted exactly, names equal to `processing/*.json`. This is an initial acceptance check, not a runtime requirement.
- **O3 Labels:** BIM → `ha.building_information_modelling`; spec → `ha.general_specification_for_building_works`; others → `ha.resources`.
- **O4 Discovery and removal:** an added link or section appears; a removed or replaced link (the v6 and 2028 replacement fixture) disappears; no failure while floors hold.
- **O5 Stability:** identical fixtures give identical records and fingerprints.
- **O6 Failure:** 404 on a section raises; `max_pages` too small raises; an unparseable `popup800600Scroll` naming `.pdf` raises; below-floor count raises.
- **O7 Popup safety:** only literal `popup800600Scroll('…')` accepted; `popupSingleWindow` and `javascript:void` ignored.
- **O8 Ingest contract:** `isInScope({source_id:'ha'})` true; unknown IDs false; tier Agentic `costOptimizer:false`.
- **O9 Hold:** held records absent from every dispatch list; dry run reports them as suppressed.
- **O10 Same-URL refresh, executable (mocked; no paid parse; no network; no corpus write):**
  - *Processor* (`refresh-path.test.ts`), with `@trigger.dev/sdk` mocked to capture `schemaTask` handlers and HTTP, parser client, storage and Mongo mocked: (i) forced refresh, **identical bytes → parser called 0 times**, revalidation metadata updated; (ii) **changed bytes → parser called once**, replacement runs, stale chunks deleted; (iii) download fails → existing chunks kept, parser not called.
  - *Orchestrator handler* (`orchestrator-refresh.test.ts`): (iv) with the hold released and a changed HEAD validator, an HA file task is dispatched with `forceRefresh: true` and the `ha` source label preserved; (v) with the hold in force, no HA HEAD check and no HA refresh dispatch occurs.
- **O11 Page exclusion:** no HA record is `record_kind: page` or `file_ext: html`; every HA record routes to file tasks, none to the web scraper.
- **Commands (verified scripts):** open-library: `python -m pytest <file> -q`, one process per file (module-level overrides in LTA/BCA/SG/payload tests make combined runs unreliable). pyramid-chat: `pnpm type-check`; `pnpm exec vitest run --project unit <paths>`; lint and format as in C2(c).

## Rollout (staged; the hold is the ingestion stop)

1. **Merge C1** after the S2 go. The next 02:00 UTC crawl publishes `ha` records; ingest ignores them (`ha` not allowlisted). Check `summary.json`: three modules succeeded, none failed. Pre-release check: the four canonical URLs present in the live feed. HEAD the new documents: record `Content-Length`, ETag and Last-Modified.
2. **Merge and deploy C2.** `ha` is allowlisted and held (`OL_DISPATCH_HOLD_SOURCE_IDS` unset). Nothing dispatches for HA.
3. **Production dry run** (no dispatch): `open-library-daily` with `{"dryRun": true, "onlySourceIds": ["ha"]}`. Expect TODO files equal to the approved count, the four absent from TODO, and `held_dispatch_suppressed` reporting the held set.
4. **Human approves** the manifest (file list, sizes, page-count cost estimate) and S2. **Release:** set `OL_DISPATCH_HOLD_SOURCE_IDS` to empty in Production (dashboard action, no code change). The next cron or publish dispatches within the 500-file cap. A targeted run skips revalidation, so it does not prove refresh.
5. **Steady state:** spot-check one new document in production retrieval after deploy (no re-parse). Watch `ingestion_runs` for `ha`: `dispatched_files` equals the approved count, no errors.

**Rollback (reversible; no corpus deletion):**
- **Stop new HA ingestion:** set `OL_DISPATCH_HOLD_SOURCE_IDS=ha`, or remove `'ha'` from `ALLOWED_SOURCE_IDS` and redeploy. Already-ingested chunks stay.
- **Stop crawling only:** `schedule.enabled: false`. This does **not** stop ingestion: skipped sources carry forward in `latest`. Use it with the hold, not instead of it.
- **Bytes and chunks already replaced:** disabling the hold does **not** restore them; a metadata revert is not a content restore. This is the existing refresh rollback limit.
- **Remove a bad document:** pyramid-os maintenance with human approval; not automated here.
- **No new backup infrastructure.** Scheduled same-URL refresh remains in scope; only destructive or paid production forced-refresh fixtures are out of scope.

## Commit tasks

- C1: `feat(crawler): add Housing Authority Resources to recurring crawl (PAI-1453)`
- C2: `feat(open-library): allowlist Housing Authority behind a dispatch hold (PAI-1453)`
- No Co-Authored-By trailers (Codex is builder); no push, PR, Linear change or deploy until a human approves.

## Risks

| Risk | Mitigation |
| --- | --- |
| Partial HA feed published | Floors, page-failure and truncation guards raise; previous snapshot kept; scoped CI check |
| Changed bytes hidden by stale validators | Existing HEAD mechanism limit, stated; no redesign here |
| Removed or superseded documents stay searchable | Stated limitation; retirement only via pyramid-os maintenance |

## Unresolved decisions and limits

- **Human go (S2):** Option B (recommended) or T with the fixed 7-item manifest. The 12-item previous-edition manifest applies to both.
- **Not verified:** publisher ETag and Last-Modified presence (Rollout step 1); Housing Bureau links (any found are dropped); the Trigger.dev environment-variable procedure.
- **Out of scope:** department and category maps (`lib/ai/tools/open-library-categories.ts`, `open-library-list-core.ts`); HA documents stay searchable without department filters until mapped. PAI-1433. Stale `scripts/open-library/lib/scope.ts`.

## Review log

Round 1 — Codex plan review (8 findings, earlier draft): R1-1 acm.pdf prefix → accepted, exact prefix exception. R1-2 licensing test cannot take `ha` → accepted, separate contract test. R1-3 deploy-before-dryRun race → accepted, hold default ON with allowlist. R1-4/5 counts, supersession, dry-run expectations → accepted, recounted, `onlySourceIds:['ha']`. R1-6 pytest contamination → accepted, import-time reassignment, per-process runs. R1-7/8 refresh proof, popup, truncation → accepted, mocked tests, literal parser, guards. R1-A–C, R1-D–E, R1-F–G (retirement wording, naming, in-repo docs, schedule disable, backup, trailers, pins) → accepted.

Round 2 — final formal Codex plan review (5 findings), fixed in revision 3: R2-1 page records not disabled → `emit_page_records: false`; O11 and C2 routing checks. R2-2 O10 not executable → mocked processor tests and build-failure rule. R2-3 rollback suggested targeted forced refresh → removed; existing rollback limit stated. R2-4 no-parse claim unqualified → qualified "if bytes unchanged"; stale validators stated. R2-5 history mislabelled → rounds separated. Precision items (2022 wording, speech/gallery manifest, commands, scope status) → accepted.

Root verification of revision 3, fixed in revision 4 (clarification of R1 discovery and required-URL resolution; no new review round): A. `required_urls` removed from engine and settings; the four URLs are initial acceptance checks only; replacement fixture added (v6 trio, 2028 spec). B. Fixtures now cover all 24 cached pages, not four; O1 parity claimed only from them. C. Orchestrator HEAD-changed refresh cases added (O10 iv–v). D. SL 2026/2022 wording corrected to inference; stale-validator limit restated without rotation claim. E. One fixed T manifest (7 items incl. GEOSpeech; Using Production Certification kept); root `crawler/scope-options.md` must be aligned to it. F. Lint and format scripts verified (`eslint .`, `prettier --check .`); scoped runs required; baseline failures reported as pre-existing. G. Header states two rounds completed, not built or deployed.

Implementation verification (2026-10-23): Claude reviewed C1 and its scheme/config fixes with no remaining blockers. Codex ran 21 HA tests plus 76 legacy/regression tests, formatting/syntax checks, and two live 27-document crawls. Sanitized fixture edges were compared to original captured publisher HTML. C1 is committed; the paired C2 gate must ship before any paid activation.
