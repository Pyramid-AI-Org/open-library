<!-- focus: PAI-1696 Cap 123 Buildings Ordinance and Minor Works crawl inventory -->
<!-- use-when: operating or extending the Cap 123 / Minor Works Open Library addition -->

# Cap 123 and Minor Works (PAI-1696)

Adds the Buildings Ordinance family and the Buildings Department Minor Works
Control System pages that were missing from Open Library.

## Crawl inventory

### Legislation — `bd.buildings_ordinance` (18 PDFs)

Bounded list on `www.elegislation.gov.hk`: Cap 123 principal plus Cap 123A–Q.

| Cap | Title | Priority |
| --- | --- | --- |
| 123 | Buildings Ordinance | — |
| 123A | Administration | — |
| 123B | Construction (regs) | — |
| 123C | Demolition Works | family |
| 123D | Lifts (historical) | family |
| 123E | Escalators (historical) | family |
| 123F | Planning (~45 MB) | — |
| 123G–M | streets, refuse, drainage, ventilating, oil, appeal, energy | Cap 123I for Hysan |
| 123N | Minor Works | first — Kai Shing item questions |
| 123O–Q | Minor Works fees, Inspection and Repair, Construction | — |

The elegislation HTML index tabs (`?tab=s`, `?tab=m`) and HTML statute views are
JS shells: after the client-check handshake they still contain no ordinance
text. They are **not** in the crawl list. The verified PDF downloads are the
corpus. Cap 123D/E remain published on the site as historical lift/escalator
regs.

elegislation serves a client-check gate. The crawler only registers URLs;
pyramid-os ingest completes the handshake before downloading (paired PR).

### Minor Works pages — `bd.minor_works`

Section spider over `/en/building-works/minor-works/` plus `/en/resources/legal-matters/`.

Start URLs cover the family indexes:

- Minor Works home, introduction, procedures, designated exempted works
- **Minor Works Items** (per-item pages such as item 1.17)
- Types of Minor Works (updates more often — Yip)
- Guidelines / pamphlets
- Legal matters index

CoP MBIS/MWIS, MWGG and MWTG are already collected by the existing BD codes
crawlers (`codes_design_manuals_*`). This section fills the HTML gap Kai Shing
asked for and re-discovers pamphlet / types-table PDFs for delta checks.

BD `schedule.interval_days` is `1`, so the nightly `crawl.yml` keeps these
sources in `data/latest/urls.jsonl`. After the first ingest, `open-library-daily`
only re-fetches URLs that are new or whose content validators / hash change.

## Publication order

1. Merge this open-library catalog addition and the paired pyramid-os
   elegislation fetch PR.
2. Dispatch `crawl.yml` with crawler `bd.buildings_ordinance`, then
   `bd.minor_works`.
3. Confirm the new URLs appear in `data/latest/urls.jsonl`.
4. Run production `open-library-daily` with
   `{ "dryRun": true, "onlySourceIds": ["bd"] }`, then the same with
   `dryRun: false`.
5. Verify Cap 123N search (e.g. item 1.17), Cap 123I, Types of Minor Works /
   item pages, and that later daily runs only re-fetch on delta.
