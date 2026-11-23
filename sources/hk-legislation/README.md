<!-- focus: PAI-1741 HKSAR ordinances, subsidiary legislation and ordinance guidance crawl inventory -->
<!-- use-when: operating or extending the department legislation Open Library addition -->

# HKSAR department legislation (PAI-1741)

Adds the ordinances and subsidiary legislation that Open Library's Hong Kong
departments administer, starting from the register "HKSAR Departments
Ordinances and Subsidiary Legislation.docx" on PAI-1741, plus the department
guidance documents for those ordinances. Before this, the only legislation in
Open Library was Cap 123 (`bd.buildings_ordinance`, PAI-1696).

Each chapter is crawled in English (`cap<N>!en.pdf`) and Traditional Chinese
(`cap<N>!zh-Hant-HK.pdf`) from `www.elegislation.gov.hk`. Every URL returned a
PDF through the client-check handshake on 2026-11-23. The crawler only
registers URLs; pyramid-os ingest completes the handshake
(`elegislation-fetch.ts`, PAI-1696).

## Crawl inventory

### Tier 1: the register (222 chapters)

The register lists 239 rows: 237 distinct chapters, with Caps 502 and 572 under
both BD and FSD. Its 15 Cap 123 rows are already in `bd.buildings_ordinance`.
Probing suffixes A–BZ for each principal found no in-force subsidiary chapter
missing from the register, except Cap 132 (Tier 3).

| Source label | Chapters | Chapters crawled |
| --- | --- | --- |
| `bd.legislation` | 121 (and the Chinese Cap 123) | 1 |
| `fsd.fire_safety_legislation` | 502, 572, 636 (enforced jointly by BD and FSD) | 3 |
| `fsd.legislation` | 95 family, 132 principal, 295 family | 14 |
| `emsd.legislation` | 51, 406, 449, 598, 610, 618 families | 26 |
| `labour.legislation` | 56, 57, 59, 282, 360, 469, 509, 608 families | 53 |
| `epd.legislation` | 311, 354, 358, 400, 403, 466, 499, 603 families | 123 |
| `wsd.legislation` | 102, 102A | 2 |

### Tier 2: related chapters and guidance pages

| Source label | Chapters |
| --- | --- |
| `emsd.legislation` | 470, 470A, 624 |
| `epd.legislation` | 595, 595A, 595B, 640, 611, 611A |

Guidance pages added to the document sweeps: FSD `fs_buildings_commercial_premises_ordinance`,
`fs_industrial_building`, `cert`, `accep_eq`, `timber_store`, `open_kitchen` and
`es.hkfsd.gov.hk/dg/en/`; EMSD `other_regulatory_services/cooling_towers`.
FSD pages need the trailing slash: their links are relative to it.

### Tier 3: other departments, Chinese editions, remaining guidance

| Source label | Chapters | Chapters crawled |
| --- | --- | --- |
| `had.legislation` | 344 Building Management, 626 Property Management Services, 349 Hotel and Guesthouse, 7 Landlord and Tenant | 12 |
| `fehd.legislation` | 172 Places of Public Entertainment, Cap 132 subsidiary legislation | 61 |
| `devb.legislation` | 583, 587, 652 construction industry ordinances | 7 |
| `pland.legislation` | 131 Town Planning | 4 |
| `lands.legislation` | 28 Land (Miscellaneous Provisions) | 2 |
| `emsd.legislation` | 211 Aerial Ropeways | 3 |
| `epd.legislation` | 312 Aircraft Noise | 2 |

No Open Library department administers Cap 7 (Rating and Valuation), so it
sits with HAD's property-management ordinances. Cap 132 subsidiaries were
probed to suffix DZ.

All 322 chapters also have their Chinese edition in the same list (plus the
Chinese Cap 123): 645 e-Legislation PDFs in total.

`emsd.ordinance_guidance` lists 22 EMSD documents its sweep cannot reach: the
14 regulatory regime leaflets (links written by JavaScript) and the earlier
wiring code editions (outside the content element). Earlier editions are titled
"superseded"; run the Open Library maintenance review (`ol-maint`) after ingest
so they are marked superseded rather than current.

Further pages added to the sweeps: BD `resources/legal-matters`, FSD
`mini_storages` and `scaffolded_buildings`, EMSD `DCS_in_HK/guidelines_forms`.

The sweeps' signed-duplicate rule now requires a digit before `se`/`ov`
(`APP070se.pdf`). The earlier rule also dropped any file ending in "se" or "ov",
such as Labour's `FCO_Leaflet_Japanese.pdf` and `noise.pdf`. Across all 13
sweeps the change recovers 21 documents and still excludes BD's 243 signed
duplicates.

## Refresh

Ingest change checks cannot HEAD elegislation, so they report "changed". The
rotation reaches each URL about every 160 days at 500 checks per run; the PDF is
then re-downloaded and skipped when its hash is unchanged.

## Publication order

1. Deploy the paired pyramid-os PR (department claims for the new labels).
2. Merge this PR.
3. Dispatch `crawl.yml` for `bd`, `emsd`, `fsd`, `labour`, `epd`, `wsd`, `devb`,
   `fehd`, `had`, `lands`, `pland` and `cedd`.
4. Confirm the new URLs appear in `data/latest/urls.jsonl`.
5. Run production `open-library-daily` with `dryRun: true` and those
   `onlySourceIds`, then with `dryRun: false`.
6. Run the maintenance review for the earlier wiring code editions.
7. Verify search: Cap 406E wiring, Cap 59 factories, Cap 344 building
   management, a Chinese query on 建築物條例, and a BD list that returns Cap 121
   and Cap 572 but not Cap 406.
