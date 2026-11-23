<!-- focus: PAI-1741 HKSAR department ordinances and subsidiary legislation crawl inventory -->
<!-- use-when: operating or extending the department legislation Open Library addition -->

# HKSAR department legislation (PAI-1741)

Adds the ordinances and subsidiary legislation that BD, EMSD, FSD, Labour, EPD
and WSD administer, from the register "HKSAR Departments Ordinances and
Subsidiary Legislation.docx" on PAI-1741. Before this, the only legislation in
Open Library was Cap 123 (`bd.buildings_ordinance`, PAI-1696).

Every chapter is the English PDF `https://www.elegislation.gov.hk/hk/cap<N>!en.pdf`.
The register lists 239 rows: 237 distinct chapters, with Caps 502 and 572 under
both BD and FSD. Every register URL and every related chapter below returned a
PDF through the client-check handshake on 2026-11-23. The crawler only registers URLs; pyramid-os
ingest completes the handshake (`elegislation-fetch.ts`, PAI-1696).

## Crawl inventory

### Tier 1: the register (222 PDFs)

| Source label | Chapters | PDFs |
| --- | --- | --- |
| `bd.legislation` | 121 | 1 |
| `fsd.fire_safety_legislation` | 502, 572, 636 (enforced jointly by BD and FSD) | 3 |
| `fsd.legislation` | 95 family, 132, 295 family | 14 |
| `emsd.legislation` | 51, 406, 449, 598, 610, 618 families | 26 |
| `labour.legislation` | 56, 57, 59, 282, 360, 469, 509, 608 families | 53 |
| `epd.legislation` | 311, 354, 358, 400, 403, 466, 499, 603 families | 123 |
| `wsd.legislation` | 102, 102A | 2 |

The register's 15 Cap 123 rows are already in `bd.buildings_ordinance` and are
not repeated. The register puts the Cap 132 principal ordinance under FSD, so
`fsd.legislation` carries it. Probing suffixes A–BZ for each principal found no in-force subsidiary
chapter missing from the register, except Cap 132 (Tier 3).

### Tier 2: enhancements (9 PDFs and 8 guidance pages)

Related chapters administered by the same departments, added to their lists:

| Source label | Chapters |
| --- | --- |
| `emsd.legislation` | 470, 470A Builders' Lifts and Tower Working Platforms (Safety); 624 District Cooling Services |
| `epd.legislation` | 595, 595A, 595B Hazardous Chemicals Control; 640 Mercury Control; 611, 611A Motor Vehicle Idling |

Ordinance guidance pages whose documents were not in the feed, added to the
department document sweeps:

| Sweep | Page | Documents |
| --- | --- | --- |
| `fsd_document_sweep` | `fire_protection/fs_buildings_commercial_premises_ordinance` | Cap 502 guide |
| `fsd_document_sweep` | `fire_protection/fs_industrial_building` | Cap 636 pamphlets, checklists |
| `fsd_document_sweep` | `fire_protection/cert` | Cap 95B FSIC letters, checklists |
| `fsd_document_sweep` | `fire_protection/accep_eq` | approved and accepted equipment lists |
| `fsd_document_sweep` | `fire_protection/timber_store` | Cap 95 timber store guides |
| `fsd_document_sweep` | `fire_protection/open_kitchen` | open kitchen design guide |
| `fsd_document_sweep` | `es.hkfsd.gov.hk/dg/en/` | Cap 295 dangerous goods advice |
| `emsd_document_sweep` | `other_regulatory_services/cooling_towers` | cooling tower O&M practice |

### Tier 3: not crawled

| Item | Why not |
| --- | --- |
| Cap 132 subsidiaries (46) | FEHD's ordinance; FSD administers only parts |
| Caps 344, 626, 7, 131, 28, 583, 587, 652, 172, 349 | other bureaux and departments; needs a scope decision |
| Caps 211, 312 | EMSD / EPD, but outside building and property work |
| EMSD regulatory regime leaflets (14) | links are written by JavaScript; needs a document list |
| Chinese and bilingual chapter PDFs | register asks for English |
| Superseded Cap 406 CoP editions, DCS forms, mini-storage records, scaffold forms, Labour translations | superseded, forms or translations |

## Refresh

Ingest change checks cannot HEAD elegislation, so they report "changed". The
rotation covers each URL about every 160 days at 500 checks per run; the PDF is
then re-downloaded and skipped when its hash is unchanged.

## Publication order

1. Deploy the paired pyramid-os PR (department claims for the new labels).
2. Merge this PR.
3. Dispatch `crawl.yml` for `bd`, `emsd`, `fsd`, `labour`, `epd` and `wsd`.
4. Confirm the new URLs appear in `data/latest/urls.jsonl`.
5. Run production `open-library-daily` with
   `{ "dryRun": true, "onlySourceIds": ["bd", "emsd", "fsd", "labour", "epd", "wsd"] }`,
   then with `dryRun: false`.
6. Verify search: Cap 406E wiring, Cap 59 factories, Cap 95B FSIC, Cap 311 air
   pollution, and a BD list that returns Cap 121 and Cap 572 but not Cap 406.
