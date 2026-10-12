<!-- focus: PAI-1457 reviewed licensing PDF inventory and ingestion verification -->
<!-- use-when: publishing or validating the Hysan licensing addition -->

# Hysan licensing PDFs (PAI-1457)

The exact 16-document inventory lives under the five `licensing_guides` sections
of `config/settings.yaml`. These direct PDFs enter the normal crawler feed and
are downloaded, parsed and periodically checked by the existing file pipeline.
The crawler intentionally does not download 1,527 PDF pages on each catalog run.

All 16 URLs returned valid PDFs on 2026-10-12. The EDB PDF is the extension guide,
not a new-school guide. The drug-treatment PDF filename contains `20221101`, but
its cover says December 2001; do not infer an edition from the filename.
Publication dates remain null: a month/year on the cover is not an exact day.

## Publication order

1. Deploy pyramid-os department/source/domain mappings and pyramid-chat
   ingestion allowlist/parser routing/tool schemas from the paired PAI-1457 PRs.
2. Merge this catalog addition, then dispatch `crawl.yml` per new department
   (`fehd`, `swd`, `had`, `edb`, `police`). Scoped runs preserve other sources.
3. Verify the exact inventory is present in `data/latest/urls.jsonl`.
4. The publish hook starts the full daily orchestrator after 10 minutes. For
   immediate verification, run production `open-library-daily` with
   `{ "dryRun": true, "onlySourceIds": ["fehd","swd","had","edb","police"] }`.
   Check skips/failures/caps and storage binding, then run the same payload with
   `dryRun: false`. The manual task is not production-guarded; select production.
5. For every PDF, verify indexed chunks and embeddings, search by title and
   department, list under `publications`, ask a content question and open its
   citation. Preserve null dates and current/canonical defaults. Retry failures
   through the existing processor; never force-refresh successes or remove data.

No schema changes, new infrastructure or PAI-1433 ingestion migration are needed.

## Download verification

| Department | Document | Pages | SHA-256 |
|---|---|---:|---|
| fehd | [A Guide to Application for Restaurant Licences](https://www.fehd.gov.hk/english/howtoseries/forms/new/A_Guide_to_Restaurant.PDF) | 256 | `54d593e5472fe6e6d077130b7ce6fe239f0dcd495506d26049ca5d721e769689` |
| fehd | [Guide to Application for Places of Public Entertainment Licences (Cinemas/Theatres)](https://www.fehd.gov.hk/english/forms/pdf/guide_fehd104.pdf) | 107 | `8dc3c01a240125c82313b0fa1d811f19dd17567ab998ad83ae55fbcf81671280` |
| fehd | [Guide to Application for Places of Public Entertainment Licences for Places Other Than Cinemas and Theatres](https://www.fehd.gov.hk/english/forms/pdf/ppe_non_cinema_guide.pdf) | 151 | `d28ae9cce304fb90d436c79b6c45f0d8a4d67498e7ecaa1214e3fe2954ad31d0` |
| fehd | [Guide to Application for Karaoke Establishment Permits in Restaurants](https://www.fehd.gov.hk/english/forms/karaoke.pdf) | 53 | `04f94c098b1448a68e546450092582c19801da222410fbc0692e8603d4aee387` |
| fehd | [Guide to Application for Liquor Licences and Club Liquor Licences — Liquor Licensing Board](https://www.fehd.gov.hk/english/licensing/ll-cll-appguide.pdf) | 44 | `6194fef4fe632db7dee5273cb128f0ce4402067386e225c0c8e6833f5e670ef4` |
| swd | [Code of Practice for Residential Care Homes for the Elderly — June 2024 Revised Edition](https://www.swd.gov.hk/storage/asset/section/417/Code_of_Practice_for_Residential_Care_Homes_for_the_Elderly_June_2024_Revised_Edition(2).pdf) | 268 | `894b9480d9c2f9cb24fb8b137ea01d444695d1f0dd140cc431b07832c48d0b90` |
| swd | [Code of Practice on Mutual Help Child Care Centres — Revised September 2021](https://www.swd.gov.hk/storage/asset/section/435/en/Code_of_Practice_on_MHCCC_eng_(Revised_September_2021).pdf) | 27 | `dcc68b77c00727f8e053cdd20beb80348c4331a0bd1b058a471b1ab8d58564aa` |
| swd | [Code of Practice for Drug Dependent Persons Treatment and Rehabilitation Centres](https://www.swd.gov.hk/storage/asset/section/429/en/LODTC_CoP_20221101(Eng).pdf.pdf) | 78 | `9e67c1c76d8b6393d45bbc23e36ef96a117b29a6b643a5557689c8a8b5006dff` |
| swd | [Code of Practice for Residential Care Homes for Persons with Disabilities — June 2024 Revised Edition (Updated October 2024)](https://www.swd.gov.hk/storage/asset/section/423/Code_of_Practice_for_RCHDs_June_2024_Revised_Edition_Eng_(Updated%20in%20Oct%202024).pdf) | 270 | `6e0ef6e34eaf9ff29855213030c23c564064fa1aa2d12d4c998bc262bbe7611c` |
| had | [A Layman's Guide to Licence Applications under the Hotel and Guesthouse Accommodation Ordinance](https://www.hadla.gov.hk/filemanager/en/docs/layman.pdf) | 22 | `954649eda908eb55c398cdfcd16b1eca40db67c67d0d3182b2a1a1edafd3c78e` |
| had | [A Layman's Guide to Certificate of Compliance Applications under the Clubs (Safety of Premises) Ordinance](https://www.had.gov.hk/file_manager/en/documents/public_services/licensing/Laymans-Guide.pdf) | 21 | `458fd42ddde797acc60937aa78d59253133d2f1c480fba6b721e924ec663a744` |
| edb | [Guidelines for Extension of School Premises (For Premises Not Designed and Constructed as a School) — June 2026](https://www.edb.gov.hk/attachment/en/sch-admin/sch-registration/about-sch-registration/Guidelines%20Extension%202026/3b_eguide-ext%20of%20sch%20premises_purpose-built.pdf) | 128 | `315356551c302dfce6f3a700bbfc17b375a7517952f630a4da50b573e31a3516` |
| police | [A Guide to the Application for a Massage Establishments Licence](https://www.police.gov.hk/info/doc/licensing/general/en/gls_mel_Guideline(e).pdf) | 35 | `25ed6bfe162daa3cae0ac3b1a422174bccaf46d23bc03f7bbf631c534c9b072e` |
| had | [Guidance Notes on Application for the Grant of Amusement Game Centre Licence — Revised December 2025](https://www.hadla.gov.hk/el/filemanager/common/docs/forms/Guidance_Notes_on_Application_for_the_Grant_of_Amusement_Game_Centre_Licence_eng.pdf) | 29 | `58ec1bde473bc831d104b58f43d5efa58be22591af007a3440578f60ec276ce0` |
| had | [Mahjong/Tin Kau Licence Application Forms and Guide — December 2025](https://www.hadla.gov.hk/el/filemanager/common/docs/forms/MJ-TK_Application_Forms_Guide_eng.pdf) | 19 | `e90baaeceb6f771c98db1b55a04064cffb18ce71712b69c191dac516238fb5c5` |
| had | [Public Dance Hall/Dancing School Licence Application Forms and Guide — Revised December 2025](https://www.hadla.gov.hk/el/filemanager/common/docs/forms/PDH_DS_Application_Forms_Guide_eng.pdf) | 19 | `80547852693c46030e4417c8cde0b40ad17205866cf1189ea2093ce876715f8c` |
