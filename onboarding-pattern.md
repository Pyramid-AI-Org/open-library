# Onboarding pattern: adding a jurisdiction to the Open Library

*Status: proposed. Fifteen Singapore agencies specified; fourteen harvested live, 21 August 2026 —
8,968 records, 7,036 of them documents.*

## The problem this replaces

Every source in the library so far was scoped by hand. Someone screenshotted a
department's pages into a slide deck, drew an arrow at each link that mattered,
and wrote a note beside it — *"ALL pdfs inside these links are needed"*, *"the
web-text of this page and this pdf is needed"*, *"go to these 3 links"*. The
deck was the specification, and a developer read it and wrote a crawler.

That deck carried real judgement. Someone had decided that practice notes were
in and press releases were out, that the Central Data Bank sub-pages were worth
walking and the landing page was not. But it could not be diffed, could not be
reviewed by anyone who was not looking at the same screenshots, said nothing
about what had been *considered and rejected*, and had to be redrawn whenever
a site was restyled. Onboarding eighteen Hong Kong departments took months.

The pattern below keeps the judgement and drops the screenshots.

## The four phases

```
1. RECON     fingerprint the site, inventory it, measure what is there
2. SCOPE     write scope.yaml — the reviewable artifact — and get it approved
3. GENERATE  crawler modules + settings, from the approved spec
4. VERIFY    run it, compare against the spec, report the gaps
```

The human gate moved from *before the survey* to *between the survey and the
code*. Claude does the tedious part — finding every page, measuring every
section — and a person spends their attention on the part that needs it: is
this the right material to collect?

### Phase 1 — Recon

One script, `recon_site.py`, answers the two questions everything else depends
on: **how are the pages enumerated**, and **is the content in the HTML the
server returns**.

It fetches `robots.txt`, finds and follows the sitemap, fingerprints the
platform, groups every URL by path prefix, and samples pages to count the
documents each section holds. On BCA that was one command and 843 URLs grouped
into a section table, with the newsroom flagged as probably out of scope before
anyone looked at it.

The single most valuable finding is usually the platform. BCA runs on
**Isomer-Next**, the Open Government Products platform that a large share of
Singapore agencies use. Knowing that gave us, for free: a complete sitemap with
`lastmod` dates, a predictable content container, document titles in
`aria-label`, and — the big one — the fact that listing pages ship their entire
collection in the response even when they render ten items behind forty pages
of pagination.

### Phase 2 — Scope spec

`sources/<jur>-<source>/scope.yaml` is the arrow deck, rewritten as something a
person can read, argue with, diff and version. Each crawler row is one arrow.
`intent` is the note beside it, drawn from a small fixed vocabulary that maps
directly onto the old phrases:

| Old note on the slide | `intent` |
| --- | --- |
| "This Pdf is needed" | `specific_documents` |
| "ALL pdfs inside these links are needed" | `section_documents` |
| "The web-text of this page is needed" | `page_text` |
| "Go to these 3 links" | `section_documents` + explicit page list |
| (new) links to the Act on Statutes Online | `external_references` |

Three fields do the work the deck could not:

- **`rationale`** — why this belongs in a library of regulatory material, in
  plain language. A reviewer who cannot tell why a row exists cannot tell
  whether it is wrong.
- **`out_of_scope`** — sections looked at and rejected, with reasons. This is
  the difference between a decision and a blind spot, and it is what stops the
  next person re-litigating the newsroom.
- **`expected`** — measured counts from recon, which Phase 4 checks against. A
  run that returns 40 documents where the spec says 400 becomes visibly wrong
  rather than quietly wrong.

The spec carries `status: review` until a person signs off. Code should not
merge from an unapproved spec.

### Phase 3 — Generate

Scope lives in `config/settings.yaml`; behaviour lives in a shared engine. Two
engines now cover most government sites, and Phase 1 chooses between them with
one question — **is there a usable sitemap?**

| Engine | Use when | Bounded by |
| --- | --- | --- |
| `crawlers/common/sitemap.py` | The site publishes a usable sitemap. | The sitemap — the page set is known before anything is fetched. |
| `crawlers/common/spider.py` | It does not. | A path allowlist, a depth limit and a page budget, together. |

Two engines have now covered Isomer, Adobe AEM, Sitecore and Sitefinity across
fifteen agencies with nothing changed but YAML.

A crawler module is then three lines:

```python
from crawlers.common.isomer import IsomerSectionCrawler

class Crawler(IsomerSectionCrawler):
    name = "safety_and_standards"
```

One module per scope row, even though the logic is shared — the crawler name
lands in every record's `source` field, which is how the viewer filters and how
a coverage regression gets traced back to a section.

The engines are where the leverage is. Neither is agency-specific: between
them they cover any Isomer site and any site with no discovery surface at all.
Onboarding URA, HDB or NEA should be a scope spec and a handful of short
modules, not a new crawler.

### Phase 4 — Verify

`verify_coverage.py` reads the approved spec and a run's output and asks
whether they agree: are the counts within tolerance, did the exclusions
exclude, are all the hosts on the allowlist, is anything from an out-of-scope
section present, does every record have a usable name, are the named
acceptance documents actually there. It exits non-zero, so it can gate a PR.

Tested against a deliberately broken run, it caught every planted defect: a
missing crawler, an excluded path that leaked, a footer link collected as a
document, a record named "Download", a duplicate URL, and a flagship document
that had gone missing.

## The BCA result — sitemap-driven

| Crawler | Pages | Documents | Legislation links |
| --- | ---: | ---: | ---: |
| `safety_and_standards` | 56 | 378 | 65 |
| `sustainability_legislation` | 7 | 55 | 9 |
| `guidebooks_and_publications` | 8 | 44 | — |
| `accessibility_and_universal_design` | 5 | 7 | — |
| `circulars` | 1 | 351 of 404 items | — |

≈1,009 records, from one shared engine and five three-line modules.

Judgement calls worth recording, because they are the kind that recur:

- **Statutes Online links are kept**, marked `external_reference`. BCA's codes
  are written under the Building Control Act, which lives on `sso.agc.gov.sg`.
  A library of codes with no route to the law they implement is half a library.
- **Enforcement actions are cut** — they name individual contractors, turn over
  constantly, and would dominate the daily diff.
- **Green Mark scheme pages are cut; the environmental sustainability
  regulations are kept.** Promotional versus binding.
- **CORENET is deferred, not dropped** — it carries consolidated codes for
  several agencies and deserves its own source rather than being folded into
  `bca`.

## The LTA result — spider-driven

LTA was chosen as the second source precisely because it is unlike the first.
It is Adobe AEM, it publishes no sitemap at all (`/sitemap.xml` and
`/robots.txt` both 404), and the browser tooling that mapped BCA is blocked on
the domain. It was the test of whether the workflow survives a site that gives
you nothing.

| Crawler | Pages | Documents | Circulars (CORENET) |
| --- | ---: | ---: | ---: |
| `transport_infrastructure_standards` | 2 | 17 | — |
| `street_works` | 5 | 26 | — |
| `railway_protection` | 3 | 10* | — |
| `development_construction_resources` | 1 | 9 | — |
| `active_mobility` | 2 | 2 | 3 |
| `vehicle_parking` | 2 | 2 | — |

\* estimate — the page returned 403 to the survey tooling; the spec says so and
asks for re-measurement on the first live run.

The material is the Highways Department set, in Singapore: Civil Design
Criteria for Road and Rail Transit Systems, the Infrastructure and
Architectural Design Criteria volumes, Materials and Workmanship
specifications, Standard Details of Road Elements, and the codes of practice
for street works, works on public streets and traffic control at work zones.

Judgement calls specific to this one:

- **Forms and checklists are kept.** On most sites a form is administrative
  clutter. Here a submission is judged against the current version of the form,
  so the form *is* the requirement.
- **Superseded revisions are kept.** Civil Design Criteria Rev A2 is still
  published beside Rev A3, and the revision in force when a project was
  designed is the one that governs it.
- **The hub row is pinned to depth 0**, because its children have their own
  rows and letting it follow into them would record every document twice.
- **LTA's circulars are not on LTA's site.** They go to CORENET, which
  strengthens the case for building CORENET as its own source — it now carries
  deferred material for two agencies.

The important result is negative: nothing about the four phases had to change.
Only the engine did. What the spec had to absorb was the loss of certainty —
with a sitemap, `documents: 378` is a count; with a spider, `documents: 26` is
a floor, and the spec says which is which. That distinction is what makes an
approver's sign-off meaningful rather than nominal.

## The full Singapore set

Thirteen further agencies were onboarded in one pass, translating every Hong Kong
department in the equivalence document. Fifteen sources, 53 crawlers, 42 new scope
rows — and the whole thing needed no new engine, only two upgrades to the existing
ones.

| Hong Kong | Singapore | source_id | Status |
| --- | --- | --- | --- |
| Buildings Department | Building and Construction Authority | `bca` | **Harvested** — 1,009 records |
| Highways + Transport | Land Transport Authority | `lta` | Specified |
| Lands Department | Singapore Land Authority | `sla` | **Harvested** — 333 records |
| Planning Department | Urban Redevelopment Authority | `ura` | Specified |
| Drainage + Water Supplies | PUB | `pub` | Specified |
| Environmental Protection | National Environment Agency | `nea` | Specified |
| Fire Services | Singapore Civil Defence Force | `scdf` | Specified |
| Labour Department | Ministry of Manpower | `mom` | Specified |
| Architectural Services | HDB and JTC | `hdb`, `jtc` | Specified |
| Electrical & Mechanical | Energy Market Authority | `ema` | Specified |
| Hong Kong Herbarium | National Parks Board | `nparks` | Specified |
| Legislative Council | Parliament of Singapore | `parliament` | Specified |
| Telephone Directory | SGDI | `sgdi` | Specified |
| Development Bureau | Ministry of National Development | `mnd` | Disabled |

**Specified** means the scope spec, crawler modules, settings block and offline tests
all exist and pass; what is missing is a live run, because the build environment
cannot reach government hosts. Distinguishing that from **Harvested** in every
summary is the difference between a plan and a claim.

### What the second wave taught the engines

Two upgrades, both prompted by real failures rather than anticipated:

**Sitemaps have three failure modes and each looks like success.** MOM publishes a
`<sitemapindex>` rather than a `<urlset>`, and the parser returned zero pages while
appearing healthy. NEA stamps all ~520 sitemap entries with the same 2018 date, so
using `lastmod` as a publication date fabricates data. And most Sitecore and
Sitefinity sitemaps list pages but no documents at all. `parse_sitemap` now returns
`(entries, nested)` and the caller follows children; the rest is a spec-writing
discipline.

**Every CMS has one asset path, and it beats every other document filter.** AEM keeps
files under `/content/dam/`, Sitecore under `/-/media/`, Sitefinity under
`/docs/default-source/`, Isomer on a shared host under a numeric tenant id. Nothing in
a site's navigation ever points into those, so one prefix separates documents from
chrome exactly — no content-container heuristic required. Four prefixes covered
fifteen agencies.

Both engines also learned to strip cache-busting parameters (`?sfvrsn=`, `?hash=`)
from record identity. Records are keyed on the URL, and Sitefinity re-stamps that
parameter on every republish — left in, a daily diff fills with phantom churn.

### What the third wave added: a third engine

The two engines above both assume the links are anchors. Six sources proved that
assumption wrong in six different ways, so `crawlers/common/payload.py` adds three
more shapes for sites whose data is in the response but not in the DOM:

- **`FlightPayloadCrawler`** — one request, and the whole collection comes back
  inside the Next.js flight payload. Parliament's Order Paper is 2,168 records with
  no pagination at all.
- **`ServerActionCrawler`** — for the same framework when only page one is embedded.
  Replays the pagination server action with the limit raised; 619 and 772 records in
  one call each.
- **`ApiIndexCrawler`** — for an out-of-band JSON index of unknown shape. It walks
  whatever comes back, records every asset path with a document extension, and pairs
  each with the nearest title and date above it in the tree. Deliberately schema-less,
  because the point is to work without knowing the schema.

The last of those is also the engine CORENET has been waiting for.

A caution that came with them, and it is the reason the engines print as loudly as
they do: **all three of these failure modes are silent by default.** A payload key
that has been renamed, a listing that returns ten of 619, a `.model.json` URL that
quietly serves HTML, a server action whose build hash has gone stale — none of them
raises, and every one of them yields a plausible-looking empty result. Each now
prints a specific message, and one of them raises outright.

### Where the mapping does not hold

Four findings matter more than the counts, because they are the kind a table hides:

- **The Development Bureau does not map to a ministry.** MND is a policy ministry with
  no circular library at all; the technical circulars are BCA's and URA's. `sg-mnd`
  ships disabled and says why.
- **The Planning Department mapping inverts.** In Hong Kong the full chapter was the
  PDF and the summary was to be skipped. At URA the full handbook is the *HTML page*
  and the only PDF is the summary — so collecting documents alone would gather exactly
  what the original reviewer excluded.
- **LegCo has no counterpart.** Singapore's Parliament has no panel system and nothing
  like the Public Works Subcommittee. The substance lives in Hansard, which is not
  published as documents but is available whole through a JSON endpoint, one call per
  sitting.
- **The Herbarium equivalent does not exist until March 2027.** Flora & Fauna Web is
  the substitute, and it is unusually well-shaped: records are server-rendered at
  URLs derivable from the record id, so the database can be enumerated by counting.

### CORENET, four times over

The clearest signal from doing many agencies at once. BCA publishes some circulars to
CORENET, LTA publishes essentially all of them there, SCDF has a folder there, and
PUB's circulars are mirrored there server-rendered while PUB's own listing is not.
Four specs now defer material to one portal that serves seven regulators — and it is
driven by a clean JSON API with issuer, category and effective date on every item.

Building it once is worth more than four partial duplicates. It needs a third engine,
API-driven rather than sitemap or spider, and it is the single highest-value next
piece of work.

## The live runs — what fifteen agencies actually returned

Specifying a source and running one are different claims, and the gap between them
is where this kind of work usually goes wrong. It went wrong here, twice, in ways
worth writing down.

Eight sources were harvested on the first pass: **3,646 records, 2,876 documents**.
Six more had been specified and either returned nothing or returned far too little.
A second pass fixed five of those six and pinned down why the sixth cannot be
fixed from here. The corpus now stands at **8,968 records, 7,036 of them documents,
across fourteen agencies** — without a single site having changed.

| Source | Records | Documents | Note |
| --- | ---: | ---: | --- |
| `parliament` | 3,562 | 3,559 | was 0 — wrong prefix *and* wrong engine |
| `bca` | 1,015 | 823 | as measured |
| `ema` | 822 | 694 | was 94 — two unlinked JSON indexes |
| `sgdi` | 699 | 0 | directory pages by design |
| `nparks` | 504 | 219 | was 119 — listings are not server-rendered |
| `scdf` | 482 | 444 | 3× the survey estimate |
| `ura` | 444 | 229 | 3 rows truncated |
| `nea` | 440 | 311 | 1 row truncated |
| `mom` | 361 | 235 | 2.3×, once the sitemap index was followed |
| `sla` | 333 | 332 | exactly as predicted |
| `pub` | 115 | 79 | 2 rows return nothing, as predicted |
| `lta` | 80 | 65 | was 0 — page scripting blocked on the domain |
| `jtc` | 72 | 46 | 1.8× |
| `hdb` | 39 | 0 | **blocked** — pages only |
| `mnd` | — | — | **disabled** — robots.txt bot challenge |

Per-source counts read lower than the first build in places (SCDF 686 → 482, MOM
517 → 361) because the database now keys one row per URL per agency, the way the
repository keys records. A file linked from two crawler rows became one row naming
both sections. Nothing was lost; the earlier figures counted the same document twice.

Every scope row carries `measured: true`, `measured: partial` or `measured: false`,
so the two kinds of number are never confused.

### The lesson the second pass taught

All six sources that came back empty had run **cleanly**. No exception, no timeout,
and five of the six reported a plausible page count. Nothing in the output said
anything was wrong.

> **"Pages fetched" and "records found" are different observations, and only the
> second is coverage.**

A listing row that returns pages and zero documents is a *defect*, not a finding —
and it is the single cheapest signal to act on, because it is visible in the run
output the moment the run ends. Five of the six were extraction bugs sitting behind
exactly that signal.

### The six, and what was actually wrong

**Parliament, 0 → 3,559 documents.** Two mistakes compounding. The survey read
`/docs/default-source/` off the corporate pages — correct for the rest of the site,
and wrong for every document this source wants, which are served from
`/api/media/<uuid>/<filename>`. The prefix excluded all of them. Separately,
`/parliamentary-business/**` is a Next.js app whose item links are React handlers
rather than anchors, so an HTML crawler sees the listing and none of its contents.
One host, two platforms, and the spec described only one of them. The Order Paper
turned out to ship all 2,168 records in its first response; the other two listings
return 619 and 772 from a single server-action call each.

**EMA, 94 → 822 records.** The spec said, in as many words, "no machine-readable
discovery surface at all". There were two, neither linked from anywhere a crawler
would look: `/bin/servlets/custom-sitemap` returns all 1,452 published pages as
JSON, and `/graphql/list.json` enumerates every AEM persisted query *with its query
text*. Executing those queries returned the collections the Angular listings render
— one of them holding 504 documents on its own.

**NParks, 119 → 504 records.** Pages server-rendered, listings not. Two of the three
seeds in the original spec were JavaScript listings, so the spider fetched the seed
and stopped. Driving from the sitemap instead reaches 260 pages and 199 documents,
plus 45 more from CUGE, which sits on its own origin and needs its own run.

**SGDI, 0 → 699 records.** The `statutory_boards` row matched exactly one page. Every
board on `/statutory-boards` is linked as
`/ministries/<ministry>/statutory-boards/<board>` — right about the section, wrong
about the URLs, and a path allowlist has no way to tell you so.

**LTA, 0 → 80 records.** Not a site problem at all: page scripting is blocked on the
domain by the browser tooling's own policy, and the build environment cannot reach
`.gov.sg`. A plain fetching proxy that returns pages as markdown reaches them one at
a time — slower, and enough to onboard the source rather than defer it. It also
turned up an HTML sitemap at `/content/ltagov/en/sitemap.html`, linked only from the
footer, listing a page that link-following never reached.

**HDB, still blocked — and precisely so.** Two obstacles, each defeating exactly what
the other would have solved: HTTP 403 to the browser route on every request including
the homepage, and JavaScript-rendered page bodies for the route that *can* reach the
host. The outcome is a complete page inventory (39) and an empty document inventory,
and the spec says so with `measured: partial`, `documents: 0` and a
`documents_target: 30` beside it. HDB's `/-/media/` asset tree does broadly mirror
its page tree, which makes inferring the document URLs tempting and cheap. It is a
pattern, not a guarantee, and a library full of guessed URLs is worse than one with
an honest gap — because the gap is visible and the guesses are not.

### Finding an endpoint without a proxy

Twice this pass, the fix was an endpoint nothing linked to. The technique that found
both needs no interception, no devtools protocol and no proxy: load the listing in a
browser and read

```js
performance.getEntriesByType('resource')
  .filter(e => e.initiatorType === 'fetch' || e.initiatorType === 'xmlhttprequest')
  .map(e => e.name)
```

It works *after* the page has loaded. Whatever the page called is in that list. On a
JavaScript listing, that list is the answer — and it is now the first thing to try
before writing "no discovery surface" into a spec.

### Some things must be read, not guessed

Parliament's two paginated listings need a `next-action` header whose value is a
**build hash**: it changes on every deploy, appears only in the compiled JS, and
cannot be derived from the page. It has to be captured from a live pagination request
and put in settings. Two consequences shaped the engine. A stale value returns HTTP
500, so the crawler raises rather than reporting an empty collection — swallowing it
would turn a 772-document source into a silent zero. And the document URL has to come
out of the record: an earlier attempt built `/api/media/<record id>` from a payload
that carried no file field and produced 619 URLs that all looked right and all 404'd.

### Rows that stopped at their page budget, and said so

`mom.workplace_safety_and_health` stopped with 97 URLs queued, `ura.development_control`
with 34, and three others with fewer. Each is recorded in the spec as
`truncated_queue`, and each of those counts is explicitly a floor. `sgdi` truncated
too, with 343 queued — but that one is deliberate: it is bounded at depth 2 because
depth 3 is officer records, which this source does not want. The spec says which
kind of truncation it is.

This remains the single design decision that paid off most. A crawl that hits its cap
and stops silently is indistinguishable from one that covered its section, and the
coverage report scores it as a pass.

### Predictions confirmed, and two counts that look wrong

PUB's codes-of-practice and circulars listings were flagged during survey as
JavaScript-rendered with floor counts. **Both returned exactly zero documents.**
Predicting a limitation and then measuring it is the difference between a caveat and
a finding.

Two results look like failures and are not, and both are now written into their specs
so nobody "fixes" them later. `nparks.publications_resources` returns 111 pages and 2
documents — checked by diffing raw HTML against the DOM on a 25-page sample, which
found no document URL hidden in a payload. That section really is prose articles with
nothing attached. And `ema.licences` holding 504 of EMA's 694 documents is one
persisted query returning one collection, not double-counting.

### Fixes, measured rather than asserted

- MOM: **222 → 517 raw records** once the sitemap parser followed `<sitemapindex>` to
  its children. Read as a urlset, an index yields nothing while the crawler looks
  perfectly healthy doing it.
- Parliament: **0 → 3,559 documents** from one corrected path prefix and a change of
  engine.
- EMA: **94 → 822 records** from two endpoints that were there the whole time.

Quote the before and after. "Now handles payload-rendered listings" is a claim; those
numbers are a result, and they tell the next person how much of a corpus this class
of bug can hide.

## Technique worth reusing

**Sitemap-first discovery.** One request, complete coverage, usually with
`lastmod` dates. Far better than spidering, and it cannot miss a page hidden
behind a mega-menu.

**The Next.js payload trick.** A listing that says "404 items" and renders ten
is not a forty-page walk. Concatenate the `self.__next_f.push` chunks, unescape
each as a JSON string literal, and the whole collection is there with titles and
dates. Confirm the item count matches what the page claims, then trust it.
Unescape each chunk *before* joining — chunk boundaries fall inside escape
sequences, and joining first corrupts exactly the titles that contain quotes.

**When the payload holds only page one, replay the pagination action.** Next.js
server actions take a limit field that is usually not capped server-side: sending
1,000 where the UI sends 10 returned whole collections in one request. The
`next-action` header is a build hash — capture it, do not guess it, and let a
stale one fail loudly.

**Ask the browser what the page requested.** `performance.getEntriesByType('resource')`
lists every fetch a page made, after the fact, with no interception. On a
JavaScript-rendered listing that list is the shortest path to the data. On AEM,
also try `/graphql/list.json` — it enumerates every persisted query with its text
— and `/bin/servlets/*`. Neither is linked from anywhere.

**Treat "pages but no documents" as a defect, not a finding.** It is the one
signal that catches a wrong extraction route at zero cost, and it caught five
sources here.

**Attributes over anchor text for names.** Visible link text is written for
layout — truncated, or just "Download". `aria-label` carried the full title
plus `[PDF, 5.1 MB]` on every BCA document. The record name is what a person
searches on later.

**Scope extraction to the content container.** On the Building Control Act page
that was the difference between 71 links and 118; the 47 dropped were header,
mega-menu, footer and six social accounts.

**Identify documents by asset path where the CMS has one.** On AEM every
published file sits under `/content/dam/` and nothing in the navigation points
there. One prefix does what a content-container heuristic does, but exactly.

**Strip cache-busting query strings from record identity.** `?sfvrsn=`, `?v=`
and their relatives make the same file look new on every run. Records are keyed
on the URL, so the daily archive diff fills with phantom adds and removes. Keep
the fetchable URL in metadata and use the clean one as identity.

**Resolve titles page-wide, not link by link.** A "Codes of Practice" heading
over three PDFs names the group, not any member of it — falling back to it per
link produces records nobody can tell apart. Claim a heading only when one link
on the page sits under it. This came out of LTA and was applied back to the
Isomer engine, which is the argument for shared engines in one sentence:
copied loops do not get better when a second site teaches you something.

**Say when a bound bites.** A crawl that hits `max_pages` and stops silently is
indistinguishable from one that covered the section — and the coverage report
scores it as a pass.

**Offline tests with realistic fixtures.** Two layers: parser tests against
markup shaped like the real thing, and a wiring test that stubs HTTP and runs
the real crawlers through the real settings file. Neither needs the government
site to be up, and together they catch the failures that actually happen — a
restyle that moves the content container, or a spider that quietly escapes its
section.

## Where this goes next

**Immediate:** a person reviews the fifteen scope specs and sets the ones they agree
with to `approved`. Then re-run the five truncated rows with a larger `max_pages`
and write the new counts back. Before the first production run of Parliament,
re-capture the two `action_id` values — they are build hashes, they go stale on every
site deploy, and the settings file ships with placeholders on purpose. Retry
`lta.railway_protection` and the one refused `street_works` page; those are hard
constraints on building near an MRT line and they are the one knowingly short row in
that source.

**Then:** HDB is the only source still blocked, and it needs something this
environment does not have — a headless browser that can reach the host, or an agreed
crawl path with the site owner. It should not be closed by inferring `/-/media/` URLs
from page paths.

**Worth building once, at jurisdiction level rather than per agency:**

- A **Statutes Online source**, so Singapore legislation is collected properly
  rather than only as references reached from agency pages.
- A **CORENET source**. This has now come up twice: BCA publishes some
  circulars there, and LTA publishes essentially all of them there. CORENET
  carries circulars for BCA, LTA, URA, SCDF, PUB and NEA together, so building
  it once is worth more than six partial duplicates. Already in the repository
  TODO.
- A second shared engine for whatever platform the next non-Isomer agency uses.
  WordPress and Drupal both expose clean JSON APIs and would each be a day.

**Retrofit, optionally:** the eighteen Hong Kong sources have no scope specs.
Writing them retrospectively would make the existing coverage reviewable and
let `verify_coverage.py` watch them for drift. Worth doing for the sources
whose crawlers are hardest to read.
