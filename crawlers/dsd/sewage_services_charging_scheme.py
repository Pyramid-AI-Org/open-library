"""DSD - Sewage Services Charging Scheme.

The charging regime under the Sewage Services Ordinance (Cap. 463): the
Polluter Pays Principle, the Sewage Charge and Trade Effluent Surcharge, the
business classifications that set the TES rate, billing and charge adjustment,
waivers, and reassessment of discharge strength. The scheme's own appendices
and the DSD_TES / DSD_DF_COD application forms hang off these pages.

DSD's other crawlers all target document tables under /EN/Technical_Documents/,
so nothing reached this part of the site and a question about the Sewage Charge
found nothing (PAI-1037). The tree has no sitemap and its listing is plain
navigation, so it is followed with the bounded spider engine.

Note DSD spells the section two ways: the landing page sits at
/EN/SewageServicesChargingScheme/ while every child is under
/EN/Sewage_Services_Charging_Scheme/. Both prefixes are allowed.

`document_path_prefixes` is deliberately narrow. The Sewage Services Operating
Accounts page links 46 annual financial reports from a different tree; they are
company accounts rather than charging rules, and pinning documents to the
scheme's own /uploads/page/SewageServicesChargingScheme/ keeps them out.

Behaviour lives in `crawlers.common.spider.SectionSpiderCrawler`; scope lives in
`config/settings.yaml` under `crawlers.dsd.pages.sewage_services_charging_scheme`.
"""

from __future__ import annotations

from crawlers.common.spider import SectionSpiderCrawler


class Crawler(SectionSpiderCrawler):
    name = "sewage_services_charging_scheme"
