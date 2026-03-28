from __future__ import annotations

from adapters.asml_global import ASMLGlobalAdapter
from adapters.base import BaseAdapter
from adapters.generic_playwright import GenericPlaywrightAdapter
from adapters.portal_adapters import ApplyInAdapter, ASMAdapter, CareerLinkAdapter, RecruiterAdapter, WorkdayAdapter, GenericDetailPlaywrightAdapter, AMDAdapter
from adapters.platforms import SearchPlatformAdapter
from adapters.samsung import SamsungDSAdapter
from adapters.sk import SKHynixAdapter
from adapters.tsmc import TSMCAdapter


def build_adapter(company_cfg, source_cfg) -> BaseAdapter:
    parser = source_cfg.parser
    if parser == "samsung_ds":
        return SamsungDSAdapter(company_cfg, source_cfg)
    if parser == "sk_hynix":
        return SKHynixAdapter(company_cfg, source_cfg)
    if parser == "asml_global":
        return ASMLGlobalAdapter(company_cfg, source_cfg)
    if parser == "tsmc":
        return TSMCAdapter(company_cfg, source_cfg)
    if parser == "applyin":
        return ApplyInAdapter(company_cfg, source_cfg)
    if parser == "careerlink":
        return CareerLinkAdapter(company_cfg, source_cfg)
    if parser == "workday":
        return WorkdayAdapter(company_cfg, source_cfg)
    if parser == "recruiter":
        return RecruiterAdapter(company_cfg, source_cfg)
    if parser == "asm":
        return ASMAdapter(company_cfg, source_cfg)
    if parser == "platform_search":
        return SearchPlatformAdapter(company_cfg, source_cfg)
    if parser == "generic_detail":
        return GenericDetailPlaywrightAdapter(company_cfg, source_cfg)
    if parser == "amd_detail":
        return AMDAdapter(company_cfg, source_cfg)
    return GenericPlaywrightAdapter(company_cfg, source_cfg)
