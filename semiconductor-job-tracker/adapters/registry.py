
from __future__ import annotations

from adapters.asml_global import ASMLGlobalAdapter
from adapters.base import BaseAdapter
from adapters.generic_playwright import GenericPlaywrightAdapter
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
    return GenericPlaywrightAdapter(company_cfg, source_cfg)
