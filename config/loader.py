from __future__ import annotations

from pathlib import Path
import yaml

from core.models import AppConfig, CompanyConfig, FilterConfig, RuntimeConfig, SourceConfig


def _parse_source(raw: dict) -> SourceConfig:
    return SourceConfig(
        name=raw["name"],
        url=raw["url"],
        source_type=raw["source_type"],
        parser=raw["parser"],
        enabled=raw.get("enabled", True),
        region=raw.get("region", ""),
        meta=raw.get("meta", {}),
    )


def load_config(path: Path) -> AppConfig:
    raw = yaml.safe_load(path.read_text(encoding="utf-8"))
    companies: list[CompanyConfig] = []
    for company in raw["companies"]:
        sources = [_parse_source(s) for s in company["sources"]]
        companies.append(CompanyConfig(name=company["name"], sources=sources))
    platform_sources = [_parse_source(s) for s in raw.get("platform_sources", [])]
    filters = FilterConfig(**raw["filters"])
    runtime = RuntimeConfig(**raw["runtime"])
    return AppConfig(companies=companies, platform_sources=platform_sources, filters=filters, runtime=runtime)
