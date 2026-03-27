
from __future__ import annotations

from pathlib import Path
import yaml

from core.models import AppConfig, CompanyConfig, FilterConfig, RuntimeConfig, SourceConfig


def load_config(path: Path) -> AppConfig:
    raw = yaml.safe_load(path.read_text(encoding="utf-8"))
    companies = []
    for company in raw["companies"]:
        sources = [
            SourceConfig(
                name=s["name"],
                url=s["url"],
                source_type=s["source_type"],
                parser=s["parser"],
                enabled=s.get("enabled", True),
                region=s["region"],
                meta=s.get("meta", {}),
            )
            for s in company["sources"]
        ]
        companies.append(CompanyConfig(name=company["name"], sources=sources))
    filters = FilterConfig(**raw["filters"])
    runtime = RuntimeConfig(**raw["runtime"])
    return AppConfig(companies=companies, filters=filters, runtime=runtime)
