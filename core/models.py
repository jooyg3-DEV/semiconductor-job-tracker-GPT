from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from core.utils import infer_region_from_location


@dataclass
class SourceConfig:
    name: str
    url: str
    source_type: str
    parser: str
    enabled: bool
    region: str
    meta: dict[str, Any] = field(default_factory=dict)


@dataclass
class CompanyConfig:
    name: str
    sources: list[SourceConfig]


@dataclass
class FilterConfig:
    include_keywords: list[str]
    exclude_keywords: list[str]


@dataclass
class RuntimeConfig:
    timezone: str
    miss_threshold: int


@dataclass
class AppConfig:
    companies: list[CompanyConfig]
    platform_sources: list[SourceConfig]
    filters: FilterConfig
    runtime: RuntimeConfig


@dataclass
class JobRecord:
    company: str
    region: str
    source: str
    title: str
    url: str
    deadline: str
    qualification: str
    job_function: str
    location: str
    employment_type: str
    experience_flag: str = "N"
    masters_flag: str = "N"
    phd_flag: str = "N"
    job_id: str = ""
    raw_text: str = ""
    metadata: dict[str, Any] = field(default_factory=dict)

    @property
    def effective_region(self) -> str:
        return infer_region_from_location(self.location, self.region)

    @property
    def sheet_key(self) -> str:
        return self.company

    @property
    def unique_key(self) -> str:
        return self.job_id.strip() or self.url.strip()

    def to_row(self, today_str: str) -> list[str]:
        return [
            today_str,
            self.source,
            self.deadline or "없음",
            self.company,
            self.title,
            self.qualification or "",
            self.job_function or "",
            self.location or "",
            self.employment_type or "",
            self.experience_flag or "N",
            self.masters_flag or "N",
            self.phd_flag or "N",
            self.url,
        ]
