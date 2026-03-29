
from __future__ import annotations

from abc import ABC, abstractmethod

from core.models import CompanyConfig, JobRecord, SourceConfig


class BaseAdapter(ABC):
    def __init__(self, company_cfg: CompanyConfig, source_cfg: SourceConfig) -> None:
        self.company_cfg = company_cfg
        self.source_cfg = source_cfg

    @abstractmethod
    def fetch(self) -> list[JobRecord]:
        raise NotImplementedError
