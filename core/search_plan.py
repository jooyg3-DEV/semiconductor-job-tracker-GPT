from __future__ import annotations

from collections import OrderedDict

DEFAULT_KEYWORDS = [
    "process engineer",
    "process support engineer",
    "field application engineer",
    "application engineer",
    "customer engineer",
    "metrology",
    "lithography",
    "deposition",
    "etch",
    "yield",
    "integration",
    "packaging",
    "manufacturing engineer",
    "production engineer",
    "module engineer",
    "quality engineer",
]

DEFAULT_REGIONS = [
    "Korea",
    "Hwaseong",
    "Pyeongtaek",
    "Hsinchu",
    "Linkou",
    "Taichung",
    "Austin",
    "Boise",
    "Singapore",
    "Japan",
]

COMPANY_KEYWORDS = {
    "ASML": [
        "field application engineer",
        "application engineer",
        "customer engineer",
        "process engineer",
        "metrology",
        "lithography",
        "deposition",
        "yield",
    ],
    "Applied Materials": [
        "process support engineer",
        "application engineer",
        "customer engineer",
        "field application engineer",
        "process engineer",
        "deposition",
        "metrology",
        "packaging",
    ],
    "Lam Research": [
        "process engineer",
        "process support engineer",
        "customer engineer",
        "field application engineer",
        "etch",
        "deposition",
        "yield",
        "integration",
    ],
    "KLA": [
        "application engineer",
        "field application engineer",
        "process engineer",
        "metrology",
        "yield",
        "integration",
        "quality engineer",
    ],
    "TEL": [
        "process engineer",
        "application engineer",
        "customer engineer",
        "field application engineer",
        "deposition",
        "etch",
        "packaging",
    ],
    "Micron": [
        "process engineer",
        "yield",
        "integration",
        "manufacturing engineer",
        "quality engineer",
        "packaging",
    ],
    "ASM": [
        "process engineer",
        "application engineer",
        "field application engineer",
        "deposition",
        "metrology",
        "manufacturing engineer",
    ],
    "TSMC": [
        "process engineer",
        "module engineer",
        "integration",
        "yield",
        "packaging",
        "manufacturing engineer",
    ],
    "NVIDIA": [
        "process engineer",
        "yield",
        "integration",
        "packaging",
        "quality engineer",
        "manufacturing engineer",
    ],
    "AMD": [
        "process engineer",
        "yield",
        "integration",
        "packaging",
        "quality engineer",
        "manufacturing engineer",
    ],
}

COMPANY_REGIONS = {
    "삼성전자DS": ["Korea", "Hwaseong", "Pyeongtaek", "Suwon", "Cheonan"],
    "SK하이닉스": ["Korea", "Icheon", "Cheongju", "Pangyo"],
    "ASML": ["Korea", "Hwaseong", "Pyeongtaek", "Hsinchu", "Linkou", "San Diego"],
    "Applied Materials": ["Korea", "Hwaseong", "Pyeongtaek", "Hsinchu", "Austin", "Singapore"],
    "KLA": ["Korea", "Hwaseong", "Pyeongtaek", "Hsinchu", "Milpitas", "Singapore"],
    "Lam Research": ["Korea", "Hwaseong", "Pyeongtaek", "Hsinchu", "Tualatin", "Singapore"],
    "TEL": ["Korea", "Hwaseong", "Pyeongtaek", "Japan", "Hsinchu"],
    "Micron": ["Boise", "Singapore", "Taichung", "Hsinchu", "Korea"],
    "ASM": ["Singapore", "Korea", "Phoenix", "Hsinchu"],
    "TSMC": ["Hsinchu", "Taichung", "Tainan", "Phoenix", "Japan"],
    "NVIDIA": ["Taiwan", "Hsinchu", "Korea", "Singapore", "Santa Clara"],
    "AMD": ["Korea", "Taiwan", "Austin", "Singapore"],
}

LINKEDIN_COMPANY_SLUGS = {
    "삼성전자DS": "samsung-electronics",
    "SK하이닉스": "sk-hynix",
    "ASML": "asml",
    "Applied Materials": "applied-materials",
    "KLA": "kla",
    "Lam Research": "lamresearch",
    "TEL": "tokyo-electron",
    "Micron": "micron-technology",
    "ASM": "asm-international",
    "TSMC": "tsmc",
    "NVIDIA": "nvidia",
    "AMD": "amd",
}


def _dedupe_keep_order(values: list[str]) -> list[str]:
    return list(OrderedDict.fromkeys(v for v in values if v))


def get_company_keywords(company_name: str) -> list[str]:
    return _dedupe_keep_order((COMPANY_KEYWORDS.get(company_name) or []) + DEFAULT_KEYWORDS)


def get_company_regions(company_name: str) -> list[str]:
    return _dedupe_keep_order((COMPANY_REGIONS.get(company_name) or []) + DEFAULT_REGIONS)


def get_search_plan(company_name: str) -> list[str]:
    return get_company_keywords(company_name)
