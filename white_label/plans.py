from __future__ import annotations

from . import WL_PLAN_BASIC, WL_PLAN_COMMUNITY, WL_PLAN_MANUAL, WL_PLAN_PLUS, WL_PLAN_PRO, WL_PLAN_TRIAL


PLAN_FEATURE_PRIVATE_SEARCH = "private_search"
PLAN_FEATURE_PDF_DELIVERY = "pdf_delivery"
PLAN_FEATURE_INLINE_SEARCH = "inline_search"
PLAN_FEATURE_GUEST_MODE = "guest_mode"
PLAN_FEATURE_CUSTOM_BRANDING = "custom_branding"


PLAN_CONFIG: dict[str, dict[str, object]] = {
    WL_PLAN_TRIAL: {
        "label": "Trial",
        "daily_search_limit": 100,
        "daily_send_limit": 20,
        "per_minute_send_limit": 10,
        "search_results_limit": 10,
        "features": {
            PLAN_FEATURE_PRIVATE_SEARCH,
            PLAN_FEATURE_PDF_DELIVERY,
            PLAN_FEATURE_INLINE_SEARCH,
            PLAN_FEATURE_GUEST_MODE,
        },
    },
    WL_PLAN_BASIC: {
        "label": "Basic",
        "daily_search_limit": 1000,
        "daily_send_limit": 100,
        "per_minute_send_limit": 30,
        "search_results_limit": 10,
        "features": {
            PLAN_FEATURE_PRIVATE_SEARCH,
            PLAN_FEATURE_PDF_DELIVERY,
        },
    },
    WL_PLAN_PRO: {
        "label": "Pro",
        "daily_search_limit": 5000,
        "daily_send_limit": 500,
        "per_minute_send_limit": 60,
        "search_results_limit": 10,
        "features": {
            PLAN_FEATURE_PRIVATE_SEARCH,
            PLAN_FEATURE_PDF_DELIVERY,
            PLAN_FEATURE_INLINE_SEARCH,
            PLAN_FEATURE_GUEST_MODE,
            PLAN_FEATURE_CUSTOM_BRANDING,
        },
    },
    WL_PLAN_COMMUNITY: {
        "label": "Community",
        "daily_search_limit": 20000,
        "daily_send_limit": 2000,
        "per_minute_send_limit": 120,
        "search_results_limit": 10,
        "features": {
            PLAN_FEATURE_PRIVATE_SEARCH,
            PLAN_FEATURE_PDF_DELIVERY,
            PLAN_FEATURE_INLINE_SEARCH,
            PLAN_FEATURE_GUEST_MODE,
            PLAN_FEATURE_CUSTOM_BRANDING,
        },
    },
    WL_PLAN_MANUAL: {
        "label": "Manual",
        "daily_search_limit": 1000,
        "daily_send_limit": 100,
        "per_minute_send_limit": 10,
        "search_results_limit": 5,
        "features": {
            PLAN_FEATURE_PRIVATE_SEARCH,
            PLAN_FEATURE_PDF_DELIVERY,
        },
    },
}


def normalize_plan(raw: str | None) -> str:
    text = str(raw or "").strip().upper()
    if text == WL_PLAN_PLUS:
        return WL_PLAN_PRO
    if text in {WL_PLAN_TRIAL, WL_PLAN_BASIC, WL_PLAN_PRO, WL_PLAN_COMMUNITY, WL_PLAN_MANUAL}:
        return text
    return WL_PLAN_MANUAL


def plan_config(raw: str | None) -> dict[str, object]:
    return PLAN_CONFIG.get(normalize_plan(raw), PLAN_CONFIG[WL_PLAN_MANUAL])


def plan_limit(raw: str | None, key: str, fallback: int = 0) -> int:
    try:
        return max(0, int(plan_config(raw).get(key, fallback) or 0))
    except Exception:
        return max(0, int(fallback or 0))


def plan_allows(raw: str | None, feature: str) -> bool:
    features = plan_config(raw).get("features") or set()
    return str(feature or "").strip() in features


def plan_feature_summary(raw: str | None) -> str:
    plan = normalize_plan(raw)
    if plan == WL_PLAN_TRIAL:
        return "3-day trial: 100 searches/day, 20 sends/day"
    if plan == WL_PLAN_BASIC:
        return "Private PDF search/send: 1000 searches/day, 100 sends/day"
    if plan == WL_PLAN_PRO:
        return "Inline, guest mode, custom branding: 5000 searches/day, 500 sends/day"
    if plan == WL_PLAN_COMMUNITY:
        return "Community scale: 20000 searches/day, 2000 sends/day"
    return "Private PDF search/send"
