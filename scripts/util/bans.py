# -*- coding: utf-8 -*-
import re

BANNED_KEYWORDS = [
    "casino", "gambl", "betting", "wager",
    "tobacco", "cigarette", "e-cig", "vape",
    "alcohol", "brew", "distill", "spirits", "winery",
    "weapon", "firearm", "ammunition", "defense",
    "adult", "porn", "sex", "escort",
    "payday", "loan shark",
    "insur", "bank", "financial", "lending", "credit"
]

def is_banned_by_sic(sic_str: str, sic_desc: str) -> bool:
    # Finance/Insurance/RE: 6000-6999
    try:
        sic = int(sic_str) if sic_str else -1
    except ValueError:
        sic = -1
    if 6000 <= sic <= 6999:
        return True

    d = (sic_desc or "").lower()
    if any(k in d for k in ["casino", "tobacco", "cigarette", "brew", "distill", "spirits", "weapon", "defense", "adult"]):
        return True
    return False

def is_banned_by_keywords(company: str) -> bool:
    c = (company or "").lower()
    return any(k in c for k in BANNED_KEYWORDS)

def is_banned(company: str, sic: str, sic_desc: str) -> bool:
    return is_banned_by_sic(sic, sic_desc) or is_banned_by_keywords(company)
