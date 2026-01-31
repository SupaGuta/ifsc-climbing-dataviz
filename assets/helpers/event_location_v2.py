'''
Extract (city, country) from IFSC event "name" strings.

Design goals:
- Conservative: do NOT guess. If city/country can't be confidently parsed from the string, return None.
- More robust than the original version:
  - supports separators like "(B)- Munich" or "Cup- Kitzbuhel"
  - supports malformed country codes like "CHN)" (missing "(")
  - supports country names in parentheses like "(France)" or "(New Caledonia)"
  - avoids removing legitimate cities such as "Boulder"
'''

from __future__ import annotations

import re
from typing import Optional, Tuple

try:
    import pycountry  # type: ignore
except Exception:  # pragma: no cover
    pycountry = None


def _normalize_spaces(s: str) -> str:
    return " ".join(str(s).split())


# ---- Anchors (country / year) -------------------------------------------------

COUNTRY_PAREN_RE = re.compile(r"\(\s*(?P<country>[A-Z]{3})\s*\)")
# e.g. "... Chengdu CHN)"  (missing "(")
COUNTRY_RPAREN_RE = re.compile(r"(?<!\()(?P<country>[A-Z]{3})\s*\)")

YEAR_RE = re.compile(r"\b(?:19|20)\d{2}\b")
PAREN_RE = re.compile(r"\(([^()]*)\)")


def _country_name_to_alpha3(name: str) -> Optional[str]:
    """Map a country name found in the string to ISO alpha-3 (e.g. France -> FRA)."""
    if not pycountry:
        # minimal fallback for common cases seen in the dataset
        fallback = {
            "australia": "AUS",
            "austria": "AUT",
            "belgium": "BEL",
            "brazil": "BRA",
            "canada": "CAN",
            "china": "CHN",
            "czech republic": "CZE",
            "czechia": "CZE",
            "england": "GBR",
            "france": "FRA",
            "germany": "DEU",
            "great britain": "GBR",
            "hong kong": "HKG",
            "india": "IND",
            "indonesia": "IDN",
            "israel": "ISR",
            "italy": "ITA",
            "japan": "JPN",
            "mexico": "MEX",
            "morocco": "MAR",
            "netherlands": "NLD",
            "new caledonia": "NCL",
            "new zealand": "NZL",
            "peru": "PER",
            "portugal": "PRT",
            "qatar": "QAT",
            "republic of korea": "KOR",
            "russia": "RUS",
            "scotland": "GBR",
            "slovenia": "SVN",
            "south africa": "ZAF",
            "spain": "ESP",
            "switzerland": "CHE",
            "tunisia": "TUN",
            "uae": "ARE",
            "united arab emirates": "ARE",
            "united kingdom": "GBR",
            "united states": "USA",
            "united states of america": "USA",
            "usa": "USA",
            "wales": "GBR",
        }
        return fallback.get(name.strip().lower())

    try:
        c = pycountry.countries.lookup(name.strip())
        return getattr(c, "alpha_3", None)
    except Exception:
        return None


def _extract_country(s: str) -> Tuple[Optional[str], Optional[int], Optional[int]]:
    """
    Returns (country_alpha3, start_idx, end_idx) where start/end are the match span in s.
    If no country found: (None, None, None)
    """
    # Prefer well-formed "(AAA)"
    m = None
    for m in COUNTRY_PAREN_RE.finditer(s):
        pass
    if m:
        return m.group("country"), m.start(), m.end()

    # Malformed "AAA)" (missing '(')
    m2 = None
    for m2 in COUNTRY_RPAREN_RE.finditer(s):
        pass
    if m2:
        return m2.group("country"), m2.start(), m2.end()

    # Country names in parentheses (take the last one that maps to an ISO code)
    parens = list(PAREN_RE.finditer(s))
    for pm in reversed(parens):
        content = pm.group(1).strip()

        # Skip discipline markers
        if re.fullmatch(r"[LSB](?:\s*,\s*[LSB])+", content, flags=re.I) or re.fullmatch(
            r"[LSB]", content, flags=re.I
        ):
            continue

        # Skip 3-letter codes (handled above)
        if re.fullmatch(r"[A-Z]{3}", content):
            continue

        alpha3 = _country_name_to_alpha3(content)
        if alpha3:
            return alpha3, pm.start(), pm.end()

    return None, None, None


# ---- City extraction / cleanup ------------------------------------------------

# separators: comma or hyphen followed by whitespace (covers " - ", "(B)- Munich", "Cup- Kitzbuhel")
SEP_RE = re.compile(r"(?:,\s*|-\s+)")
DISC_RE = re.compile(r"\(\s*(?:L|S|B|lead|speed|boulder)(?:\s*,\s*(?:L|S|B|lead|speed|boulder))*\s*\)", re.I)
DISC_MAL_RE = re.compile(r"\b[LSB]\)", re.I)

# Regions that can appear between event name and city in "X Games Asia Shanghai" patterns
REGION_WORDS_STRICT = {"asia", "europe", "africa", "oceania", "nordic", "pan", "pan-am", "panam", "panamerican"}

# If the extracted "city" equals one of these, it's (in practice) an event brand/name, not a location.
# (Example: Melloblocco is a bouldering gathering in Val Masino / Val di Mello, not a city.)
KNOWN_NOT_CITY = {"melloblocco"}


def _trim_leading_regions(s: str) -> str:
    toks = s.split()
    while len(toks) > 1 and toks[0].strip(".,;:").lower() in REGION_WORDS_STRICT:
        toks = toks[1:]
    return " ".join(toks)


def _strip_trailing_country_from_city(chunk: str) -> str:
    """Remove trailing '(AAA)' or '(Country Name)' or ' AAA)' at the end of a city chunk."""
    c = chunk.strip()

    m = re.search(r"\s*\(([^()]*)\)\s*$", c)
    if m:
        content = m.group(1).strip()
        if re.fullmatch(r"[A-Z]{3}", content):
            c = c[: m.start()].rstrip()
        else:
            alpha3 = _country_name_to_alpha3(content)
            if alpha3:
                c = c[: m.start()].rstrip()

    # malformed trailing " AAA)"
    c = re.sub(r"\s+[A-Z]{3}\)\s*$", "", c)
    return c.strip()


def _internal_location_suffix(chunk: str) -> str:
    """
    If chunk still contains event keywords, try to keep only the suffix that looks like the location.
    Examples:
      - "Asia Cup Uttarkashi" -> "Uttarkashi"
      - "Boulder Masters Grenoble" -> "Grenoble"
      - "Boulder Montpellier" -> "Montpellier"

    Conservative: if we can't produce a non-empty suffix, keep the original chunk.
    """
    toks = chunk.split()
    if len(toks) < 2:
        return chunk

    # acceptable keywords to cut after
    acceptable = {
        "cup", "copa", "festival", "contest", "open", "trophy", "series", "games", "game",
        "championship", "championships", "bouldering", "climbing",
        "boulder", "speed", "master", "masters", "rockmaster", "rockmasters", "rockstars",
    }

    norms = [t.strip(".,;:").lower() for t in toks]
    last_pos = None
    last_kw = None
    for i, n in enumerate(norms):
        if n in acceptable:
            last_pos = i
            last_kw = n

    if last_pos is None:
        return chunk

    start = last_pos + 1

    # special for "copa <name> <city>" -> skip also <name>
    if last_kw == "copa" and start < len(toks) - 1:
        start += 1

    if start >= len(toks):
        return chunk

    out = " ".join(toks[start:]).strip()
    out = _trim_leading_regions(out)
    return out or chunk


def _cleanup_city(city_chunk: str) -> str:
    c = _normalize_spaces(city_chunk).strip().strip('"').strip()
    c = c.replace("–", "-").replace("—", "-").replace("−", "-")

    # low-risk phrase removals (seen in this dataset)
    c = re.sub(r"^\s*the\s+rock\s+", "", c, flags=re.I)
    c = re.sub(r"\bnatural\s+games?\b", "", c, flags=re.I)
    c = re.sub(r"\bat\s+sea\b", "", c, flags=re.I)
    c = re.sub(r"^\s*speed\s+rock\s+", "", c, flags=re.I)
    c = re.sub(r"^\s*boulder\s+masters?\s+", "", c, flags=re.I)
    c = re.sub(r"\s+x-?games?\s*$", "", c, flags=re.I)

    # remove ordinals like "10th"
    c = re.sub(r"\b\d+(?:st|nd|rd|th)\b", "", c, flags=re.I)

    # remove discipline markers anywhere
    c = DISC_RE.sub(" ", c)
    c = DISC_MAL_RE.sub(" ", c)

    # remove embedded years
    c = YEAR_RE.sub(" ", c)

    # Ravenna special case (kept from your original script idea)
    c = re.sub(r"Citt[àa]['’]?\s*di\s*", "", c, flags=re.I)

    # remove trailing country info
    c = _strip_trailing_country_from_city(c)

    # if leftover "- " segments exist inside the chunk, keep only the last segment
    if re.search(r"-\s+", c):
        parts = re.split(r"-\s+", c)
        for part in reversed([p.strip() for p in parts]):
            if part:
                c = part
                break

    # remove leading ordinals like "3."
    c = re.sub(r"^\s*\d+\s*[\.\)]\s*", "", c)

    # remove facility prefixes like "AREA 47"
    c = re.sub(r"^\s*AREA\s*\d+\s+", "", c, flags=re.I)

    c = _normalize_spaces(c)
    c = _internal_location_suffix(c)
    c = _normalize_spaces(c)

    # final trims
    c = c.strip(" ,;-")
    c = _trim_leading_regions(c)

    if not c:
        return ""

    # must contain at least one letter
    if not re.search(r"[A-Za-zÀ-ÖØ-öø-ÿ]", c):
        return ""

    # avoid absurdly long leftovers
    if len(c) > 60 or len(c.split()) > 8:
        return ""

    # normalize ALL CAPS to Title Case (purely cosmetic)
    if c.isupper() and len(c) > 3:
        c = c.title()

    # suppress known event names that are not locations
    if c.strip().lower() in KNOWN_NOT_CITY:
        return ""

    return c


# ---- Fallback (when no explicit separator) -----------------------------------

STOP_KEYWORDS = {
    # Acceptable "event-type" words that often appear right before a location suffix
    "cup", "copa", "festival", "contest", "open", "trophy", "series", "games", "game",
    "championship", "championships", "bouldering", "climbing", "bloc", "rockmaster", "rockmasters", "rockstars",
    # Not acceptable (too generic): used as a stop, but we won't accept the suffix if we stopped on these
    "event", "promo", "promotional",
}
ACCEPTABLE_STOPS = {
    "cup", "copa", "festival", "contest", "open", "trophy", "series", "games", "game",
    "championship", "championships", "bouldering", "climbing", "bloc", "rockmaster", "rockmasters", "rockstars",
}


def _suffix_city_from_left(left: str) -> str:
    """
    When there's no explicit delimiter, try to take the suffix after a stop keyword near the end:
      "Youth Color Climbing Festival Imst" -> "Imst"
      "Tout a Bloc l'Argentière la Bessée" -> "l'Argentière la Bessée"

    Conservative: only accept if the stop keyword is in ACCEPTABLE_STOPS.
    """
    toks = left.split()
    if not toks:
        return ""

    suffix = []
    stop = None
    for tok in reversed(toks):
        t = tok.strip(".,;:").lower()
        if t in STOP_KEYWORDS:
            stop = t
            break
        suffix.append(tok)

    if stop is None:
        return ""
    if stop not in ACCEPTABLE_STOPS:
        return ""

    return " ".join(reversed(suffix)).strip()


# ---- City/Country split from location chunks ---------------------------------

def _split_city_and_country(chunk: str) -> Tuple[str, Optional[str]]:
    """
    Split a location chunk into (city, country_alpha3) when the country is explicit.
    Returns the (possibly empty) city string and the country code (or None).
    """
    if not chunk:
        return "", None

    raw = _normalize_spaces(chunk).strip().strip(",")
    if not raw:
        return "", None

    # comma-separated "City, Country" or "City, USA"
    if "," in raw:
        parts = [p.strip() for p in raw.split(",") if p.strip()]
        if len(parts) >= 2:
            country_candidate = parts[-1]
            alpha3 = _country_name_to_alpha3(country_candidate)
            if not alpha3 and re.fullmatch(r"[A-Z]{3}", country_candidate):
                alpha3 = country_candidate
            if alpha3:
                city_part = ", ".join(parts[:-1]).strip()
                return city_part, alpha3

    # trailing country code "City USA"
    m = re.search(r"\b(?P<country>[A-Z]{3})\b$", raw)
    if m:
        country = m.group("country")
        city_part = raw[: m.start()].strip(" ,;-")
        return city_part, country

    # full chunk equals a country name
    alpha3 = _country_name_to_alpha3(raw)
    if alpha3:
        return "", alpha3

    # try trailing country name without comma: "Hong Kong China"
    toks = raw.split()
    for i in range(1, min(4, len(toks)) + 1):
        candidate = " ".join(toks[-i:])
        alpha3 = _country_name_to_alpha3(candidate)
        if alpha3:
            city_part = " ".join(toks[:-i]).strip()
            return city_part, alpha3

    return raw, None


def _city_chunk_from_left(left: str) -> str:
    seps = list(SEP_RE.finditer(left))
    if not seps:
        return _suffix_city_from_left(left)

    for i in range(len(seps) - 1, -1, -1):
        chunk = left[seps[i].end():].strip()
        if not chunk:
            continue
        city_part, country_part = _split_city_and_country(chunk)
        sep_text = seps[i].group(0)
        if city_part or not country_part or not sep_text.startswith(","):
            return chunk
        if i == 0:
            return left

    return ""


# ---- Public API ---------------------------------------------------------------

def parse_city_country(name: str) -> Tuple[Optional[str], Optional[str]]:
    """
    Parse an event name and return (city, country_alpha3).

    - country_alpha3 is an ISO-3166 alpha-3 code (e.g. FRA, USA, JPN)
    - city is the best-effort extracted location string
    - returns None for city and/or country when extraction is not possible without guessing
    """
    s = _normalize_spaces(name)

    country, c_start, _ = _extract_country(s)

    # Anchor on country when present
    if country and c_start is not None:
        left = s[:c_start].rstrip().rstrip(" ,-")
        city_chunk = _city_chunk_from_left(left)
        city_raw, _ = _split_city_and_country(city_chunk)
        city = _cleanup_city(city_raw) if city_raw else ""
        return (city or None), country

    # Otherwise anchor on the last year
    m_year = None
    for m_year in YEAR_RE.finditer(s):
        pass

    if m_year:
        left = s[:m_year.start()].rstrip().rstrip(" ,-")
        city_chunk = _city_chunk_from_left(left)
        if not city_chunk:
            _, country_from_chunk = _split_city_and_country(left)
            if country_from_chunk:
                return None, country_from_chunk

        city_raw, country_from_chunk = _split_city_and_country(city_chunk)
        city = _cleanup_city(city_raw) if city_raw else ""
        return (city or None), country_from_chunk

    return None, None
