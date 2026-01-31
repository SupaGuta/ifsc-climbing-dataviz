import re

# ---------- Anchors ----------
COUNTRY_RE = re.compile(r"\(\s*(?P<country>[A-Z]{3})\s*\)")
YEAR_RE    = re.compile(r"\b(?:19|20)\d{2}\b")

# Separators that introduce the location chunk.
# Handles " - Brno", " -Tokyo", ", Berkeley"
SEP_RE = re.compile(r"(?:\s-\s*|,\s*)")


def _last_match(regex: re.Pattern, s: str):
    """Return the last match of regex in s (or None)."""
    m = None
    for m in regex.finditer(s):
        pass
    return m


# ---------- City cleanup ----------
NOISE_TOKENS = [    
    r"\(\s*(?:L|S|B)\s*\)",
    r"rock\s*masters?",
    r"masters?",
    r"internationals?",
    r"youths?",
    r"opens?",
    r"boulder",
    r"speed",
    r"lead",
    r"color",
    r"climbing",
    r"festival",
    r"cup",
    r"EYOF",
    r"demonstration",
    r"european",
    r"days?",
    r"FISE",
    r"world",
    r"series?",
    r"int.",
    r"competitions?",
    r"junior",
    r"kids?",
    r"of",
    r"megusta",
    r"10th",
    r"anniversary"
]


ALT = "|".join(NOISE_TOKENS)

# One or more contiguous noise tokens separated by spaces
NOISE_SEQ = rf"(?:{ALT})(?:\s+(?:{ALT}))*"

# Noise at edges: (city + noise_seq) OR (noise_seq + city)
EDGE_RE = re.compile(
    rf"""^
    (?:
        (?P<city_left>.+?)\s+(?:{NOISE_SEQ})      # "Marseille International Youth"
      |
        (?:{NOISE_SEQ})\s+(?P<city_right>.+?)     # "Open Youth Marseille"
    )
    $""",
    re.IGNORECASE | re.VERBOSE
)

ONLY_NOISE_RE = re.compile(rf"^(?:{NOISE_SEQ})$", re.IGNORECASE)


def cleanup_city(city: str) -> str:
    """
    Normalize and clean a 'city chunk' that may contain event noise words.

    Returns:
        cleaned city string, or "" if it becomes empty / only noise.
    """
    c = " ".join(city.split()).strip().strip('"').strip()

    # Handle Citta'di Ravenna / Città di Ravenna -> Ravenna
    m = re.search(r"Citt[àa]['’]?\s*di\s*(.+)$", c, re.IGNORECASE)
    if m:
        c = m.group(1).strip().strip('"').strip()

    # Remove noise phrases at either edge, repeatedly if needed
    prev = None
    while c and c != prev:
        prev = c
        m = EDGE_RE.match(c)
        if not m:
            break
        c = (m.group("city_left") or m.group("city_right") or "").strip().strip('"').strip()

    # If what's left is only noise, treat as "no city"
    if not c or ONLY_NOISE_RE.match(c):
        return ""

    return c


# ---------- Main parser ----------
def parse_city_country(event_name: str):
    """
    Extract (city, country) from an event name string.

    Strategy:
      1) If a 3-letter country code "(AAA)" exists, use the LAST one as anchor.
         Extract city from the chunk right before that anchor, after the last '-' or ','.
         If no such separator exists, city is unknown -> None (but keep country).
      2) Else if a year exists, use the LAST year as anchor.
         Only infer city if an explicit separator '-' or ',' exists before the year.
      3) Otherwise return (None, None).

    Returns:
      (city: str|None, country: str|None)
    """
    s = " ".join(event_name.split())  # normalize whitespace

    # 1) Prefer last "(AAA)" anywhere
    m_country = _last_match(COUNTRY_RE, s)
    if m_country:
        country = m_country.group("country")
        left = s[:m_country.start()].rstrip()

        # Require an explicit delimiter before "(AAA)" to infer a city
        m_sep = _last_match(SEP_RE, left)
        if not m_sep:
            return None, country

        city_chunk = left[m_sep.end():].strip()
        city = cleanup_city(city_chunk)
        return (city or None), country

    # 2) No country: use last year anywhere
    m_year = _last_match(YEAR_RE, s)
    if m_year:
        left = s[:m_year.start()].rstrip()

        # Only infer city if explicitly delimited (dash or comma)
        m_sep = _last_match(SEP_RE, left)
        if not m_sep:
            return None, None

        city_chunk = left[m_sep.end():].strip()
        city = cleanup_city(city_chunk)
        return (city or None), None

    return None, None
