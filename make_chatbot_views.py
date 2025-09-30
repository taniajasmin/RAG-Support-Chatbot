#!/usr/bin/env python3
# make_chatbot_views.py
#
# Build a minimal, chatbot-focused dataset from the full scrape outputs.
# Input:  data/pages.jsonl, data/chunks.jsonl  (produced by scrape_zirmon.py)
# Output: chatbot/chunks.jsonl (+ content_hash), chatbot/state.json,
#         chatbot/structured/{prices.json,contacts.json,locations.json,teams.json}

import os
import re
import json
import hashlib
import argparse
from datetime import datetime
from typing import Iterable, Dict, Any, List, Tuple

# --------------------------- IO helpers ---------------------------

def ensure_dir(p: str) -> None:
    os.makedirs(p, exist_ok=True)

def iter_jsonl(path: str) -> Iterable[Dict[str, Any]]:
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                yield json.loads(line)

def dump_json(path: str, obj: Any) -> None:
    ensure_dir(os.path.dirname(path))
    with open(path, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2)

def write_jsonl(path: str, records: Iterable[Dict[str, Any]]) -> None:
    ensure_dir(os.path.dirname(path))
    with open(path, "w", encoding="utf-8") as f:
        for r in records:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")

def content_hash(text: str) -> str:
    return hashlib.sha1(text.encode("utf-8")).hexdigest()

# --------------------------- Markdown parsing ---------------------------

HEADING_LINE = re.compile(r"^(#{1,6})\s+(.*)$")  # captures level + title

def parse_markdown_blocks(md: str) -> Iterable[Tuple[str, Any]]:
    """
    Yields a stream of ('heading', level:int, title:str) and ('para', text:str) blocks.
    Preserves order; collapses blank lines between paragraphs.
    """
    lines = [l.rstrip() for l in md.splitlines()]
    buf: List[str] = []
    for ln in lines:
        m = HEADING_LINE.match(ln)
        if m:
            if buf:
                text = "\n".join(buf).strip()
                if text:
                    yield ("para", text)
                buf = []
            level = len(m.group(1))
            title = m.group(2).strip()
            yield ("heading", level, title)
        else:
            if ln.strip() == "":
                if buf:
                    text = "\n".join(buf).strip()
                    if text:
                        yield ("para", text)
                    buf = []
            else:
                buf.append(ln)
    if buf:
        text = "\n".join(buf).strip()
        if text:
            yield ("para", text)

# --------------------------- Minimal chunks & state ---------------------------

def build_min_chunks(chunks_path: str, out_path: str, state_path: str) -> int:
    state: Dict[str, str] = {}
    minrecs: List[Dict[str, Any]] = []
    for rec in iter_jsonl(chunks_path):
        minrec = {
            "id": rec["id"],
            "source_url": rec["source_url"],
            "page_title": rec.get("page_title") or "",
            "chunk_index": rec["chunk_index"],
            "text": rec["text"],
            "published_at": rec.get("published_at"),
            "updated_at": rec.get("updated_at"),
            "retrieved_at": rec.get("retrieved_at"),
            "metadata": {
                "h1": rec.get("metadata", {}).get("h1", []),
                "h2": rec.get("metadata", {}).get("h2", []),
                "h3": rec.get("metadata", {}).get("h3", []),
                "meta_description": rec.get("metadata", {}).get("meta_description") or "",
            },
        }
        h = content_hash(minrec["text"])
        minrec["content_hash"] = h
        state[minrec["id"]] = h
        minrecs.append(minrec)

    write_jsonl(out_path, minrecs)
    dump_json(state_path, state)
    return len(minrecs)

# --------------------------- Structured extraction (Zirmon-tuned) ---------------------------

# Price/lead/unit detection (case-insensitive)
CURRENCY_RX = re.compile(r"(?i)\b(?:idr|rp\.?)\s*([0-9][\d\.\,]*)")
UNIT_RX     = re.compile(r"(?i)/(?:\s*)(unit|units|implant|cervical|first\s*\d+\s*(?:units|cervical))\b")
LEAD_RX     = re.compile(r"(?i)\b(\d+\s*-\s*\d+|\d+)\s*working\s*days\b")

# Headings that are generic section titles (not service names)
GENERIC_HEADINGS_RX = re.compile(
    r"(?i)\b("
    r"pricing|our pricing|portfolio|our portfolio|team|our team|about|about us|"
    r"core values|core|loyalty|warranty|contact|get in touch|"
    r"excellence|collaboration|maximize benefit|doctor|zirmon dental atelier"
    r")\b"
)

def _is_price_string(s: str) -> bool:
    return CURRENCY_RX.search(s) is not None

def _lead_from_text(s: str) -> str:
    m = LEAD_RX.search(s or "")
    return m.group(1).replace(" ", "") if m else ""

def _unit_from_text(s: str) -> str:
    m = UNIT_RX.search(s or "")
    return (m.group(1) or "").strip().lower() if m else ""

def _price_from_text(s: str) -> Tuple[int, str]:
    m = CURRENCY_RX.search(s or "")
    if not m:
        return (0, "")
    raw = m.group(1)
    try:
        val = int(re.sub(r"[^\d]", "", raw))
    except Exception:
        val = 0
    return (val, m.group(0))

def extract_prices(pages_iter: Iterable[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Extracts structured price entries from markdown that uses headings for both
    service names and price/lead lines (as seen on zirmon.com).
    """
    prices: List[Dict[str, Any]] = []

    def add_price(service: str, txt: str, url: str, pending_lead: str = "", notes: str = "", suffix: str = ""):
        if not service or not txt:
            return
        amount, price_raw = _price_from_text(txt)
        if not amount:
            return
        unit = _unit_from_text(txt)
        lead = _lead_from_text(txt) or (pending_lead or "")
        name = service.strip()
        if suffix:
            name = f"{name} ({suffix})"
        entry = {
            "service": name[:100],
            "price": amount,
            "price_raw": price_raw,
            "currency": "IDR",
            "unit": unit or None,
            "lead_time": lead or None,
            "notes": (notes or "").strip()[:300] or None,
            "source_url": url,
        }
        prices.append(entry)

    for page in pages_iter:
        url = page.get("canonical_url", page.get("url"))
        md  = page.get("markdown") or ""
        if not md:
            continue

        current_service = None
        pending_notes: List[str] = []
        pending_lead = ""
        additional_flag = False  # set when we see a heading literally "additional"

        for block in parse_markdown_blocks(md):
            if block[0] == "heading":
                _, level, title = block
                title_clean = re.sub(r"[*_`]+", "", title).strip()

                # Lead-time-only headings (e.g., "#### 3-5 WORKING DAYS")
                lead_val = _lead_from_text(title_clean)
                if lead_val:
                    pending_lead = lead_val
                    continue

                # Price headings (e.g., "#### IDR 1.350.000,- / UNIT")
                if _is_price_string(title_clean):
                    add_price(
                        current_service,
                        title_clean,
                        url,
                        pending_lead=pending_lead,
                        notes="; ".join(pending_notes),
                        suffix=("additional" if additional_flag else "")
                    )
                    # After capturing a price, reset contextual flags (keep service)
                    pending_notes = []
                    additional_flag = False
                    continue

                # Literal "additional" blocks toggle suffix for next price
                if title_clean.lower().strip() == "additional":
                    additional_flag = True
                    continue

                # Consider product/service headings mainly at deeper levels (h3+)
                if level >= 3 and not GENERIC_HEADINGS_RX.search(title_clean):
                    current_service = title_clean
                    pending_notes = []
                    pending_lead = ""  # reset when a new service starts
                    additional_flag = False
                    continue

                # Ignore generic headings (h1/h2 sections, etc.)
                continue

            # Paragraph block
            if block[0] == "para":
                txt = block[1]
                # Lead time present inside paragraphs
                lead_val = _lead_from_text(txt)
                if lead_val:
                    pending_lead = lead_val

                # Price lines inside paragraphs (rare on this site but supported)
                if _is_price_string(txt):
                    add_price(
                        current_service,
                        txt,
                        url,
                        pending_lead=pending_lead,
                        notes="; ".join(pending_notes),
                        suffix=("additional" if additional_flag else "")
                    )
                    pending_notes = []
                    additional_flag = False
                else:
                    # Accumulate brief context as notes
                    if current_service:
                        snippet = re.sub(r"\s+", " ", txt).strip()
                        if snippet:
                            pending_notes.append(snippet)

    # Deduplicate identical (service, price, unit, source_url)
    seen = set()
    out: List[Dict[str, Any]] = []
    for p in prices:
        key = (p["service"], p["price"], p.get("unit"), p["source_url"])
        if key in seen:
            continue
        seen.add(key)
        out.append(p)
    return out

def extract_contacts_locations(pages_iter: Iterable[Dict[str, Any]]) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    contacts: List[Dict[str, Any]] = []
    locations: List[Dict[str, Any]] = []

    CITY_HEADS = re.compile(r"(?i)^zirmon\s+(medan|bali|jakarta)\b")

    for page in pages_iter:
        url = page.get("canonical_url", page.get("url"))
        md  = page.get("markdown") or ""
        if not md:
            continue

        last_heading = ""
        capture_city = ""  # if non-empty, next 1–3 lines form address
        lines = md.splitlines()

        for i, ln in enumerate(lines):
            m = HEADING_LINE.match(ln)
            if m:
                title = m.group(2).strip()
                last_heading = title

                # city blocks like "### ZIRMON MEDAN"
                if CITY_HEADS.search(title):
                    capture_city = re.sub(r"[*_`]+", "", title).strip()
                else:
                    capture_city = ""
                continue

            # phone numbers (loose but effective)
            for ph in re.findall(r"(?:\+?\d[\d\s\-]{7,}\d)", ln):
                contacts.append({
                    "label": (last_heading or "Phone").strip(),
                    "phone": re.sub(r"\s+", "", ph),
                    "context": lines[i-1].strip() if i > 0 else "",
                    "source_url": url,
                })

            # address lines right after a city heading (1–3 lines until blank)
            if capture_city and ln.strip():
                addr_lines = [ln.strip()]
                for j in range(1, 3):
                    if i + j < len(lines) and lines[i + j].strip():
                        addr_lines.append(lines[i + j].strip())
                    else:
                        break
                locations.append({
                    "location": capture_city,
                    "address": " ".join(addr_lines),
                    "source_url": url,
                })
                capture_city = ""  # capture once per block

    # Dedup
    def dedup(items, keyfn):
        seen, out = set(), []
        for it in items:
            k = keyfn(it)
            if k in seen:
                continue
            seen.add(k)
            out.append(it)
        return out

    contacts  = dedup(contacts,  lambda c: (c["label"], c["phone"], c["source_url"]))
    locations = dedup(locations, lambda l: (l["location"], l["address"], l["source_url"]))
    return contacts, locations

def extract_teams(pages_iter: Iterable[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Extract the named ZIRMON teams with short blurbs.
    Only capture known team names to avoid false positives.
    """
    TEAM_NAMES = {"EXCEL", "MARVEL", "FASCINA", "ADMIN", "BALI", "JAKARTA", "MAGNI"}
    teams: List[Dict[str, Any]] = []

    for page in pages_iter:
        url = page.get("canonical_url", page.get("url"))
        md  = page.get("markdown") or ""
        if not md:
            continue

        cur_team = ""
        blurb: List[str] = []

        for block in parse_markdown_blocks(md):
            if block[0] == "heading":
                # flush previous
                if cur_team:
                    teams.append({
                        "team": cur_team,
                        "blurb": " ".join(blurb)[:500],
                        "source_url": url,
                    })
                    cur_team, blurb = "", []

                _, _, title = block
                title_clean = re.sub(r"[*_`]+", "", title).strip()
                m = re.search(r"(?i)\bzirmon\s+([A-Za-z]+)", title_clean)
                if m:
                    name = m.group(1).upper()
                    if name in TEAM_NAMES:
                        cur_team = f"ZIRMON {name}"
                        continue

            elif block[0] == "para":
                if cur_team:
                    txt = re.sub(r"\s+", " ", block[1]).strip()
                    if txt:
                        blurb.append(txt)

        if cur_team:
            teams.append({
                "team": cur_team,
                "blurb": " ".join(blurb)[:500],
                "source_url": url,
            })

    # Dedup by (team, source_url)
    seen, out = set(), []
    for t in teams:
        k = (t["team"], t["source_url"])
        if k in seen:
            continue
        seen.add(k)
        out.append(t)
    return out

# --------------------------- Main ---------------------------

def main():
    ap = argparse.ArgumentParser(description="Build chatbot-focused outputs from full scrape")
    ap.add_argument("--data", default="data", help="Input folder from scraper")
    ap.add_argument("--out", default="chatbot", help="Output folder for chatbot-ready data")
    args = ap.parse_args()

    pages_path  = os.path.join(args.data, "pages.jsonl")
    chunks_path = os.path.join(args.data, "chunks.jsonl")

    min_chunks_path = os.path.join(args.out, "chunks.jsonl")
    state_path      = os.path.join(args.out, "state.json")
    struct_dir      = os.path.join(args.out, "structured")

    # 1) Minimal chunks + state (for embeddings + fast upserts)
    n = build_min_chunks(chunks_path, min_chunks_path, state_path)
    print(f"[✓] wrote {n} minimal chunks -> {min_chunks_path}")
    print(f"[✓] wrote content-hash state -> {state_path}")

    # 2) Structured facts (prices, contacts, locations, teams)
    pages = list(iter_jsonl(pages_path))

    prices = extract_prices(pages)
    dump_json(os.path.join(struct_dir, "prices.json"), prices)
    print(f"[✓] prices -> {os.path.join(struct_dir, 'prices.json')} ({len(prices)})")

    contacts, locations = extract_contacts_locations(pages)
    dump_json(os.path.join(struct_dir, "contacts.json"), contacts)
    dump_json(os.path.join(struct_dir, "locations.json"), locations)
    print(f"[✓] contacts -> {os.path.join(struct_dir, 'contacts.json')} ({len(contacts)})")
    print(f"[✓] locations -> {os.path.join(struct_dir, 'locations.json')} ({len(locations)})")

    teams = extract_teams(pages)
    dump_json(os.path.join(struct_dir, "teams.json"), teams)
    print(f"[✓] teams -> {os.path.join(struct_dir, 'teams.json')} ({len(teams)})")

if __name__ == "__main__":
    main()