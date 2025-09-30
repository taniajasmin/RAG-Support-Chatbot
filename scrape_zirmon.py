import argparse
import csv
import hashlib
import io
import json
import os
import queue
import re
import time
import urllib.parse as up
import urllib.robotparser as robotparser
from collections import defaultdict
from datetime import datetime
from typing import Dict, List, Optional, Set, Tuple

import requests
from bs4 import BeautifulSoup, Comment
from dateutil import parser as dateparser
from markdownify import markdownify as html2md
from tqdm import tqdm
import tldextract  # for safe same-domain checks

DEFAULT_UA = "Mozilla/5.0 (compatible; RAG-Scraper/1.0; +https://example.com/bot)"
SESSION = requests.Session()
ADAPT = requests.adapters.HTTPAdapter(max_retries=3, pool_connections=8, pool_maxsize=8)
SESSION.mount("http://", ADAPT)
SESSION.mount("https://", ADAPT)
SESSION.headers.update({"User-Agent": DEFAULT_UA, "Accept": "text/html,application/xhtml+xml"})

DATE_META_KEYS = [
    "article:published_time", "article:modified_time", "og:updated_time",
    "date", "dc.date", "dc.date.issued", "dc.date.modified",
    "last-modified", "revise", "publish_date", "pubdate",
]

BLOCK_TAGS = {"script", "style", "noscript", "svg", "iframe", "canvas", "form", "footer", "header", "nav"}

def norm_url(url: str) -> str:
    """Normalize URL to canonical-ish form for de-duplication."""
    u = up.urlsplit(url)
    # drop fragments; sort query
    query = "&".join(sorted([q for q in u.query.split("&") if q])) if u.query else ""
    return up.urlunsplit((u.scheme.lower(), u.netloc.lower(), u.path or "/", query, ""))

def is_same_site(seed: str, candidate: str) -> bool:
    s = up.urlsplit(seed)
    c = up.urlsplit(candidate)
    # match registrable domain via tldextract, allow subdomains
    s_ext = tldextract.extract(s.netloc)
    c_ext = tldextract.extract(c.netloc)
    return (s_ext.registered_domain == c_ext.registered_domain) and (c.scheme in ("http", "https"))

def get_robots(base: str) -> robotparser.RobotFileParser:
    u = up.urlsplit(base)
    robots_url = up.urlunsplit((u.scheme, u.netloc, "/robots.txt", "", ""))
    rp = robotparser.RobotFileParser()
    try:
        r = SESSION.get(robots_url, timeout=15)
        if r.status_code == 200:
            rp.parse(r.text.splitlines())
        else:
            rp.parse([])  # treat as empty
    except requests.RequestException:
        rp.parse([])
    return rp

def fetch(url: str, timeout=25) -> Optional[requests.Response]:
    try:
        r = SESSION.get(url, timeout=timeout)
        if r.status_code == 200 and "text/html" in r.headers.get("Content-Type", ""):
            return r
        # Allow XML for sitemap discovery
        if r.status_code == 200 and "xml" in r.headers.get("Content-Type", ""):
            return r
        return None
    except requests.RequestException:
        return None

def discover_sitemaps(base: str, rp: robotparser.RobotFileParser) -> List[str]:
    # robots.txt Sitemaps
    sitemaps = []
    try:
        # robotparser doesn't expose sitemaps; quick fetch robots again to parse
        u = up.urlsplit(base)
        robots_url = up.urlunsplit((u.scheme, u.netloc, "/robots.txt", "", ""))
        r = SESSION.get(robots_url, timeout=15)
        if r.status_code == 200:
            for line in r.text.splitlines():
                if line.lower().startswith("sitemap:"):
                    sitemaps.append(line.split(":", 1)[1].strip())
    except Exception:
        pass
    # canonical /sitemap.xml
    u = up.urlsplit(base)
    sitemaps.append(up.urlunsplit((u.scheme, u.netloc, "/sitemap.xml", "", "")))
    # de-dup
    out = []
    seen = set()
    for s in sitemaps:
        s = norm_url(s)
        if s not in seen:
            out.append(s)
            seen.add(s)
    return out

def parse_sitemap(xml_text: str, base: str) -> List[str]:
    urls = []
    try:
        soup = BeautifulSoup(xml_text, "xml")
        # urlset
        for loc in soup.find_all("loc"):
            u = loc.get_text(strip=True)
            if u:
                urls.append(u)
        # nested sitemaps
        for sm in soup.find_all("sitemap"):
            loc = sm.find("loc")
            if loc and loc.text:
                sub = SESSION.get(loc.text, timeout=20)
                if sub.status_code == 200:
                    urls.extend(parse_sitemap(sub.text, base))
    except Exception:
        pass
    # filter to same-site
    urls = [u for u in urls if is_same_site(base, u)]
    # normalize + de-dup
    out, seen = [], set()
    for u in urls:
        nu = norm_url(u)
        if nu not in seen:
            out.append(nu)
            seen.add(nu)
    return out

def clean_html_to_markdown(soup: BeautifulSoup) -> str:
    for tag in soup.find_all(BLOCK_TAGS):
        tag.decompose()
    # remove comments
    for c in soup.find_all(string=lambda s: isinstance(s, Comment)):
        c.extract()
    body = soup.body or soup
    html = str(body)
    # markdownify keeps structure nicely
    md = html2md(html, heading_style="ATX", strip=["img"])
    # compact whitespace
    md = re.sub(r"\n{3,}", "\n\n", md).strip()
    return md

def extract_dates(soup: BeautifulSoup, headers: Dict[str, str]) -> Tuple[Optional[str], Optional[str]]:
    published = None
    updated = None
    # meta tags
    for m in soup.find_all("meta"):
        k = (m.get("property") or m.get("name") or "").strip().lower()
        if not k:
            continue
        if k in DATE_META_KEYS:
            v = m.get("content")
            if v:
                try:
                    dt = dateparser.parse(v)
                    if k.endswith("published_time") or "publish" in k or k == "date" or "issued" in k:
                        if not published:
                            published = dt.isoformat()
                    if k.endswith("modified_time") or "updated" in k or "modified" in k or "revise" in k:
                        if not updated:
                            updated = dt.isoformat()
                except Exception:
                    pass
    # HTTP headers
    if not updated:
        lm = headers.get("Last-Modified")
        if lm:
            try:
                updated = dateparser.parse(lm).isoformat()
            except Exception:
                pass
    return published, updated

def extract_text_snippet(md: str, n_chars=300) -> str:
    t = re.sub(r"\s+", " ", md).strip()
    return t[:n_chars]

def absolute_url(page_url: str, maybe_url: str) -> Optional[str]:
    if not maybe_url:
        return None
    absu = up.urljoin(page_url, maybe_url)
    try:
        parsed = up.urlsplit(absu)
        if parsed.scheme in ("http", "https"):
            return absu
        return None
    except Exception:
        return None

def sha1_bytes(b: bytes) -> str:
    return hashlib.sha1(b).hexdigest()

def save_image(img_url: str, out_dir: str) -> Optional[Tuple[str, str]]:
    """Download image; return (path, sha1) or None."""
    try:
        r = SESSION.get(img_url, timeout=25, stream=True)
        if r.status_code == 200 and "image" in r.headers.get("Content-Type", ""):
            content = r.content
            h = sha1_bytes(content)
            # figure extension
            ct = r.headers.get("Content-Type", "")
            ext = ".bin"
            if "jpeg" in ct: ext = ".jpg"
            elif "png" in ct: ext = ".png"
            elif "gif" in ct: ext = ".gif"
            elif "webp" in ct: ext = ".webp"
            elif "svg" in ct: ext = ".svg"
            filename = f"{h}{ext}"
            path = os.path.join(out_dir, filename)
            os.makedirs(out_dir, exist_ok=True)
            with open(path, "wb") as f:
                f.write(content)
            return path, h
    except requests.RequestException:
        return None
    return None

def chunk_markdown(md: str, target_tokens=800) -> List[str]:
    """
    Roughly split by words ~1 token ≈ 0.75 words; so 800 tokens ≈ 600 words.
    We'll target ~650 words per chunk and split on paragraph boundaries.
    """
    paras = [p.strip() for p in md.split("\n\n") if p.strip()]
    chunks = []
    cur = []
    count = 0
    TARGET_WORDS = 650
    for p in paras:
        words = len(p.split())
        if count + words > TARGET_WORDS and cur:
            chunks.append("\n\n".join(cur))
            cur, count = [], 0
        cur.append(p)
        count += words
    if cur:
        chunks.append("\n\n".join(cur))
    return chunks

def extract_page(page_url: str, resp: requests.Response) -> Dict:
    soup = BeautifulSoup(resp.text, "html.parser")
    # canonical
    canonical = soup.find("link", rel=lambda v: v and "canonical" in v.lower())
    canonical_url = absolute_url(page_url, canonical.get("href")) if canonical else None

    title = soup.title.get_text(strip=True) if soup.title else ""
    meta_desc = ""
    meta = {}
    for m in soup.find_all("meta"):
        name = (m.get("name") or m.get("property") or "").strip().lower()
        content = m.get("content") or ""
        if not name or not content:
            continue
        meta[name] = content
        if name == "description" and not meta_desc:
            meta_desc = content

    # headings
    h1 = [h.get_text(strip=True) for h in soup.find_all("h1")]
    h2 = [h.get_text(strip=True) for h in soup.find_all("h2")]
    h3 = [h.get_text(strip=True) for h in soup.find_all("h3")]

    # links
    internal_links, external_links = [], []
    for a in soup.find_all("a"):
        href = absolute_url(page_url, a.get("href"))
        if not href:
            continue
        if is_same_site(page_url, href):
            internal_links.append(href)
        else:
            external_links.append(href)

    # images
    images = []
    for img in soup.find_all("img"):
        src = absolute_url(page_url, img.get("src"))
        if not src:
            continue
        alt = img.get("alt") or ""
        images.append({"src": src, "alt": alt})

    # dates
    published, updated = extract_dates(soup, resp.headers)

    # clean body -> markdown
    markdown = clean_html_to_markdown(soup)

    return {
        "url": page_url,
        "canonical_url": canonical_url or page_url,
        "status": resp.status_code,
        "retrieved_at": datetime.utcnow().isoformat() + "Z",
        "title": title,
        "meta_description": meta_desc,
        "meta": meta,
        "h1": h1,
        "h2": h2,
        "h3": h3,
        "published_at": published,
        "updated_at": updated,
        "internal_links": sorted(list(set(internal_links))),
        "external_links": sorted(list(set(external_links))),
        "images": images,
        "markdown": markdown,
        "snippet": extract_text_snippet(markdown),
        "content_length": len(markdown),
    }

def crawl(seed: str, out_dir="data", depth=3, delay=1.0, max_pages=5000):
    os.makedirs(out_dir, exist_ok=True)
    pages_path = os.path.join(out_dir, "pages.jsonl")
    chunks_path = os.path.join(out_dir, "chunks.jsonl")
    images_dir = os.path.join(out_dir, "images")
    images_manifest = os.path.join(out_dir, "images.csv")

    rp = get_robots(seed)
    def allowed(u: str) -> bool:
        try:
            return rp.can_fetch(SESSION.headers.get("User-Agent", DEFAULT_UA), u)
        except Exception:
            return True

    # discovery
    q = queue.Queue()
    seen: Set[str] = set()
    seeds: List[str] = [norm_url(seed)]
    # From sitemap(s)
    for sm_url in discover_sitemaps(seed, rp):
        if not allowed(sm_url):
            continue
        r = fetch(sm_url)
        if r is not None and r.status_code == 200 and "xml" in r.headers.get("Content-Type", ""):
            for u in parse_sitemap(r.text, seed):
                seeds.append(norm_url(u))

    # unique and enqueue
    for u in sorted(set(seeds)):
        if is_same_site(seed, u):
            q.put((u, 0))
            seen.add(u)

    # prepare writers
    img_fieldnames = ["page_url", "image_src", "saved_path", "sha1", "alt"]
    if not os.path.exists(images_manifest):
        with open(images_manifest, "w", newline="", encoding="utf-8") as f:
            csv.DictWriter(f, fieldnames=img_fieldnames).writeheader()

    pages_out = open(pages_path, "a", encoding="utf-8")
    chunks_out = open(chunks_path, "a", encoding="utf-8")

    pbar = tqdm(total=max_pages, desc="Crawling", unit="page")
    count = 0
    try:
        while not q.empty() and count < max_pages:
            url, d = q.get()
            if not allowed(url):
                continue

            time.sleep(delay)
            resp = fetch(url)
            if resp is None:
                continue

            page = extract_page(url, resp)

            # Write page record
            pages_out.write(json.dumps(page, ensure_ascii=False) + "\n")

            # Write chunks
            for i, chunk in enumerate(chunk_markdown(page["markdown"])):
                chunk_rec = {
                    "id": hashlib.sha1(f"{page['canonical_url']}#{i}".encode()).hexdigest(),
                    "source_url": page["canonical_url"],
                    "page_title": page["title"],
                    "chunk_index": i,
                    "text": chunk,
                    "published_at": page["published_at"],
                    "updated_at": page["updated_at"],
                    "retrieved_at": page["retrieved_at"],
                    "metadata": {
                        "h1": page["h1"],
                        "h2": page["h2"],
                        "h3": page["h3"],
                        "meta_description": page["meta_description"],
                    },
                }
                chunks_out.write(json.dumps(chunk_rec, ensure_ascii=False) + "\n")

            # Download images + manifest
            if page["images"]:
                with open(images_manifest, "a", newline="", encoding="utf-8") as f:
                    w = csv.DictWriter(f, fieldnames=img_fieldnames)
                    for img in page["images"]:
                        src = img["src"]
                        alt = img["alt"]
                        saved = save_image(src, images_dir)
                        if saved:
                            path, h = saved
                            w.writerow({
                                "page_url": page["canonical_url"],
                                "image_src": src,
                                "saved_path": os.path.relpath(path, out_dir),
                                "sha1": h,
                                "alt": alt,
                            })

            # enqueue new internal links
            if d < depth:
                for link in page["internal_links"]:
                    n = norm_url(link)
                    if n not in seen and is_same_site(seed, n) and allowed(n):
                        seen.add(n)
                        q.put((n, d + 1))

            count += 1
            pbar.update(1)
    finally:
        pages_out.close()
        chunks_out.close()
        pbar.close()

    print(f"\nDone.\n - Pages: {pages_path}\n - Chunks: {chunks_path}\n - Images: {images_dir}\n - Image manifest: {images_manifest}")

def main():
    ap = argparse.ArgumentParser(description="RAG scraper for zirmon.com")
    ap.add_argument("--seed", default="https://zirmon.com/", help="Seed URL (default: https://zirmon.com/)")
    ap.add_argument("--out", default="data", help="Output directory")
    ap.add_argument("--depth", type=int, default=3, help="Crawl depth (default: 3)")
    ap.add_argument("--delay", type=float, default=1.0, help="Delay between requests in seconds (default: 1.0)")
    ap.add_argument("--max-pages", type=int, default=5000, help="Safety cap on total pages (default: 5000)")
    args = ap.parse_args()

    # lock to same registrable domain as seed
    crawl(args.seed, out_dir=args.out, depth=args.depth, delay=args.delay, max_pages=args.max_pages)

if __name__ == "__main__":
    main()