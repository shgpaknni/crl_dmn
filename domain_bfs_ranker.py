#!/usr/bin/env python3
"""BFS domain ranker with branded-domain skip logic (depth 3)."""

from __future__ import annotations

import argparse
import gzip
import html.parser
import io
import re
import socket
import ssl
import threading
import urllib.error
import urllib.parse
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

try:
    import tldextract  # type: ignore
except Exception:  # optional dependency
    tldextract = None

COMMON_SECOND_LEVEL_SUFFIXES = {
    "co.uk",
    "org.uk",
    "gov.uk",
    "ac.uk",
    "co.jp",
    "ne.jp",
    "or.jp",
    "com.au",
    "net.au",
    "org.au",
    "com.br",
    "com.mx",
    "com.tr",
    "co.in",
    "firm.in",
    "gen.in",
    "ind.in",
    "net.in",
    "org.in",
    "co.kr",
    "co.nz",
    "com.sg",
    "com.my",
    "com.hk",
    "com.cn",
    "com.tw",
}

HREF_RE = re.compile(r"(?i)\bhref\s*=\s*(['\"])(.*?)\1")
SCHEME_RE = re.compile(r"^[a-zA-Z][a-zA-Z0-9+.-]*://")


class LinkExtractor(html.parser.HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.links: list[str] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag.lower() != "a":
            return
        for key, value in attrs:
            if key and key.lower() == "href" and value:
                self.links.append(value)


def normalize_domain(host: str | None) -> str | None:
    if not host:
        return None

    host = host.strip().lower().rstrip(".")
    if not host:
        return None

    if "@" in host:
        host = host.split("@", 1)[-1]
    if ":" in host:
        host = host.split(":", 1)[0]
    if host in {"", "localhost"}:
        return None

    if tldextract is not None:
        parts = tldextract.extract(host)
        if parts.domain and parts.suffix:
            return f"{parts.domain}.{parts.suffix}".lower()

    labels = [part for part in host.split(".") if part]
    if len(labels) < 2:
        return None

    candidate_suffix = ".".join(labels[-2:])
    if candidate_suffix in COMMON_SECOND_LEVEL_SUFFIXES and len(labels) >= 3:
        return ".".join(labels[-3:])
    return ".".join(labels[-2:])


def to_url(domain: str, scheme: str) -> str:
    return f"{scheme}://{domain}/"


def decode_body(data: bytes, encoding: str | None) -> str:
    if not data:
        return ""
    if encoding:
        try:
            return data.decode(encoding, errors="replace")
        except LookupError:
            pass
    for codec in ("utf-8", "latin-1"):
        try:
            return data.decode(codec, errors="replace")
        except UnicodeDecodeError:
            continue
    return data.decode("utf-8", errors="replace")


def parse_links(html_text: str, base_url: str) -> list[str]:
    parser = LinkExtractor()
    try:
        parser.feed(html_text)
    except Exception:
        parser.links.extend(match[1] for match in HREF_RE.findall(html_text))

    domains: list[str] = []
    for href in parser.links:
        href = href.strip()
        if not href or href.startswith(("#", "mailto:", "tel:", "javascript:")):
            continue

        absolute = urllib.parse.urljoin(base_url, href)
        if not SCHEME_RE.match(absolute):
            continue

        parsed = urllib.parse.urlparse(absolute)
        if parsed.scheme not in {"http", "https"}:
            continue

        domain = normalize_domain(parsed.netloc)
        if domain:
            domains.append(domain)

    return domains


def fetch_html(domain: str, timeout: float, max_read_bytes: int) -> str:
    headers = {
        "User-Agent": "Mozilla/5.0 (compatible; DomainBFSRanker/2.0)",
        "Accept": "text/html,application/xhtml+xml;q=0.9,*/*;q=0.1",
        "Accept-Encoding": "gzip",
        "Connection": "close",
    }
    context = ssl.create_default_context()

    for scheme in ("https", "http"):
        req = urllib.request.Request(url=to_url(domain, scheme), headers=headers, method="GET")
        try:
            with urllib.request.urlopen(req, timeout=timeout, context=context) as resp:
                ctype = resp.headers.get("Content-Type", "").lower()
                if "text/html" not in ctype and "application/xhtml+xml" not in ctype:
                    return ""

                raw = resp.read(max_read_bytes)
                if "gzip" in resp.headers.get("Content-Encoding", "").lower():
                    try:
                        raw = gzip.GzipFile(fileobj=io.BytesIO(raw)).read()
                    except OSError:
                        pass

                return decode_body(raw, resp.headers.get_content_charset())
        except (urllib.error.URLError, urllib.error.HTTPError, TimeoutError, socket.timeout, ssl.SSLError):
            continue

    return ""


class WriterBundle:
    def __init__(self, out_dir: Path) -> None:
        out_dir.mkdir(parents=True, exist_ok=True)
        self.seed_fp = open(out_dir / "rank0_seed.txt", "w", encoding="utf-8", newline="\n")
        self.rank1_fp = open(out_dir / "rank1.txt", "w", encoding="utf-8", newline="\n")
        self.rank2_fp = open(out_dir / "rank2.txt", "w", encoding="utf-8", newline="\n")
        self.rank3_fp = open(out_dir / "rank3.txt", "w", encoding="utf-8", newline="\n")
        self.skipped_fp = open(out_dir / "skipped_branded.txt", "w", encoding="utf-8", newline="\n")
        self._lock = threading.Lock()

    def write_seed(self, seed: str) -> None:
        with self._lock:
            self.seed_fp.write(seed + "\n")

    def write_rank(self, domain: str, rank: int) -> None:
        with self._lock:
            if rank == 1:
                self.rank1_fp.write(domain + "\n")
            elif rank == 2:
                self.rank2_fp.write(domain + "\n")
            elif rank == 3:
                self.rank3_fp.write(domain + "\n")

    def write_skipped(self, domain: str) -> None:
        with self._lock:
            self.skipped_fp.write(domain + "\n")

    def close(self) -> None:
        self.seed_fp.close()
        self.rank1_fp.close()
        self.rank2_fp.close()
        self.rank3_fp.close()
        self.skipped_fp.close()


def load_domains(path: Path) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    with open(path, "r", encoding="utf-8", errors="ignore") as f:
        for line in f:
            d = normalize_domain(line.strip())
            if d and d not in seen:
                seen.add(d)
                out.append(d)
    return out


def crawl_seed(seed: str, branded: set[str], output_root: Path, timeout: float, workers: int, max_nodes_per_depth: int, max_read_bytes: int) -> None:
    seed_dir = output_root / seed.replace(".", "_")
    writers = WriterBundle(seed_dir)
    try:
        writers.write_seed(seed)
        seen_non_branded: set[str] = {seed}
        seen_skipped: set[str] = set()
        current_level: list[str] = [seed]

        for depth in range(0, 3):
            next_depth = depth + 1
            discovered_next: set[str] = set()

            with ThreadPoolExecutor(max_workers=workers) as pool:
                futures = {pool.submit(fetch_html, domain, timeout, max_read_bytes): domain for domain in current_level}

                for future in as_completed(futures):
                    source_domain = futures[future]
                    try:
                        html_text = future.result()
                    except Exception:
                        continue
                    if not html_text:
                        continue

                    for domain in parse_links(html_text, to_url(source_domain, "https")):
                        if domain == source_domain:
                            continue
                        if domain in branded:
                            if domain not in seen_skipped:
                                seen_skipped.add(domain)
                                writers.write_skipped(domain)
                            continue
                        if domain in seen_non_branded:
                            continue

                        seen_non_branded.add(domain)
                        writers.write_rank(domain, next_depth)
                        if next_depth < 3:
                            discovered_next.add(domain)

            if not discovered_next:
                break

            if max_nodes_per_depth > 0 and len(discovered_next) > max_nodes_per_depth:
                current_level = sorted(discovered_next)[:max_nodes_per_depth]
            else:
                current_level = list(discovered_next)
    finally:
        writers.close()


def main() -> int:
    parser = argparse.ArgumentParser(description="BFS rank domains by depth (1..3), skipping branded discoveries.")
    parser.add_argument("--branded", required=True, type=Path, help="Branded source-of-truth domain file.")
    parser.add_argument("--seed", type=str, help="Single seed domain for one crawl session.")
    parser.add_argument("--seeds", type=Path, help="Optional seed list file. Ignored when --seed is set.")
    parser.add_argument("--output", required=True, type=Path, help="Output directory.")
    parser.add_argument("--timeout", type=float, default=8.0, help="HTTP timeout seconds per request.")
    parser.add_argument("--workers", type=int, default=32, help="Parallel fetch workers per BFS level.")
    parser.add_argument("--max-nodes-per-depth", type=int, default=0, help="Cap domains forwarded to next depth (0 = unlimited).")
    parser.add_argument("--max-seeds", type=int, default=0, help="Limit number of seeds in batch mode (0 = all).")
    parser.add_argument("--max-read-bytes", type=int, default=2_000_000, help="Max bytes read per page response.")

    args = parser.parse_args()

    branded = set(load_domains(args.branded))

    if args.seed:
        seed_norm = normalize_domain(args.seed)
        if not seed_norm:
            raise SystemExit("Invalid --seed domain")
        seeds = [seed_norm]
    else:
        seed_path = args.seeds if args.seeds else args.branded
        seeds = load_domains(seed_path)

    if args.max_seeds > 0:
        seeds = seeds[: args.max_seeds]

    args.output.mkdir(parents=True, exist_ok=True)

    total = len(seeds)
    for idx, seed in enumerate(seeds, start=1):
        print(f"[{idx}/{total}] Crawling seed: {seed}")
        crawl_seed(seed, branded, args.output, args.timeout, args.workers, args.max_nodes_per_depth, args.max_read_bytes)

    print("Done.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
