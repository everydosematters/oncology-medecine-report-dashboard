import json
import re
import requests
from bs4 import BeautifulSoup

LISTING = "https://fdaghana.gov.gh/newsroom/product-recalls-and-alerts/"
AJAX = "https://fdaghana.gov.gh/wp-admin/admin-ajax.php"

UA = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/145.0.0.0 Safari/537.36"
)

HEADERS = {
    "User-Agent": UA,
    "Accept": "application/json, text/javascript, */*; q=0.01",
    "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
    "X-Requested-With": "XMLHttpRequest",
    "Origin": "https://fdaghana.gov.gh",
    "Referer": LISTING,
}


def detect_column_count(html: str) -> int:
    soup = BeautifulSoup(html, "html.parser")
    for table in soup.find_all("table"):
        text = " ".join(table.get_text(" ", strip=True).lower().split())
        if "date recall was issued" in text and "product name" in text:
            ths = table.find_all("th")
            if ths:
                return len(ths)
    return 13  # fallback


def find_nonce_candidates(html: str) -> list[str]:
    """
    Find nonce-like tokens (hex-ish) near relevant JS text.
    Returns a unique list of candidates.
    """
    candidates = set()

    # 1) direct key patterns (most reliable)
    key_patterns = [
        r'wdtNonce"\s*:\s*"([^"]+)"',
        r'wdt_nonce"\s*:\s*"([^"]+)"',
        r'wdtAjaxNonce"\s*:\s*"([^"]+)"',
        r'wdt_ajax_nonce"\s*:\s*"([^"]+)"',
        r'"security"\s*:\s*"([^"]+)"',
        r'"nonce"\s*:\s*"([^"]+)"',
        r'"_wpnonce"\s*:\s*"([^"]+)"',
    ]
    for pat in key_patterns:
        for m in re.finditer(pat, html):
            candidates.add(m.group(1))

    # 2) contextual search: grab chunks of script that mention wdt/datatable/ajax and extract hex tokens
    for m in re.finditer(r"(?:wdt|datatable|admin-ajax|ajax|nonce|security).{0,400}", html, flags=re.I | re.S):
        chunk = m.group(0)
        for tok in re.findall(r"\b[a-f0-9]{8,32}\b", chunk, flags=re.I):
            candidates.add(tok)

    # prune obviously wrong tokens (very short etc.)
    out = [c for c in candidates if 8 <= len(c) <= 32]
    # stable order (longer first often)
    out.sort(key=lambda x: (-len(x), x))
    return out


def make_datatables_payload(
    action: str,
    table_id: int,
    draw: int,
    start: int,
    length: int,
    ncols: int,
    nonce_key: str | None,
    nonce_value: str | None,
) -> dict[str, str]:
    payload: dict[str, str] = {
        "action": action,
        "table_id": str(table_id),
        # harmless aliases that some installs require:
        "wdtTableId": str(table_id),
        "tableId": str(table_id),
        "draw": str(draw),
        "start": str(start),
        "length": str(length),
        "search[value]": "",
        "search[regex]": "false",
        "order[0][column]": "0",
        "order[0][dir]": "desc",
    }

    if nonce_key and nonce_value:
        payload[nonce_key] = nonce_value

    for i in range(ncols):
        payload[f"columns[{i}][data]"] = str(i)
        payload[f"columns[{i}][name]"] = ""
        payload[f"columns[{i}][searchable]"] = "true"
        payload[f"columns[{i}][orderable]"] = "true"
        payload[f"columns[{i}][search][value]"] = ""
        payload[f"columns[{i}][search][regex]"] = "false"

    return payload


def looks_like_json(text: str) -> bool:
    t = text.lstrip("\ufeff").strip()
    return t.startswith("{") and t.endswith("}")


def discover_working_request(table_id: int = 47) -> tuple[str, str | None, str | None, int]:
    """
    Returns (action, nonce_key, nonce_value, ncols) for a request that produces JSON.
    Raises if none found.
    """
    s = requests.Session()
    r0 = s.get(LISTING, headers={"User-Agent": UA}, timeout=60)
    r0.raise_for_status()
    html = r0.text

    print("Cookies after priming:", s.cookies.get_dict())

    ncols = detect_column_count(html)
    print("Detected columns:", ncols)

    nonce_candidates = find_nonce_candidates(html)
    print("Nonce candidates found:", len(nonce_candidates))
    print("Top candidates:", nonce_candidates[:8])

    # actions to try (wpDataTables varies by install/version)
    actions = [
        "get_wdtable",
        "wpdatatable_get_ajax_data",
        "wdt_get_ajax_data",
        "wpdatatables_get_ajax_data",
        "get_wdt_table_data",
    ]

    nonce_keys = [
        "wdtNonce",
        "wdt_nonce",
        "wdtAjaxNonce",
        "wdt_ajax_nonce",
        "security",
        "nonce",
        "_wpnonce",
    ]

    # Try: action × nonce_key × nonce_value (including None)
    # We try "no nonce" first for each action, then candidates.
    test_nonce_values = [None] + nonce_candidates[:25]  # cap to keep it fast

    for action in actions:
        for nonce_key in ([None] + nonce_keys):
            for nonce_value in test_nonce_values:
                payload = make_datatables_payload(
                    action=action,
                    table_id=table_id,
                    draw=1,
                    start=0,
                    length=10,
                    ncols=ncols,
                    nonce_key=nonce_key,
                    nonce_value=nonce_value,
                )

                resp = s.post(
                    AJAX,
                    params={"action": action, "table_id": str(table_id)},
                    data=payload,
                    headers=HEADERS,
                    timeout=60,
                )

                txt = resp.text.lstrip("\ufeff").strip()

                # Skip empties quickly
                if not txt:
                    continue

                # WP often returns "0" / "-1" for failures
                if txt in ("0", "-1"):
                    continue

                # If HTML, ignore (wrong action or blocked)
                if txt.startswith("<"):
                    continue

                if looks_like_json(txt):
                    # validate it parses
                    try:
                        obj = json.loads(txt)
                    except Exception:
                        continue

                    # verify it has a data-like field
                    if isinstance(obj, dict) and (obj.get("data") or obj.get("aaData") or obj.get("tableData")):
                        print("\n✅ FOUND working combo!")
                        print("action:", action)
                        print("nonce_key:", nonce_key)
                        print("nonce_value:", nonce_value)
                        return action, nonce_key, nonce_value, ncols

    raise RuntimeError("Could not discover a working wpDataTables AJAX request automatically.")


def fetch_all_rows(table_id: int = 47, page_size: int = 200) -> list:
    action, nonce_key, nonce_value, ncols = discover_working_request(table_id=table_id)

    s = requests.Session()
    s.get(LISTING, headers={"User-Agent": UA}, timeout=60)

    all_rows = []
    start = 0
    draw = 1

    while True:
        payload = make_datatables_payload(
            action=action,
            table_id=table_id,
            draw=draw,
            start=start,
            length=page_size,
            ncols=ncols,
            nonce_key=nonce_key,
            nonce_value=nonce_value,
        )

        resp = s.post(
            AJAX,
            params={"action": action, "table_id": str(table_id)},
            data=payload,
            headers=HEADERS,
            timeout=60,
        )
        resp.raise_for_status()

        txt = resp.text.lstrip("\ufeff").strip()
        if not txt or txt in ("0", "-1") or txt.startswith("<"):
            break

        obj = json.loads(txt)
        rows = obj.get("data") or obj.get("aaData") or []
        if not rows:
            break

        all_rows.extend(rows)
        print("Fetched:", len(rows), " | Total:", len(all_rows))

        start += len(rows)
        draw += 1

        if len(rows) < page_size:
            break

    return all_rows


if __name__ == "__main__":
    rows = fetch_all_rows(table_id=47, page_size=200)
    print("\nTOTAL ROWS:", len(rows))
    if rows:
        print("FIRST ROW TYPE:", type(rows[0]))
        for i in range(len(rows)):
            print()
            print("===="*10)
            print(rows[i])
            print()

            if i == 10:
                break