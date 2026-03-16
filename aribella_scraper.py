import html
import io
import json
import mimetypes
import os
import re
import time
from urllib.parse import parse_qs, unquote_plus, urljoin, urlparse

import requests
from bs4 import BeautifulSoup

from google.auth.transport.requests import Request
from google.oauth2 import service_account
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseUpload

BASE_URL = "https://aribella.com.au"

COLLECTION_API_URL = "https://aribella.com.au/collections/kaftans/products.json"
COLLECTION_PAGE_URL = "https://aribella.com.au/collections/kaftans"

SALE_SOURCE_URLS = [
    "https://aribella.com.au/collections/sale/products/kehlani-strap-frill-dress?variant=43117869891629",
    "https://aribella.com.au/collections/sale/products/antionette-gypsy-dress",
    "https://aribella.com.au/products/open-short-kaftan?_pos=60&_sid=f29f6d151&_ss=r",
    "https://aribella.com.au/search?q=print+tops&search=",
    "https://aribella.com.au/search?q=silk+dress&search=",
]

HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Accept": "*/*",
}

LIMIT = 250
TIMEOUT = 30
SLEEP_SECONDS = 0.35

# Use an integer for testing, or None for all products.
MAX_PRODUCTS_PER_COLLECTION = None

# True = upload image directly to Google Drive
UPLOAD_IMAGES_TO_DRIVE = True

# If True, products missing in the next full run are appended with is_available=0
COMPARE_WITH_PREVIOUS = True

# Google API settings
CREDENTIALS_FILE = "credentials.json"
TOKEN_FILE = "token.json"
SCOPES = [
    "https://www.googleapis.com/auth/drive",
    "https://www.googleapis.com/auth/spreadsheets",
]

# Google Drive folder ID for images
DRIVE_FOLDER_ID = "1AMegVS3qqXIVANDdtf44QhVbd8RMtDvg"

# Google Sheets spreadsheet ID
SHEET_ID = "10fshaUtIwptjTXcWENynbNnNMA6HHyVdKPkEzdY635s"

COLLECTION_SHEET_NAME = "Aribella_collection_sheet"
SALE_SHEET_NAME = "Aribella_sale_sheet"
COLLECTION_IMAGE_PREFIX = "C_Aribella_"
SALE_IMAGE_PREFIX = "S_Aribella_"


def build_session():
    session = requests.Session()
    session.headers.update(HEADERS)
    return session


SESSION = build_session()


def clean_html(text):
    if not text:
        return ""
    text = html.unescape(str(text))
    text = re.sub(r"<br\s*/?>", "\n", text, flags=re.I)
    text = re.sub(r"</p>", "\n", text, flags=re.I)
    text = re.sub(r"</li>", "\n", text, flags=re.I)
    text = re.sub(r"<[^>]+>", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def dedupe_keep_order(values):
    seen = set()
    result = []
    for val in values:
        if val is None:
            continue
        val = str(val).strip()
        if not val:
            continue
        key = val.lower()
        if key not in seen:
            seen.add(key)
            result.append(val)
    return result


def cents_to_money(value):
    if value in [None, "", "null"]:
        return None
    try:
        return round(float(value) / 100.0, 2)
    except Exception:
        return None


def safe_float(value):
    if value in [None, "", "null"]:
        return None
    try:
        return float(str(value).replace(",", "").strip())
    except Exception:
        return None


def normalize_url(url):
    if not url:
        return ""
    url = str(url).strip()

    if url.startswith("//"):
        return "https:" + url
    if url.startswith("/"):
        return BASE_URL + url
    if url.startswith("http://") or url.startswith("https://"):
        return url
    return url


def safe_get(session, url, params=None):
    try:
        response = session.get(url, params=params, timeout=TIMEOUT, allow_redirects=True)
        return response
    except Exception as error:
        print(f"Request failed: {url} | {error}")
        return None


def get_style(product_data):
    return product_data.get("type") or product_data.get("product_type") or ""


def cap_image_width(url, width=400):
    """Append/replace the Shopify CDN width param to limit download size."""
    if not url:
        return url
    from urllib.parse import parse_qsl, urlencode, urlunparse
    parsed = urlparse(url)
    query = dict(parse_qsl(parsed.query, keep_blank_values=True))
    query["width"] = str(width)
    new_query = urlencode(query)
    return urlunparse((parsed.scheme, parsed.netloc, parsed.path, parsed.params, new_query, parsed.fragment))


def get_image_url(product_data):
    images = product_data.get("images", []) or []

    if images and isinstance(images[0], str):
        return cap_image_width(normalize_url(images[0]))

    if images and isinstance(images[0], dict):
        return cap_image_width(normalize_url(images[0].get("src", "")))

    featured = product_data.get("featured_image")
    if isinstance(featured, str) and featured.strip():
        return cap_image_width(normalize_url(featured))

    return ""


def get_size_option_position(product_data):
    options = product_data.get("options", []) or []

    for opt in options:
        name = str(opt.get("name", "")).strip().lower()
        if name in {"size", "sizes"}:
            pos = opt.get("position")
            try:
                pos = int(pos)
                if pos in [1, 2, 3]:
                    return pos
            except Exception:
                pass

    return None


def get_sizes(product_data):
    variants = product_data.get("variants", []) or []
    options = product_data.get("options", []) or []

    size_position = get_size_option_position(product_data)
    sizes = []

    if size_position:
        size_key = f"option{size_position}"
        for variant in variants:
            if variant.get("available") is True:
                value = variant.get(size_key)
                if value is not None:
                    value = str(value).strip()
                    if value and value.lower() not in {"default", "default title"}:
                        sizes.append(value)

        sizes = dedupe_keep_order(sizes)
        if sizes:
            return " | ".join(sizes)

    for opt in options:
        name = str(opt.get("name", "")).strip().lower()
        if name in {"size", "sizes"}:
            for value in opt.get("values", []) or []:
                value = str(value).strip()
                if value and value.lower() not in {"default", "default title"}:
                    sizes.append(value)

    sizes = dedupe_keep_order(sizes)
    return " | ".join(sizes)


def get_available_variants(product_data):
    variants = product_data.get("variants", []) or []
    return [variant for variant in variants if variant.get("available") is True]


def is_product_available(product_data):
    return len(get_available_variants(product_data)) > 0


def sanitize_filename(value, max_length=120):
    value = html.unescape(str(value or "")).strip()
    value = re.sub(r"[^\w\- ]+", "", value)
    value = re.sub(r"\s+", "_", value)
    value = value.strip("_")

    if not value:
        value = "product"

    if len(value) > max_length:
        value = value[:max_length].rstrip("_")

    return value


def get_image_extension(image_url):
    if not image_url:
        return ".jpg"

    parsed = urlparse(image_url)
    path = parsed.path or ""
    _, ext = os.path.splitext(path)

    ext = ext.lower().strip()
    if ext in [".jpg", ".jpeg", ".png", ".webp"]:
        return ext

    return ".jpg"


def build_image_filename(product_title, image_url, image_prefix):
    safe_title = sanitize_filename(product_title)
    ext = get_image_extension(image_url)
    return f"{image_prefix}{safe_title}{ext}"


def get_google_services():
    with open(CREDENTIALS_FILE, "r", encoding="utf-8") as creds_file:
        creds_json = json.load(creds_file)

    if creds_json.get("type") == "service_account":
        creds = service_account.Credentials.from_service_account_file(
            CREDENTIALS_FILE,
            scopes=SCOPES,
        )
        drive_service = build("drive", "v3", credentials=creds)
        sheets_service = build("sheets", "v4", credentials=creds)
        return drive_service, sheets_service

    creds = None
    if os.path.exists(TOKEN_FILE):
        creds = Credentials.from_authorized_user_file(TOKEN_FILE, SCOPES)

    needs_auth = (
        not creds
        or not creds.valid
        or (hasattr(creds, "has_scopes") and not creds.has_scopes(SCOPES))
    )

    if needs_auth:
        if creds and creds.expired and creds.refresh_token:
            try:
                creds.refresh(Request())
            except Exception:
                creds = None

        if not creds or (hasattr(creds, "has_scopes") and not creds.has_scopes(SCOPES)):
            flow = InstalledAppFlow.from_client_secrets_file(CREDENTIALS_FILE, SCOPES)
            creds = flow.run_local_server(port=0)

        with open(TOKEN_FILE, "w", encoding="utf-8") as token_file:
            token_file.write(creds.to_json())

    drive_service = build("drive", "v3", credentials=creds)
    sheets_service = build("sheets", "v4", credentials=creds)
    return drive_service, sheets_service


def escape_drive_query_value(value):
    return str(value).replace("\\", "\\\\").replace("'", "\\'")


def find_existing_drive_file(service, folder_id, filename):
    safe_name = escape_drive_query_value(filename)
    query = f"name = '{safe_name}' and '{folder_id}' in parents and trashed = false"

    result = (
        service.files()
        .list(
            q=query,
            spaces="drive",
            fields="files(id, name)",
            pageSize=1,
        )
        .execute()
    )

    files = result.get("files", [])
    if files:
        return files[0]
    return None


def download_image_bytes(image_url):
    response = SESSION.get(image_url, timeout=60)
    response.raise_for_status()
    return response.content


def upload_file_to_drive(service, file_bytes, filename, folder_id, mime_type):
    existing = find_existing_drive_file(service, folder_id, filename)
    if existing:
        return existing["name"]

    file_metadata = {
        "name": filename,
        "parents": [folder_id],
    }

    media = MediaIoBaseUpload(
        io.BytesIO(file_bytes),
        mimetype=mime_type or "application/octet-stream",
        resumable=True,
    )

    created = (
        service.files()
        .create(
            body=file_metadata,
            media_body=media,
            fields="id, name",
        )
        .execute()
    )

    return created["name"]


def upload_image_to_drive(service, image_url, filename, folder_id):
    if not image_url or not filename:
        return ""

    try:
        file_bytes = download_image_bytes(image_url)
        mime_type = mimetypes.guess_type(filename)[0] or "application/octet-stream"
        uploaded_name = upload_file_to_drive(
            service=service,
            file_bytes=file_bytes,
            filename=filename,
            folder_id=folder_id,
            mime_type=mime_type,
        )
        return uploaded_name
    except Exception as error:
        print(f"Drive upload failed for {image_url}: {error}")
        return ""


def previous_rows_by_key(rows, id_field):
    out = {}
    for row in rows:
        key = str(row.get(id_field, "")).strip() or str(row.get("product_url", "")).strip()
        if key:
            out[key] = row
    return out


def merge_with_previous(current_rows, previous_rows, id_field, fieldnames):
    current_map = previous_rows_by_key(current_rows, id_field)
    previous_map = previous_rows_by_key(previous_rows, id_field)

    merged = list(current_rows)

    for key, prev_row in previous_map.items():
        if key not in current_map:
            row = dict(prev_row)
            row["is_available"] = "0"
            cleaned = {field: row.get(field, "") for field in fieldnames}
            merged.append(cleaned)

    return merged


def get_sheet_titles(sheets_service, spreadsheet_id):
    result = (
        sheets_service.spreadsheets()
        .get(
            spreadsheetId=spreadsheet_id,
            fields="sheets(properties(title))",
        )
        .execute()
    )
    sheets = result.get("sheets", [])
    return {
        sheet["properties"]["title"]
        for sheet in sheets
        if "properties" in sheet and "title" in sheet["properties"]
    }


def ensure_sheet_exists(sheets_service, spreadsheet_id, sheet_name):
    existing_titles = get_sheet_titles(sheets_service, spreadsheet_id)
    if sheet_name in existing_titles:
        return

    body = {
        "requests": [
            {
                "addSheet": {
                    "properties": {
                        "title": sheet_name
                    }
                }
            }
        ]
    }

    (
        sheets_service.spreadsheets()
        .batchUpdate(
            spreadsheetId=spreadsheet_id,
            body=body,
        )
        .execute()
    )


def load_previous_rows_from_sheet(sheets_service, spreadsheet_id, sheet_name):
    ensure_sheet_exists(sheets_service, spreadsheet_id, sheet_name)

    result = (
        sheets_service.spreadsheets()
        .values()
        .get(
            spreadsheetId=spreadsheet_id,
            range=f"'{sheet_name}'",
        )
        .execute()
    )

    values = result.get("values", [])
    if not values:
        return []

    header = values[0]
    data_rows = values[1:]

    rows = []
    for raw_row in data_rows:
        row = {}
        for idx, col_name in enumerate(header):
            row[col_name] = raw_row[idx] if idx < len(raw_row) else ""
        rows.append(row)

    return rows


def clear_sheet_values(sheets_service, spreadsheet_id, sheet_name):
    ensure_sheet_exists(sheets_service, spreadsheet_id, sheet_name)

    (
        sheets_service.spreadsheets()
        .values()
        .clear(
            spreadsheetId=spreadsheet_id,
            range=f"'{sheet_name}'",
            body={},
        )
        .execute()
    )


def save_sheet(sheets_service, spreadsheet_id, sheet_name, rows, fieldnames):
    ensure_sheet_exists(sheets_service, spreadsheet_id, sheet_name)

    values = [fieldnames]
    for row in rows:
        values.append([row.get(field, "") for field in fieldnames])

    clear_sheet_values(sheets_service, spreadsheet_id, sheet_name)

    (
        sheets_service.spreadsheets()
        .values()
        .update(
            spreadsheetId=spreadsheet_id,
            range=f"'{sheet_name}'!A1",
            valueInputOption="RAW",
            body={"values": values},
        )
        .execute()
    )


def fetch_collection_products(session, max_products=None):
    all_products = []
    page = 1
    seen_ids = set()

    while True:
        params = {
            "limit": LIMIT,
            "page": page,
        }

        response = safe_get(session, COLLECTION_API_URL, params=params)
        if response is None:
            break

        response.raise_for_status()

        products = response.json().get("products", [])
        print(f"[Collection] Page {page} | Products found: {len(products)}")

        if not products:
            break

        for product in products:
            pid = product.get("id")
            if pid and pid in seen_ids:
                continue
            if pid:
                seen_ids.add(pid)
            all_products.append(product)

            if max_products is not None and len(all_products) >= max_products:
                return all_products

        page += 1
        time.sleep(SLEEP_SECONDS)

    return all_products


def get_collection_price_and_stock(product_data):
    variants = product_data.get("variants", []) or []
    available_prices = []
    all_prices = []
    has_available = False

    for variant in variants:
        available = bool(variant.get("available", False))
        price = safe_float(variant.get("price"))

        if price is not None:
            all_prices.append(price)
            if available:
                available_prices.append(price)

        if available:
            has_available = True

    if available_prices:
        min_price = min(available_prices)
    elif all_prices:
        min_price = min(all_prices)
    else:
        min_price = ""

    return min_price, has_available


def build_collection_rows(products, drive_service=None):
    rows = []

    for idx, product in enumerate(products, start=1):
        print(f"[COLLECTION {idx}/{len(products)}] {product.get('title', '')}")

        min_price, is_available = get_collection_price_and_stock(product)
        if not is_available:
            continue

        product_handle = product.get("handle", "")
        product_url = f"{BASE_URL}/products/{product_handle}" if product_handle else ""
        image_url = get_image_url(product)

        image_name = ""
        if (
            UPLOAD_IMAGES_TO_DRIVE
            and image_url
            and drive_service
            and DRIVE_FOLDER_ID
            and DRIVE_FOLDER_ID != "PUT_YOUR_DRIVE_FOLDER_ID_HERE"
        ):
            target_name = build_image_filename(
                product_title=product.get("title", ""),
                image_url=image_url,
                image_prefix=COLLECTION_IMAGE_PREFIX,
            )
            image_name = upload_image_to_drive(
                service=drive_service,
                image_url=image_url,
                filename=target_name,
                folder_id=DRIVE_FOLDER_ID,
            )

        row = {
            "Id": product.get("id", ""),
            "brand": product.get("vendor", ""),
            "store_link": COLLECTION_PAGE_URL,
            "title": product.get("title", ""),
            "price": min_price,
            "description": clean_html(product.get("body_html", "")),
            "Size": get_sizes(product),
            "Style": get_style(product),
            "image_url": image_url,
            "image": image_name,
            "product_url": product_url,
            "is_available": "1",
            "source_link": COLLECTION_API_URL,
        }
        rows.append(row)

        time.sleep(SLEEP_SECONDS)

    deduped = []
    seen_urls = set()
    for row in rows:
        key = str(row.get("product_url", "")).strip()
        if key and key in seen_urls:
            continue
        if key:
            seen_urls.add(key)
        deduped.append(row)

    return deduped


def classify_url(url):
    path = urlparse(url).path.lower()
    if "/search" in path:
        return "search"
    if "/products/" in path:
        return "product"
    if "/collections/" in path:
        return "collection"
    return "other"


def canonical_product_url(url):
    parsed = urlparse(url)
    match = re.search(r"/products/([^/?#]+)", parsed.path)
    if not match:
        return None
    handle = match.group(1).strip()
    if not handle:
        return None
    return f"{BASE_URL}/products/{handle}"


def product_js_url(product_url):
    canonical = canonical_product_url(product_url)
    if not canonical:
        return None
    handle = canonical.rstrip("/").split("/")[-1]
    return f"{BASE_URL}/products/{handle}.js"


def get_collection_name_from_source_url(source_url):
    parsed = urlparse(source_url)
    path = parsed.path

    if "/search" in path:
        query = parse_qs(parsed.query).get("q", [""])[0].strip()
        return unquote_plus(query) if query else "search"

    match = re.search(r"/collections/([^/]+)", path)
    if match:
        return match.group(1).replace("-", " ").strip()

    return "sale"


def get_sale_pricing(product_data):
    available_variants = get_available_variants(product_data)

    candidate_pairs = []
    for variant in available_variants:
        price = cents_to_money(variant.get("price"))
        compare_at_price = cents_to_money(variant.get("compare_at_price"))

        if (
            price is not None
            and compare_at_price is not None
            and compare_at_price > price
        ):
            candidate_pairs.append((compare_at_price, price))

    if not candidate_pairs:
        return None, None, None, False

    original_price = max(item[0] for item in candidate_pairs)
    sale_price = min(item[1] for item in candidate_pairs)

    if original_price <= sale_price:
        return None, None, None, False

    discount_percent = round(((original_price - sale_price) / original_price) * 100, 2)
    return original_price, sale_price, discount_percent, True


def extract_search_product_links(search_html):
    soup = BeautifulSoup(search_html, "html.parser")
    found = []

    selectors = [
        'main a[href*="/products/"]',
        '.product-item a[href*="/products/"]',
        '.card-wrapper a[href*="/products/"]',
        '.grid-product__link[href*="/products/"]',
        '.product-card a[href*="/products/"]',
        '.collection a[href*="/products/"]',
        '.search-results a[href*="/products/"]',
    ]

    for selector in selectors:
        for anchor in soup.select(selector):
            href = (anchor.get("href") or "").strip()
            if "/products/" not in href:
                continue
            abs_url = urljoin(BASE_URL, href)
            canon = canonical_product_url(abs_url)
            if canon:
                found.append(canon)

    if not found:
        for anchor in soup.find_all("a", href=True):
            href = anchor["href"].strip()
            if "/products/" not in href:
                continue
            abs_url = urljoin(BASE_URL, href)
            canon = canonical_product_url(abs_url)
            if canon:
                found.append(canon)

    found = dedupe_keep_order(found)

    cleaned = []
    for url in found:
        handle = url.rstrip("/").split("/")[-1].strip().lower()

        if handle in {"gift-card", "gift-cards"}:
            continue
        if not handle or handle == "all":
            continue

        cleaned.append(url)

    return cleaned


def fetch_search_product_urls(session, search_url):
    response = safe_get(session, search_url)
    if response is None:
        return []

    if response.status_code != 200:
        print(f"Search page failed: {search_url} | status={response.status_code}")
        return []

    product_urls = extract_search_product_links(response.text)
    print(f"[Sale] Search URL: {search_url} | extracted product URLs: {len(product_urls)}")
    return product_urls


def fetch_product_json(session, source_product_url):
    js_url = product_js_url(source_product_url)
    if not js_url:
        return None, None

    response = safe_get(session, js_url)
    if response is None:
        return None, js_url

    if response.status_code != 200:
        print(f"Product JS failed: {js_url} | status={response.status_code}")
        return None, js_url

    try:
        return response.json(), js_url
    except Exception as error:
        print(f"JSON parse failed: {js_url} | {error}")
        return None, js_url


def build_sale_row(product_data, site_url, source_link, drive_service=None):
    if not is_product_available(product_data):
        return None

    original_price, sale_price, discount_percent, on_sale = get_sale_pricing(product_data)
    if not on_sale:
        return None

    image_url = get_image_url(product_data)

    image_name = ""
    if (
        UPLOAD_IMAGES_TO_DRIVE
        and image_url
        and drive_service
        and DRIVE_FOLDER_ID
        and DRIVE_FOLDER_ID != "PUT_YOUR_DRIVE_FOLDER_ID_HERE"
    ):
        target_name = build_image_filename(
            product_title=product_data.get("title", ""),
            image_url=image_url,
            image_prefix=SALE_IMAGE_PREFIX,
        )
        image_name = upload_image_to_drive(
            service=drive_service,
            image_url=image_url,
            filename=target_name,
            folder_id=DRIVE_FOLDER_ID,
        )

    product_handle = product_data.get("handle", "")
    canonical_url = f"{BASE_URL}/products/{product_handle}" if product_handle else ""

    return {
        "id": product_data.get("id", ""),
        "brand": product_data.get("vendor", ""),
        "title": product_data.get("title", ""),
        "original_price": original_price,
        "sale_price": sale_price,
        "discount_percent": discount_percent,
        "site_url": site_url,
        "image_url": image_url,
        "image": image_name,
        "created_at": product_data.get("created_at", ""),
        "product_url": canonical_url,
        "Style": get_style(product_data),
        "Collection Name": get_collection_name_from_source_url(site_url),
        "Sizes": get_sizes(product_data),
        "Description": clean_html(product_data.get("description", "") or product_data.get("body_html", "")),
        "is_available": "1",
        "source_link": source_link,
    }


def build_sale_rows(session, drive_service=None, max_products=None):
    discovered_products = []
    seen_products = set()

    print("[Sale] Collecting product URLs from source URLs...")

    for source_url in SALE_SOURCE_URLS:
        source_type = classify_url(source_url)

        if source_type == "search":
            urls = fetch_search_product_urls(session, source_url)
            for product_url in urls:
                canon = canonical_product_url(product_url)
                if canon and canon not in seen_products:
                    seen_products.add(canon)
                    discovered_products.append({
                        "site_url": source_url,
                        "product_url": canon,
                    })

                    if max_products is not None and len(discovered_products) >= max_products:
                        break

        elif source_type == "product":
            canon = canonical_product_url(source_url)
            if canon and canon not in seen_products:
                seen_products.add(canon)
                discovered_products.append({
                    "site_url": source_url,
                    "product_url": canon,
                })

        if max_products is not None and len(discovered_products) >= max_products:
            break

        time.sleep(SLEEP_SECONDS)

    print(f"[Sale] Unique product URLs collected: {len(discovered_products)}")

    rows = []

    for idx, item in enumerate(discovered_products, start=1):
        site_url = item["site_url"]
        prod_url = item["product_url"]

        print(f"[SALE {idx}/{len(discovered_products)}] Fetching product: {prod_url}")

        product_data, js_url = fetch_product_json(session, prod_url)
        if not product_data:
            time.sleep(SLEEP_SECONDS)
            continue

        row = build_sale_row(
            product_data,
            site_url=site_url,
            source_link=js_url,
            drive_service=drive_service,
        )
        if row:
            rows.append(row)

        time.sleep(SLEEP_SECONDS)

    deduped = []
    seen_urls = set()
    for row in rows:
        key = str(row.get("product_url", "")).strip()
        if key and key in seen_urls:
            continue
        if key:
            seen_urls.add(key)
        deduped.append(row)

    return deduped


def main():
    drive_service, sheets_service = get_google_services()

    sale_fields = [
        "id",
        "brand",
        "title",
        "original_price",
        "sale_price",
        "discount_percent",
        "site_url",
        "image_url",
        "image",
        "created_at",
        "product_url",
        "Style",
        "Collection Name",
        "Sizes",
        "Description",
        "is_available",
        "source_link",
    ]

    collection_fields = [
        "Id",
        "brand",
        "store_link",
        "title",
        "price",
        "description",
        "Size",
        "Style",
        "image_url",
        "image",
        "product_url",
        "is_available",
        "source_link",
    ]

    print("Starting Aribella scrape...")

    collection_products = fetch_collection_products(
        SESSION,
        max_products=MAX_PRODUCTS_PER_COLLECTION,
    )
    print(f"[Collection] Total raw products fetched: {len(collection_products)}")

    collection_rows = build_collection_rows(
        collection_products,
        drive_service=drive_service,
    )

    sale_rows = build_sale_rows(
        SESSION,
        drive_service=drive_service,
        max_products=MAX_PRODUCTS_PER_COLLECTION,
    )

    if COMPARE_WITH_PREVIOUS:
        previous_sale_rows = load_previous_rows_from_sheet(
            sheets_service=sheets_service,
            spreadsheet_id=SHEET_ID,
            sheet_name=SALE_SHEET_NAME,
        )
        sale_rows = merge_with_previous(
            current_rows=sale_rows,
            previous_rows=previous_sale_rows,
            id_field="id",
            fieldnames=sale_fields,
        )

        previous_collection_rows = load_previous_rows_from_sheet(
            sheets_service=sheets_service,
            spreadsheet_id=SHEET_ID,
            sheet_name=COLLECTION_SHEET_NAME,
        )
        collection_rows = merge_with_previous(
            current_rows=collection_rows,
            previous_rows=previous_collection_rows,
            id_field="Id",
            fieldnames=collection_fields,
        )

    save_sheet(
        sheets_service=sheets_service,
        spreadsheet_id=SHEET_ID,
        sheet_name=SALE_SHEET_NAME,
        rows=sale_rows,
        fieldnames=sale_fields,
    )
    print(f"Updated Google Sheet tab {SALE_SHEET_NAME} with {len(sale_rows)} rows")

    save_sheet(
        sheets_service=sheets_service,
        spreadsheet_id=SHEET_ID,
        sheet_name=COLLECTION_SHEET_NAME,
        rows=collection_rows,
        fieldnames=collection_fields,
    )
    print(f"Updated Google Sheet tab {COLLECTION_SHEET_NAME} with {len(collection_rows)} rows")


if __name__ == "__main__":
    main()