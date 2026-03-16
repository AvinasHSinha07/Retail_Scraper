import html
import io
import json
import mimetypes
import os
import re
import time
from urllib.parse import urlparse

import requests
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google.oauth2 import service_account
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseUpload

SITE_ID = "v2lp5d"
BASE_URL = "https://api.searchspring.net/api/search/search.json"
SITE_BASE = "https://camilla.com"

HEADERS = {
    "User-Agent": "Mozilla/5.0"
}

COLLECTIONS = {
    "sale": {
        "handle": "sale",
        "site_url": "https://camilla.com/collections/sale",
        "sheet_name": "Camilla_sale_sheet",
        "image_prefix": "S_Camilla_",
    },
    "collection": {
        "handle": "new-arrivals",
        "site_url": "https://camilla.com/collections/new-arrivals",
        "sheet_name": "Camilla_collection_sheet",
        "image_prefix": "C_Camilla_",
    }
}

# -------------------------------------------------
# USER SETTINGS
# -------------------------------------------------

RESULTS_PER_PAGE = 100
FETCH_DESCRIPTIONS = True
REQUEST_DELAY = 0.0  # increase if site starts rate-limiting
MAX_PRODUCTS_PER_COLLECTION = 30  # set like 50 for testing, keep None for all

# True = upload image directly to Google Drive
UPLOAD_IMAGES_TO_DRIVE = True

# If True, products missing in the next full run are appended with is_available=0
# Keep this FALSE while testing with small MAX_PRODUCTS_PER_COLLECTION
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


def build_session():
    session = requests.Session()

    retry = Retry(
        total=5,
        connect=5,
        read=5,
        backoff_factor=1,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"]
    )

    adapter = HTTPAdapter(max_retries=retry)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    session.headers.update(HEADERS)

    return session


SESSION = build_session()


def parse_variants(raw_value):
    if not raw_value:
        return []

    try:
        decoded = html.unescape(str(raw_value))
        data = json.loads(decoded)
        return data if isinstance(data, list) else []
    except Exception:
        return []


def extract_sizes(variants):
    sizes = []
    seen = set()

    for v in variants:
        title = str(v.get("title", "")).strip()
        if title:
            key = title.lower()
            if key not in seen:
                seen.add(key)
                sizes.append(title)

    return " | ".join(sizes)


def full_product_url(relative_url):
    if not relative_url:
        return ""

    relative_url = str(relative_url).strip()
    if relative_url.startswith("http"):
        return relative_url

    return SITE_BASE + relative_url


def calc_discount_percent(msrp, price):
    try:
        msrp = float(msrp)
        price = float(price)
        if msrp > 0 and msrp > price:
            return round(((msrp - price) / msrp) * 100, 2)
    except Exception:
        pass
    return ""


def clean_html_text(value):
    if not value:
        return ""

    value = html.unescape(str(value))
    soup = BeautifulSoup(value, "html.parser")
    text = soup.get_text(" ", strip=True)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def extract_description_from_item(item):
    candidate_keys = [
        "description",
        "descriptionHtml",
        "description_html",
        "body",
        "body_html",
        "bodyHtml",
        "contents",
        "content",
        "mfield_global_description",
        "mfield_description",
        "longDescription",
        "shortDescription"
    ]

    for key in candidate_keys:
        value = item.get(key)
        if value:
            cleaned = clean_html_text(value)
            if cleaned:
                return cleaned

    return ""


def fetch_product_page_description(product_url):
    if not product_url:
        return ""

    try:
        response = SESSION.get(product_url, timeout=30)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, "html.parser")

        selectors = [
            "div.product__description",
            "div.product-description",
            "div.product-single__description",
            "div.rte",
            "div.accordion__content",
            "section.product__description",
            'div[class*="product-description"]',
            'div[class*="description"]'
        ]

        for selector in selectors:
            elements = soup.select(selector)
            for el in elements:
                text = clean_html_text(str(el))
                if text and len(text) > 40:
                    return text

        for script in soup.find_all("script", type="application/ld+json"):
            raw_json = script.string or script.get_text(strip=True)
            if not raw_json:
                continue

            try:
                data = json.loads(raw_json)

                if isinstance(data, dict):
                    desc = data.get("description")
                    if desc:
                        cleaned = clean_html_text(desc)
                        if cleaned:
                            return cleaned

                elif isinstance(data, list):
                    for obj in data:
                        if isinstance(obj, dict):
                            desc = obj.get("description")
                            if desc:
                                cleaned = clean_html_text(desc)
                                if cleaned:
                                    return cleaned

            except Exception:
                continue

    except Exception:
        pass

    return ""


def get_description(item):
    desc = extract_description_from_item(item)
    if desc:
        return desc

    if FETCH_DESCRIPTIONS:
        product_url = full_product_url(item.get("url"))
        return fetch_product_page_description(product_url)

    return ""


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


def build_image_filename(product_title, collection_key, image_url):
    prefix = COLLECTIONS[collection_key]["image_prefix"]
    safe_title = sanitize_filename(product_title)
    ext = get_image_extension(image_url)
    return f"{prefix}{safe_title}{ext}"


def product_is_available(item):
    direct_keys = [
        "available",
        "isAvailable",
        "inStock",
        "in_stock",
        "stock",
        "inventory",
        "inventory_quantity",
    ]

    for key in direct_keys:
        if key in item:
            value = item.get(key)

            if isinstance(value, bool):
                return 1 if value else 0

            if isinstance(value, (int, float)):
                return 1 if value > 0 else 0

            value_str = str(value).strip().lower()
            if value_str in {"true", "1", "yes", "available", "in stock"}:
                return 1
            if value_str in {"false", "0", "no", "out of stock", "sold out", "unavailable"}:
                return 0

    variants = parse_variants(item.get("ss_variants"))
    if variants:
        for variant in variants:
            for key in [
                "available",
                "isAvailable",
                "inStock",
                "in_stock",
                "stock",
                "inventory",
                "inventory_quantity",
                "inventoryQuantity",
                "qty",
            ]:
                if key in variant:
                    value = variant.get(key)

                    if isinstance(value, bool) and value:
                        return 1
                    if isinstance(value, (int, float)) and value > 0:
                        return 1

                    value_str = str(value).strip().lower()
                    if value_str in {"true", "1", "yes", "available", "in stock"}:
                        return 1

        return 0

    return 1


def get_google_services():
    """
    Supports:
    1) Installed-app OAuth credentials.json
    2) Service-account JSON credentials.json
    """
    with open(CREDENTIALS_FILE, "r", encoding="utf-8") as f:
        creds_json = json.load(f)

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

        with open(TOKEN_FILE, "w", encoding="utf-8") as token:
            token.write(creds.to_json())

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
    except Exception as e:
        print(f"Drive upload failed for {image_url}: {e}")
        return ""


def fetch_all_products(collection_handle, results_per_page=100, max_products=None):
    all_results = []
    seen_ids = set()
    seen_pages = set()
    page = 1

    while True:
        if page in seen_pages:
            print(f"  Page {page} already seen. Stopping to avoid loop.")
            break

        seen_pages.add(page)

        params = {
            "siteId": SITE_ID,
            "resultsFormat": "native",
            "page": page,
            "resultsPerPage": results_per_page,
            "bgfilter.collection_handle": collection_handle
        }

        print(f"Fetching {collection_handle} page {page}...")
        response = SESSION.get(BASE_URL, params=params, timeout=30)
        response.raise_for_status()
        data = response.json()

        results = data.get("results", [])
        pagination = data.get("pagination", {}) or {}

        current_page = pagination.get("currentPage", page)
        total_pages = pagination.get("totalPages")
        total_results = pagination.get("totalResults")
        next_page = pagination.get("nextPage", 0)

        if not results:
            print("  No results on this page, stopping.")
            break

        new_count = 0
        for item in results:
            pid = item.get("id")
            if pid and pid not in seen_ids:
                seen_ids.add(pid)
                all_results.append(item)
                new_count += 1

                if max_products is not None and len(all_results) >= max_products:
                    print(f"  Reached max_products={max_products}. Stopping.")
                    return all_results

        print(
            f"  Found {len(results)} results, added {new_count} new products"
            f" | current_page={current_page}"
            f" | total_pages={total_pages}"
            f" | total_results={total_results}"
            f" | next_page={next_page}"
        )

        if REQUEST_DELAY > 0:
            time.sleep(REQUEST_DELAY)

        if not next_page:
            break

        page = next_page

    return all_results


def previous_rows_by_key(rows, id_field):
    out = {}
    for row in rows:
        key = str(row.get(id_field, "")).strip() or str(row.get("product_url", "")).strip()
        if key:
            out[key] = row
    return out


def merge_with_previous(current_rows, previous_rows, id_field, fieldnames):
    """
    If a product existed in previous output but is missing in current output,
    append it with is_available=0.
    Use ONLY for full runs, not test runs with small max_products.
    """
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
        s["properties"]["title"]
        for s in sheets
        if "properties" in s and "title" in s["properties"]
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


def build_sale_rows(results, drive_service=None):
    rows = []
    total = len(results)

    for idx, item in enumerate(results, start=1):
        print(f"[SALE {idx}/{total}] {item.get('name')}")

        variants = parse_variants(item.get("ss_variants"))
        sizes = extract_sizes(variants)
        description = get_description(item)
        image_src = item.get("imageUrl") or ""

        image_name = ""
        if (
            UPLOAD_IMAGES_TO_DRIVE
            and image_src
            and drive_service
            and DRIVE_FOLDER_ID
            and DRIVE_FOLDER_ID != "PUT_YOUR_DRIVE_FOLDER_ID_HERE"
        ):
            target_name = build_image_filename(
                product_title=item.get("name", ""),
                collection_key="sale",
                image_url=image_src,
            )
            image_name = upload_image_to_drive(
                service=drive_service,
                image_url=image_src,
                filename=target_name,
                folder_id=DRIVE_FOLDER_ID,
            )

        rows.append({
            "id": item.get("id", ""),
            "brand": item.get("brand", ""),
            "title": item.get("name", ""),
            "original_price": item.get("msrp", ""),
            "sale_price": item.get("price", ""),
            "discount_percent": calc_discount_percent(item.get("msrp"), item.get("price")),
            "site_url": COLLECTIONS["sale"]["site_url"],
            "image_url": image_src,
            "image": image_name,
            "created_at": "",
            "product_url": full_product_url(item.get("url")),
            "Style": item.get("mfield_global_title_type", ""),
            "Collection Name": "sale",
            "Sizes": sizes,
            "Description": description,
            "is_available": str(product_is_available(item)),
            "source_link": COLLECTIONS["sale"]["site_url"],
        })

        if REQUEST_DELAY > 0:
            time.sleep(REQUEST_DELAY)

    return rows


def build_collection_rows(results, drive_service=None):
    rows = []
    total = len(results)

    for idx, item in enumerate(results, start=1):
        print(f"[COLLECTION {idx}/{total}] {item.get('name')}")

        variants = parse_variants(item.get("ss_variants"))
        sizes = extract_sizes(variants)
        description = get_description(item)
        image_src = item.get("imageUrl") or ""

        image_name = ""
        if (
            UPLOAD_IMAGES_TO_DRIVE
            and image_src
            and drive_service
            and DRIVE_FOLDER_ID
            and DRIVE_FOLDER_ID != "PUT_YOUR_DRIVE_FOLDER_ID_HERE"
        ):
            target_name = build_image_filename(
                product_title=item.get("name", ""),
                collection_key="collection",
                image_url=image_src,
            )
            image_name = upload_image_to_drive(
                service=drive_service,
                image_url=image_src,
                filename=target_name,
                folder_id=DRIVE_FOLDER_ID,
            )

        rows.append({
            "Id": item.get("id", ""),
            "brand": item.get("brand", ""),
            "store_link": COLLECTIONS["collection"]["site_url"],
            "title": item.get("name", ""),
            "price": item.get("price", ""),
            "description": description,
            "Size": sizes,
            "Style": item.get("mfield_global_title_type", ""),
            "image_url": image_src,
            "image": image_name,
            "product_url": full_product_url(item.get("url")),
            "is_available": str(product_is_available(item)),
            "source_link": COLLECTIONS["collection"]["site_url"],
        })

        if REQUEST_DELAY > 0:
            time.sleep(REQUEST_DELAY)

    return rows


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

    sale_results = fetch_all_products(
        COLLECTIONS["sale"]["handle"],
        results_per_page=RESULTS_PER_PAGE,
        max_products=MAX_PRODUCTS_PER_COLLECTION
    )

    collection_results = fetch_all_products(
        COLLECTIONS["collection"]["handle"],
        results_per_page=RESULTS_PER_PAGE,
        max_products=MAX_PRODUCTS_PER_COLLECTION
    )

    print(f"\nTotal raw sale products fetched: {len(sale_results)}")
    print(f"Total raw collection products fetched: {len(collection_results)}")

    sale_rows = build_sale_rows(sale_results, drive_service=drive_service)
    collection_rows = build_collection_rows(collection_results, drive_service=drive_service)

    if COMPARE_WITH_PREVIOUS:
        previous_sale_rows = load_previous_rows_from_sheet(
            sheets_service=sheets_service,
            spreadsheet_id=SHEET_ID,
            sheet_name=COLLECTIONS["sale"]["sheet_name"],
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
            sheet_name=COLLECTIONS["collection"]["sheet_name"],
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
        sheet_name=COLLECTIONS["sale"]["sheet_name"],
        rows=sale_rows,
        fieldnames=sale_fields,
    )
    print(f"Updated Google Sheet tab {COLLECTIONS['sale']['sheet_name']} with {len(sale_rows)} rows")

    save_sheet(
        sheets_service=sheets_service,
        spreadsheet_id=SHEET_ID,
        sheet_name=COLLECTIONS["collection"]["sheet_name"],
        rows=collection_rows,
        fieldnames=collection_fields,
    )
    print(f"Updated Google Sheet tab {COLLECTIONS['collection']['sheet_name']} with {len(collection_rows)} rows")


if __name__ == "__main__":
    main()