import html
import io
import json
import mimetypes
import os
import re
import time
from urllib.parse import parse_qsl, urlencode, urlparse, urlunparse

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google.oauth2 import service_account
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseUpload

# ============================================================
# CONFIG
# ============================================================

COLLECTION_URLS = [
    "https://kaftansthatbling.com/collections/long-embellished-kaftans?filter.v.availability=1&filter.v.price.gte=&filter.v.price.lte=&sort_by=price-descending",
    "https://kaftansthatbling.com/collections/box-styled-silk-kaftans?filter.v.availability=1&filter.v.price.gte=&filter.v.price.lte=&sort_by=price-descending",
    "https://kaftansthatbling.com/collections/long-embellished-maxi-dresses?filter.v.availability=1&filter.v.price.gte=&filter.v.price.lte=&sort_by=price-descending",
    "https://kaftansthatbling.com/collections/short-embellished-kaftans?filter.v.availability=1&filter.v.price.gte=&filter.v.price.lte=&sort_by=price-descending",
    "https://kaftansthatbling.com/collections/silk-embellished-dresses?filter.v.availability=1&filter.v.price.gte=&filter.v.price.lte=&sort_by=price-descending",
    "https://kaftansthatbling.com/collections/silk-hi-low-embellished-frill-dresses?filter.v.availability=1&filter.v.price.gte=&filter.v.price.lte=&sort_by=price-descending",
    "https://kaftansthatbling.com/collections/silk-tops-pants?filter.v.availability=1&filter.v.price.gte=&filter.v.price.lte=&sort_by=price-descending",
    "https://kaftansthatbling.com/collections/up-to-70-off-sale-styles?filter.v.availability=1&filter.v.price.gte=&filter.v.price.lte=&sort_by=manual",
]

SALE_FIELDS = [
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

SHEET_NAME = "Kaftan_sale_sheet"
IMAGE_PREFIX = "S_Kaftan_"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Accept": "application/json,text/html,*/*",
    "Accept-Language": "en-US,en;q=0.9",
}

MAX_PRODUCTS_PER_COLLECTION = None
REQUEST_DELAY_SECONDS = 0

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


# ============================================================
# HELPERS
# ============================================================

def build_session():
    session = requests.Session()
    retry = Retry(
        total=5,
        backoff_factor=1,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    session.headers.update(HEADERS)
    return session


SESSION = build_session()


def clean_html(text):
    if not text:
        return ""
    text = html.unescape(text)
    text = re.sub(r"<[^>]+>", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def safe_float(value):
    if value in (None, "", "null"):
        return None
    try:
        return float(str(value).replace(",", "").strip())
    except Exception:
        return None


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


def get_base_domain(page_url):
    parsed = urlparse(page_url)
    return f"{parsed.scheme}://{parsed.netloc}"


def get_collection_handle(page_url):
    """
    Example:
    https://kaftansthatbling.com/collections/long-embellished-kaftans?... 
    -> long-embellished-kaftans
    """
    path = urlparse(page_url).path.strip("/")
    parts = path.split("/")
    if len(parts) >= 2 and parts[0] == "collections":
        return parts[1]
    return ""


def build_collection_api_url(page_url):
    parsed = urlparse(page_url)
    return f"{parsed.scheme}://{parsed.netloc}{parsed.path.rstrip('/')}/products.json"


def is_page_filtered_for_availability(page_url):
    return "filter.v.availability=1" in page_url


def get_product_url(base_domain, handle):
    return f"{base_domain}/products/{handle}"


def dedupe_keep_order(items):
    seen = set()
    out = []
    for item in items:
        item = str(item).strip()
        if item and item not in seen:
            seen.add(item)
            out.append(item)
    return out


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


def build_image_filename(product_title, image_url):
    safe_title = sanitize_filename(product_title)
    ext = get_image_extension(image_url)
    return f"{IMAGE_PREFIX}{safe_title}{ext}"


def add_or_replace_query_param(url, key, value):
    parsed = urlparse(url)
    query = dict(parse_qsl(parsed.query, keep_blank_values=True))
    query[key] = str(value)
    new_query = urlencode(query)
    return urlunparse((parsed.scheme, parsed.netloc, parsed.path, parsed.params, new_query, parsed.fragment))


def get_first_main_image_src(product, preferred_width=400):
    # Shopify exposes the primary image in product['image']; fallback to first product['images'] entry.
    main_image = product.get("image")
    if isinstance(main_image, dict):
        src = str(main_image.get("src", "")).strip()
        if src:
            return add_or_replace_query_param(src, "width", preferred_width)

    images = product.get("images", []) or []
    if images and isinstance(images[0], dict):
        src = str(images[0].get("src", "")).strip()
        if src:
            return add_or_replace_query_param(src, "width", preferred_width)

    return ""


def parse_sizes(product):
    variants = product.get("variants", []) or []
    options = product.get("options", []) or []

    sizes = []

    # First try options with name "Size"
    for opt in options:
        name = str(opt.get("name", "")).strip().lower()
        if name == "size":
            sizes.extend(opt.get("values", []) or [])

    # Fallback to variant option1
    if not sizes:
        for variant in variants:
            option1 = variant.get("option1")
            if option1 and str(option1).strip().lower() not in {"default title", "default"}:
                sizes.append(option1)

    return dedupe_keep_order(sizes)


def calculate_prices(product):
    variants = product.get("variants", []) or []

    sale_prices = []
    compare_prices = []

    for variant in variants:
        price = safe_float(variant.get("price"))
        compare_at_price = safe_float(variant.get("compare_at_price"))

        if price is not None:
            sale_prices.append(price)

        if compare_at_price is not None:
            compare_prices.append(compare_at_price)

    sale_price = min(sale_prices) if sale_prices else ""
    original_price = max(compare_prices) if compare_prices else ""

    discount_percent = ""
    if original_price not in ("", None) and sale_price not in ("", None):
        if float(original_price) > 0:
            discount_percent = round(
                ((float(original_price) - float(sale_price)) / float(original_price)) * 100,
                2,
            )

    return original_price, sale_price, discount_percent


def is_available_product(product):
    variants = product.get("variants", []) or []
    return any(bool(v.get("available", False)) for v in variants)


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
    keep its previous row as-is.
    This prevents partial runs from flipping unchecked products to unavailable.
    """
    current_map = previous_rows_by_key(current_rows, id_field)
    previous_map = previous_rows_by_key(previous_rows, id_field)

    merged = list(current_rows)

    for key, prev_row in previous_map.items():
        if key not in current_map:
            row = dict(prev_row)
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


def build_row(product, collection_page_url, drive_service=None):
    base_domain = get_base_domain(collection_page_url)
    collection_handle = get_collection_handle(collection_page_url)

    product_id = product.get("id", "")
    brand = product.get("vendor", "")
    title = product.get("title", "")
    created_at = product.get("created_at", "")
    handle = product.get("handle", "")
    product_url = get_product_url(base_domain, handle) if handle else ""

    original_price, sale_price, discount_percent = calculate_prices(product)

    image_url = get_first_main_image_src(product)
    image_name = ""
    if (
        UPLOAD_IMAGES_TO_DRIVE
        and image_url
        and drive_service
        and DRIVE_FOLDER_ID
        and DRIVE_FOLDER_ID != "PUT_YOUR_DRIVE_FOLDER_ID_HERE"
    ):
        target_name = build_image_filename(
            product_title=title,
            image_url=image_url,
        )
        image_name = upload_image_to_drive(
            service=drive_service,
            image_url=image_url,
            filename=target_name,
            folder_id=DRIVE_FOLDER_ID,
        )

    sizes = parse_sizes(product)
    description = clean_html(product.get("body_html", ""))

    # Style column -> using title, same logic as your earlier sheet structure
    style = title

    available = is_available_product(product)

    row = {
        "id": product_id,
        "brand": brand,
        "title": title,
        "original_price": original_price,
        "sale_price": sale_price,
        "discount_percent": discount_percent,
        "site_url": collection_page_url,
        "image_url": image_url,
        "image": image_name,
        "created_at": created_at,
        "product_url": product_url,
        "Style": style,
        "Collection Name": collection_handle,
        "Sizes": " | ".join(sizes),
        "Description": description,
        "is_available": "1" if available else "0",
        "source_link": collection_page_url,
    }

    return row


# ============================================================
# FETCH LOGIC
# ============================================================

def fetch_collection_products(collection_page_url, max_products=None):
    """
    Scrape products from one selected collection using its /products.json endpoint.

    Important:
    - If the page URL contains filter.v.availability=1, this function keeps only
      products where at least one variant is available.
    - max_products means maximum final kept products for that collection.
    """
    api_url = build_collection_api_url(collection_page_url)
    respect_availability_filter = is_page_filtered_for_availability(collection_page_url)

    all_products = []
    page = 1
    seen_ids = set()

    while True:
        params = {"limit": 250, "page": page}

        response = SESSION.get(api_url, params=params, timeout=30)
        response.raise_for_status()

        data = response.json()
        raw_products = data.get("products", []) or []

        if not raw_products:
            break

        for product in raw_products:
            product_id = product.get("id")
            if product_id in seen_ids:
                continue

            available = is_available_product(product)

            if respect_availability_filter and not available:
                continue

            seen_ids.add(product_id)
            all_products.append(product)

            if max_products is not None and len(all_products) >= max_products:
                return all_products

        page += 1
        time.sleep(REQUEST_DELAY_SECONDS)

    return all_products


# ============================================================
# MAIN
# ============================================================

def main():
    drive_service, sheets_service = get_google_services()

    all_rows = []

    print("\nStarting scrape...\n")

    for idx, collection_page_url in enumerate(COLLECTION_URLS, start=1):
        collection_handle = get_collection_handle(collection_page_url)
        print(f"[{idx}/{len(COLLECTION_URLS)}] Scraping collection: {collection_handle}")

        try:
            products = fetch_collection_products(
                collection_page_url=collection_page_url,
                max_products=MAX_PRODUCTS_PER_COLLECTION,
            )

            print(f"  -> Products kept: {len(products)}")

            rows = []
            total_products = len(products)
            for product_idx, product in enumerate(products, start=1):
                product_title = str(product.get("title", "")).strip() or str(product.get("id", ""))
                print(f"    Processing product {product_idx}/{total_products}: {product_title}")
                try:
                    row = build_row(
                        product=product,
                        collection_page_url=collection_page_url,
                        drive_service=drive_service,
                    )
                    rows.append(row)
                except Exception as product_error:
                    print(f"    Skipped one product due to error: {product_error}")

            all_rows.extend(rows)

        except Exception as e:
            print(f"  -> Failed for {collection_handle}: {e}")

        time.sleep(REQUEST_DELAY_SECONDS)

    deduped_rows = []
    seen_ids = set()
    for row in all_rows:
        product_id = str(row.get("id", "")).strip()
        if product_id and product_id in seen_ids:
            continue
        if product_id:
            seen_ids.add(product_id)
        deduped_rows.append(row)

    previous_rows = load_previous_rows_from_sheet(
        sheets_service=sheets_service,
        spreadsheet_id=SHEET_ID,
        sheet_name=SHEET_NAME,
    )
    deduped_rows = merge_with_previous(
        current_rows=deduped_rows,
        previous_rows=previous_rows,
        id_field="id",
        fieldnames=SALE_FIELDS,
    )

    save_sheet(
        sheets_service=sheets_service,
        spreadsheet_id=SHEET_ID,
        sheet_name=SHEET_NAME,
        rows=deduped_rows,
        fieldnames=SALE_FIELDS,
    )

    print("\nDone.")
    print(f"Updated Google Sheet tab {SHEET_NAME} with {len(deduped_rows)} rows")


if __name__ == "__main__":
    main()