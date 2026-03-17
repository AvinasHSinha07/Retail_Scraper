import html
import io
import json
import mimetypes
import os
import re
import time
from urllib.parse import urlparse

import requests

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google.oauth2 import service_account
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseUpload

BASE_URL = "https://czarinaworld.com"

COLLECTIONS = {
    "sale": {
        "handle": "sale",
        "url": f"{BASE_URL}/collections/sale",
        "api": f"{BASE_URL}/collections/sale/products.json",
        "sheet_name": "Czarina_sale_sheet",
        "image_prefix": "S_Czarina_",
    },
    "new-arrivals": {
        "handle": "new-arrivals",
        "url": f"{BASE_URL}/collections/new-arrivals",
        "api": f"{BASE_URL}/collections/new-arrivals/products.json",
        "sheet_name": "Czarina_collection_sheet",
        "image_prefix": "C_Czarina_",
    },
}

HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Accept": "application/json,text/html,*/*",
}

# -------------------------------------------------
# USER SETTINGS
# -------------------------------------------------

# Use 5 for testing, 500 for production, or None for all
MAX_PRODUCTS_PER_COLLECTION = None

# True = upload image directly to Google Drive
UPLOAD_IMAGES_TO_DRIVE = True

# If True, products missing in the next full run are appended with is_available=0
# Keep this FALSE while testing with MAX_PRODUCTS_PER_COLLECTION = 5
COMPARE_WITH_PREVIOUS = True

REQUEST_DELAY = 0

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


def clean_html_to_text(html_text):
    if not html_text:
        return ""

    text = re.sub(r"<br\s*/?>", "\n", html_text, flags=re.I)
    text = re.sub(r"</p>|</li>|</ul>|</ol>", "\n", text, flags=re.I)
    text = re.sub(r"<[^>]+>", " ", text)
    text = html.unescape(text)
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n+", "\n", text)

    return text.strip()


def unique_preserve(items):
    seen = set()
    out = []
    for item in items:
        if item not in seen:
            seen.add(item)
            out.append(item)
    return out


def normalize_numeric_strings(values):
    out = []
    for value in values:
        if value is None:
            continue
        value_str = str(value).strip()
        if value_str == "" or value_str.lower() == "none":
            continue
        out.append(value_str)
    return out


def get_variant_price_values(product):
    variants = product.get("variants", []) or []

    price_values = unique_preserve(
        normalize_numeric_strings([variant.get("price") for variant in variants])
    )

    compare_values = unique_preserve(
        normalize_numeric_strings([variant.get("compare_at_price") for variant in variants])
    )

    return price_values, compare_values


def join_or_single(values):
    if not values:
        return ""
    if len(values) == 1:
        return values[0]
    return " | ".join(values)


def compute_discount_percent_strict(product):
    price_values, compare_values = get_variant_price_values(product)

    if len(price_values) != 1 or len(compare_values) != 1:
        return ""

    try:
        price = float(price_values[0])
        compare_price = float(compare_values[0])
    except Exception:
        return ""

    if compare_price <= 0 or compare_price <= price:
        return ""

    discount = ((compare_price - price) / compare_price) * 100
    return f"{discount:.2f}"


def get_sizes_from_api(product):
    sizes = []

    for option in product.get("options", []) or []:
        if str(option.get("name", "")).strip().lower() == "size":
            for value in option.get("values", []) or []:
                if value is not None and str(value).strip():
                    sizes.append(str(value).strip())

    if not sizes:
        for variant in product.get("variants", []) or []:
            value = variant.get("option1")
            if value is not None and str(value).strip():
                sizes.append(str(value).strip())

    seen = set()
    out = []
    for size in sizes:
        key = size.lower()
        if key not in seen:
            seen.add(key)
            out.append(size)

    return ", ".join(out)


def get_first_image_src(product):
    images = product.get("images", []) or []
    if images:
        return images[0].get("src", "") or ""
    return ""


def get_product_url(product):
    handle = product.get("handle", "")
    if not handle:
        return ""
    return f"{BASE_URL}/products/{handle}"


def product_is_available(product):
    if "available" in product:
        try:
            return 1 if bool(product.get("available")) else 0
        except Exception:
            pass

    variants = product.get("variants", []) or []
    for variant in variants:
        if variant.get("available") is True:
            return 1

        inventory_qty = variant.get("inventory_quantity")
        if isinstance(inventory_qty, (int, float)) and inventory_qty > 0:
            return 1

    return 0


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


def get_google_services():
    """
    Supports:
    1) Installed-app OAuth credentials.json
    2) Service-account JSON credentials.json
    """
    with open(CREDENTIALS_FILE, "r", encoding="utf-8") as f:
        creds_json = json.load(f)

    # Service account JSON
    if creds_json.get("type") == "service_account":
        creds = service_account.Credentials.from_service_account_file(
            CREDENTIALS_FILE,
            scopes=SCOPES,
        )
        drive_service = build("drive", "v3", credentials=creds)
        sheets_service = build("sheets", "v4", credentials=creds)
        return drive_service, sheets_service

    # Installed app / OAuth client secrets JSON
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
    response = requests.get(image_url, headers=HEADERS, timeout=60)
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


def fetch_collection_products(collection_api_url, max_products=None, limit=250):
    all_products = []
    page = 1
    seen = set()

    while True:
        params = {"limit": limit, "page": page}
        response = requests.get(collection_api_url, headers=HEADERS, params=params, timeout=30)
        response.raise_for_status()

        data = response.json()
        products = data.get("products", [])

        print(f"Page {page}: fetched {len(products)} products from {collection_api_url}")

        if not products:
            break

        for product in products:
            pid = product.get("id")
            if pid not in seen:
                seen.add(pid)
                all_products.append(product)

                if max_products is not None and len(all_products) >= max_products:
                    print(f"Reached max_products={max_products}")
                    return all_products

        if len(products) < limit:
            break

        page += 1
        time.sleep(REQUEST_DELAY)

    return all_products


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


def build_sale_rows(products, collection_key, drive_service=None):
    rows = []
    collection_url = COLLECTIONS[collection_key]["url"]
    collection_handle = COLLECTIONS[collection_key]["handle"]

    for idx, product in enumerate(products, start=1):
        print(f"[SALE {idx}/{len(products)}] {product.get('title', '')}")

        price_values, compare_values = get_variant_price_values(product)
        image_src = get_first_image_src(product)

        image_name = ""
        if (
            UPLOAD_IMAGES_TO_DRIVE
            and image_src
            and drive_service
            and DRIVE_FOLDER_ID
            and DRIVE_FOLDER_ID != "PUT_YOUR_DRIVE_FOLDER_ID_HERE"
        ):
            target_name = build_image_filename(
                product_title=product.get("title", ""),
                collection_key=collection_key,
                image_url=image_src,
            )
            image_name = upload_image_to_drive(
                service=drive_service,
                image_url=image_src,
                filename=target_name,
                folder_id=DRIVE_FOLDER_ID,
            )

        row = {
            "id": product.get("id", ""),
            "brand": product.get("vendor", ""),
            "title": product.get("title", ""),
            "original_price": join_or_single(compare_values),
            "sale_price": join_or_single(price_values),
            "discount_percent": compute_discount_percent_strict(product),
            "site_url": collection_url,
            "image_url": image_src,
            "image": image_name,
            "created_at": product.get("created_at", ""),
            "product_url": get_product_url(product),
            "Style": product.get("product_type", ""),
            "Collection Name": collection_handle,
            "Sizes": get_sizes_from_api(product),
            "Description": clean_html_to_text(product.get("body_html", "")),
            "is_available": str(product_is_available(product)),
            "source_link": collection_url,
        }
        rows.append(row)

    return rows


def build_collection_rows(products, collection_key, drive_service=None):
    rows = []
    collection_url = COLLECTIONS[collection_key]["url"]

    for idx, product in enumerate(products, start=1):
        print(f"[COLLECTION {idx}/{len(products)}] {product.get('title', '')}")

        price_values, _ = get_variant_price_values(product)
        image_src = get_first_image_src(product)

        image_name = ""
        if (
            UPLOAD_IMAGES_TO_DRIVE
            and image_src
            and drive_service
            and DRIVE_FOLDER_ID
            and DRIVE_FOLDER_ID != "PUT_YOUR_DRIVE_FOLDER_ID_HERE"
        ):
            target_name = build_image_filename(
                product_title=product.get("title", ""),
                collection_key=collection_key,
                image_url=image_src,
            )
            image_name = upload_image_to_drive(
                service=drive_service,
                image_url=image_src,
                filename=target_name,
                folder_id=DRIVE_FOLDER_ID,
            )

        row = {
            "Id": product.get("id", ""),
            "brand": product.get("vendor", ""),
            "store_link": collection_url,
            "title": product.get("title", ""),
            "price": join_or_single(price_values),
            "description": clean_html_to_text(product.get("body_html", "")),
            "Size": get_sizes_from_api(product),
            "Style": product.get("product_type", ""),
            "image_url": image_src,
            "image": image_name,
            "product_url": get_product_url(product),
            "is_available": str(product_is_available(product)),
            "source_link": collection_url,
        }
        rows.append(row)

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

    # SALE
    sale_products = fetch_collection_products(
        COLLECTIONS["sale"]["api"],
        max_products=MAX_PRODUCTS_PER_COLLECTION,
        limit=250,
    )

    sale_rows = build_sale_rows(
        products=sale_products,
        collection_key="sale",
        drive_service=drive_service,
    )

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

    save_sheet(
        sheets_service=sheets_service,
        spreadsheet_id=SHEET_ID,
        sheet_name=COLLECTIONS["sale"]["sheet_name"],
        rows=sale_rows,
        fieldnames=sale_fields,
    )
    print(f"Updated Google Sheet tab {COLLECTIONS['sale']['sheet_name']} with {len(sale_rows)} rows")

    # COLLECTION
    new_arrivals_products = fetch_collection_products(
        COLLECTIONS["new-arrivals"]["api"],
        max_products=MAX_PRODUCTS_PER_COLLECTION,
        limit=250,
    )

    collection_rows = build_collection_rows(
        products=new_arrivals_products,
        collection_key="new-arrivals",
        drive_service=drive_service,
    )

    previous_collection_rows = load_previous_rows_from_sheet(
        sheets_service=sheets_service,
        spreadsheet_id=SHEET_ID,
        sheet_name=COLLECTIONS["new-arrivals"]["sheet_name"],
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
        sheet_name=COLLECTIONS["new-arrivals"]["sheet_name"],
        rows=collection_rows,
        fieldnames=collection_fields,
    )
    print(f"Updated Google Sheet tab {COLLECTIONS['new-arrivals']['sheet_name']} with {len(collection_rows)} rows")


if __name__ == "__main__":
    main()