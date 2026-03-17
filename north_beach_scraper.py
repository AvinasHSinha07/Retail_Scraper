import re
import html
import io
import json
import mimetypes
import os
from urllib.parse import urlparse, parse_qs

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google.oauth2 import service_account
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseUpload

BASE_URL = "https://www.northbeachboutique.net.au"

# COLLECTION SHEET SOURCE
COLLECTION_HANDLE = "kaftans"
COLLECTION_URL = f"{BASE_URL}/collections/{COLLECTION_HANDLE}"
COLLECTION_API_BASE = f"{COLLECTION_URL}/products.json"

# SALE SHEET SOURCE
SALE_PRODUCT_URL = (
    "https://www.northbeachboutique.net.au/collections/outlet/products/"
    "aliita-c82861?variant=46448391782659"
)
SALE_PRODUCT_API = (
    "https://www.northbeachboutique.net.au/collections/outlet/products/aliita-c82861.json"
)

HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Accept": "application/json,text/plain,*/*",
}

COLLECTION_OUTPUT_CSV = "collection_sheet.csv"
SALE_OUTPUT_CSV = "sale_sheet.csv"
PAGE_LIMIT = 250
MAX_PRODUCTS_PER_COLLECTION = None  # e.g. 20 for testing, or None for all

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

SALE_SHEET_NAME = "NorthBeach_sale_sheet"
COLLECTION_SHEET_NAME = "NorthBeach_collection_sheet"
SALE_IMAGE_PREFIX = "S_NorthBeach_"
COLLECTION_IMAGE_PREFIX = "C_NorthBeach_"


def create_session():
    session = requests.Session()

    retry = Retry(
        total=5,
        read=5,
        connect=5,
        backoff_factor=1,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
    )

    adapter = HTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    session.headers.update(HEADERS)
    return session


SESSION = create_session()


def clean_html(text):
    if not text:
        return ""

    text = html.unescape(str(text))
    text = re.sub(r"<br\s*/?>", "\n", text, flags=re.I)
    text = re.sub(r"</p>", "\n", text, flags=re.I)
    text = re.sub(r"</li>", "\n", text, flags=re.I)
    text = re.sub(r"</div>", "\n", text, flags=re.I)
    text = re.sub(r"<[^>]+>", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def money_to_float(value):
    if value in (None, "", "None"):
        return None
    try:
        return float(str(value).replace(",", "").strip())
    except Exception:
        return None


def dedupe_keep_order(values):
    seen = set()
    result = []

    for value in values:
        value = str(value).strip()
        if not value:
            continue
        if value.lower() in {"default", "default title"}:
            continue
        if value not in seen:
            seen.add(value)
            result.append(value)

    return result


def get_json(session, url, params=None):
    response = session.get(url, params=params, timeout=30)
    response.raise_for_status()
    return response.json()


def get_collection_name_from_url(url):
    path_parts = urlparse(url).path.strip("/").split("/")
    if "collections" in path_parts:
        idx = path_parts.index("collections")
        if idx + 1 < len(path_parts):
            return path_parts[idx + 1]
    return ""


def title_case_handle(handle):
    if not handle:
        return ""
    return handle.replace("-", " ").replace("_", " ").title()


def build_collection_product_url(product_handle, collection_handle):
    return f"{BASE_URL}/collections/{collection_handle}/products/{product_handle}"


def extract_variant_id_from_url(product_url):
    parsed = urlparse(product_url)
    query = parse_qs(parsed.query)
    variant_ids = query.get("variant", [])
    if variant_ids:
        return str(variant_ids[0]).strip()
    return ""


def get_selected_variant(product, product_url):
    variant_id = extract_variant_id_from_url(product_url)
    if not variant_id:
        return None

    for variant in product.get("variants", []) or []:
        if str(variant.get("id", "")).strip() == variant_id:
            return variant

    return None


def get_variant_image_url(variant):
    if not variant or not isinstance(variant, dict):
        return ""

    featured_image = variant.get("featured_image")
    if isinstance(featured_image, dict) and featured_image.get("src"):
        return featured_image["src"]

    image = variant.get("image")
    if isinstance(image, dict) and image.get("src"):
        return image["src"]

    return ""


def get_primary_image_url(product, selected_variant=None):
    variant_image = get_variant_image_url(selected_variant)
    if variant_image:
        return variant_image

    if isinstance(product.get("image"), dict) and product["image"].get("src"):
        return product["image"]["src"]

    images = product.get("images") or []
    if images:
        first = images[0]
        if isinstance(first, dict):
            return first.get("src", "")
        return str(first)

    return ""


def get_image_name(image_url):
    if not image_url:
        return ""
    return os.path.basename(urlparse(image_url).path)


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
    with open(CREDENTIALS_FILE, "r", encoding="utf-8") as credentials_file:
        creds_json = json.load(credentials_file)

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


def is_variant_available(variant):
    if not variant:
        return False

    if variant.get("available") is True:
        return True

    inventory_qty = variant.get("inventory_quantity")
    if isinstance(inventory_qty, (int, float)) and inventory_qty > 0:
        return True

    return False


def is_product_available(product):
    variants = product.get("variants", []) or []
    if not variants:
        return False

    for variant in variants:
        if is_variant_available(variant):
            return True

    return False


def get_size_option_key(product):
    """
    Detect which variant field corresponds to Size:
    option1 / option2 / option3
    """
    for option in product.get("options", []):
        name = str(option.get("name", "")).strip().lower()
        position = option.get("position")

        if name == "size" and position in (1, 2, 3):
            return f"option{position}"

    return None


def get_sizes(product, only_available=False):
    variants = product.get("variants", []) or []
    size_option_key = get_size_option_key(product)

    sizes = []

    # Best case: explicit Size option exists
    if size_option_key:
        for variant in variants:
            if only_available and not is_variant_available(variant):
                continue

            value = variant.get(size_option_key)
            if value:
                sizes.append(value)

    else:
        # Fallback: only use option1 to avoid pulling colour/pattern from option2/option3
        for variant in variants:
            if only_available and not is_variant_available(variant):
                continue

            value = variant.get("option1")
            if value:
                sizes.append(value)

    sizes = dedupe_keep_order(sizes)
    return ", ".join(sizes)


def get_style(product):
    for option in product.get("options", []):
        name = str(option.get("name", "")).strip().lower()
        if name == "style":
            values = dedupe_keep_order(option.get("values") or [])
            return ", ".join(values)

    tags = product.get("tags", [])
    if isinstance(tags, str):
        tags = [t.strip() for t in tags.split(",") if t.strip()]

    styles = []
    for tag in tags:
        low = str(tag).strip().lower()
        if low.startswith("style:"):
            styles.append(str(tag).split(":", 1)[1].strip())
        elif low.startswith("style_"):
            styles.append(str(tag).split("_", 1)[1].strip())
        elif low.startswith("style-"):
            styles.append(str(tag).split("-", 1)[1].strip())

    styles = dedupe_keep_order(styles)
    return ", ".join(styles)


def get_collection_price(product):
    variants = product.get("variants", []) or []

    available_prices = [
        money_to_float(v.get("price"))
        for v in variants
        if is_variant_available(v)
    ]
    available_prices = [p for p in available_prices if p is not None]

    if available_prices:
        return min(available_prices)

    prices = [money_to_float(v.get("price")) for v in variants]
    prices = [p for p in prices if p is not None]
    return min(prices) if prices else None


def get_sale_price_summary(product, selected_variant=None):
    if selected_variant:
        sale_price = money_to_float(selected_variant.get("price"))
        original_price = money_to_float(selected_variant.get("compare_at_price"))

        if original_price is None:
            original_price = sale_price

        discount_percent = 0
        if (
            original_price is not None
            and sale_price is not None
            and original_price > 0
            and sale_price < original_price
        ):
            discount_percent = round(((original_price - sale_price) / original_price) * 100, 2)

        return original_price, sale_price, discount_percent

    variants = product.get("variants", []) or []

    prices = [money_to_float(v.get("price")) for v in variants]
    prices = [p for p in prices if p is not None]

    compare_prices = [money_to_float(v.get("compare_at_price")) for v in variants]
    compare_prices = [p for p in compare_prices if p is not None and p > 0]

    sale_price = min(prices) if prices else None
    original_price = max(compare_prices) if compare_prices else sale_price

    discount_percent = 0
    if (
        original_price is not None
        and sale_price is not None
        and original_price > 0
        and sale_price < original_price
    ):
        discount_percent = round(((original_price - sale_price) / original_price) * 100, 2)

    return original_price, sale_price, discount_percent


def normalize_collection_product(product, collection_handle, source_link, drive_service=None):
    image_url = get_primary_image_url(product)
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
        "store_link": f"{BASE_URL}/collections/{collection_handle}",
        "title": product.get("title", ""),
        "price": get_collection_price(product),
        "description": clean_html(product.get("body_html", "")),
        "Size": get_sizes(product, only_available=True),
        "Style": get_style(product),
        "image_url": image_url,
        "image": image_name,
        "product_url": build_collection_product_url(product.get("handle", ""), collection_handle),
        "is_available": "1" if is_product_available(product) else "0",
        "source_link": source_link,
    }
    return row


def normalize_sale_product(product, product_url, source_link, drive_service=None):
    selected_variant = get_selected_variant(product, product_url)
    image_url = get_primary_image_url(product, selected_variant=selected_variant)
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
            image_prefix=SALE_IMAGE_PREFIX,
        )
        image_name = upload_image_to_drive(
            service=drive_service,
            image_url=image_url,
            filename=target_name,
            folder_id=DRIVE_FOLDER_ID,
        )

    original_price, sale_price, discount_percent = get_sale_price_summary(
        product,
        selected_variant=selected_variant,
    )

    collection_handle = get_collection_name_from_url(product_url)

    row = {
        "id": product.get("id", ""),
        "brand": product.get("vendor", ""),
        "title": product.get("title", ""),
        "original_price": original_price,
        "sale_price": sale_price,
        "discount_percent": discount_percent,
        "site_url": BASE_URL,
        "image_url": image_url,
        "image": image_name,
        "created_at": product.get("created_at", ""),
        "product_url": product_url,
        "Style": get_style(product),
        "Collection Name": title_case_handle(collection_handle),
        "Sizes": get_sizes(product, only_available=True),
        "Description": clean_html(product.get("body_html", "")),
        "is_available": "1" if (
            is_variant_available(selected_variant)
            if selected_variant is not None
            else is_product_available(product)
        ) else "0",
        "source_link": source_link,
    }
    return row


def fetch_all_collection_products(session, collection_api_base, page_limit=250, max_products=None):
    all_products = []
    seen_ids = set()
    page = 1

    while True:
        params = {
            "limit": page_limit,
            "page": page,
        }

        print(f"Fetching collection page {page} ...")
        data = get_json(session, collection_api_base, params=params)
        products = data.get("products", [])

        print(f"Products on page {page}: {len(products)}")

        if not products:
            break

        for product in products:
            product_id = product.get("id")
            if product_id not in seen_ids:
                seen_ids.add(product_id)
                all_products.append(product)

                if max_products is not None and len(all_products) >= max_products:
                    return all_products

        if len(products) < page_limit:
            break

        page += 1

    return all_products


def build_collection_rows(session, drive_service=None):
    print("\nStarting North Beach collection scrape...")

    products = fetch_all_collection_products(
        session=session,
        collection_api_base=COLLECTION_API_BASE,
        page_limit=PAGE_LIMIT,
        max_products=MAX_PRODUCTS_PER_COLLECTION,
    )

    # Exclude fully sold out products
    products = [p for p in products if is_product_available(p)]

    source_link = COLLECTION_API_BASE

    rows = []
    for idx, product in enumerate(products, start=1):
        print(f"[COLLECTION {idx}/{len(products)}] {product.get('title', '')}")
        rows.append(
            normalize_collection_product(
                product=product,
                collection_handle=COLLECTION_HANDLE,
                source_link=source_link,
                drive_service=drive_service,
            )
        )

    return rows


def build_sale_rows(session, drive_service=None):
    print("\nStarting North Beach sale scrape...")

    data = get_json(session, SALE_PRODUCT_API)
    product = data.get("product", {})

    row = normalize_sale_product(
        product=product,
        product_url=SALE_PRODUCT_URL,
        source_link=SALE_PRODUCT_API,
        drive_service=drive_service,
    )

    return [row]

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

    sale_rows = build_sale_rows(SESSION, drive_service=drive_service)
    collection_rows = build_collection_rows(SESSION, drive_service=drive_service)

    should_compare = COMPARE_WITH_PREVIOUS and MAX_PRODUCTS_PER_COLLECTION is None
    if should_compare:
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
    elif COMPARE_WITH_PREVIOUS:
        print(
            "Skipping previous-row merge because MAX_PRODUCTS_PER_COLLECTION is set "
            "(test/partial run)."
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