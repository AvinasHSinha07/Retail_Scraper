import os

import camilla_scraper
import czarina_scraper
import kaftan_scraper
import aribella_scraper
import north_beach_scraper

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google.oauth2 import service_account
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build

CREDENTIALS_FILE = "credentials.json"
TOKEN_FILE = "token.json"
SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
]
SHEET_ID = "10fshaUtIwptjTXcWENynbNnNMA6HHyVdKPkEzdY635s"

COMBINED_SALE_SHEET = "Combined_sale_sheet"
COMBINED_COLLECTION_SHEET = "Combined_collection_sheet"

SALE_SOURCE_SHEETS = [
    "Camilla_sale_sheet",
    "Czarina_sale_sheet",
    "Kaftan_sale_sheet",
    "Aribella_sale_sheet",
    "NorthBeach_sale_sheet",
]

COLLECTION_SOURCE_SHEETS = [
    "Camilla_collection_sheet",
    "Czarina_collection_sheet",
    "Aribella_collection_sheet",
    "NorthBeach_collection_sheet",
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

COLLECTION_FIELDS = [
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


def get_google_services():
    creds = None

    if os.path.exists(CREDENTIALS_FILE):
        with open(CREDENTIALS_FILE, "r", encoding="utf-8") as f:
            creds_json = f.read()
        if '"type": "service_account"' in creds_json:
            creds = service_account.Credentials.from_service_account_file(
                CREDENTIALS_FILE,
                scopes=SCOPES,
            )

    if creds is None:
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

    sheets_service = build("sheets", "v4", credentials=creds)
    return sheets_service


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


def load_rows_from_sheet(sheets_service, spreadsheet_id, sheet_name):
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


def dedupe_rows(rows):
    seen = set()
    out = []

    for row in rows:
        product_url = str(row.get("product_url", "")).strip().lower()
        row_id = str(row.get("id", "") or row.get("Id", "")).strip().lower()
        brand = str(row.get("brand", "")).strip().lower()
        title = str(row.get("title", "")).strip().lower()

        if product_url:
            key = ("product_url", product_url)
        elif row_id:
            key = ("id", row_id)
        else:
            key = ("brand_title", brand, title)

        if key in seen:
            continue

        seen.add(key)
        out.append(row)

    return out


def collect_combined_rows(sheets_service, spreadsheet_id, source_tabs, fieldnames):
    existing_titles = get_sheet_titles(sheets_service, spreadsheet_id)
    combined_rows = []

    for tab in source_tabs:
        if tab not in existing_titles:
            continue

        rows = load_rows_from_sheet(sheets_service, spreadsheet_id, tab)
        for row in rows:
            normalized = {field: row.get(field, "") for field in fieldnames}
            # Skip fully blank records.
            if not any(str(v).strip() for v in normalized.values()):
                continue
            combined_rows.append(normalized)

    return dedupe_rows(combined_rows)


def rebuild_combined_sheets():
    print("\nRebuilding combined sheets...")
    sheets_service = get_google_services()

    combined_sale_rows = collect_combined_rows(
        sheets_service=sheets_service,
        spreadsheet_id=SHEET_ID,
        source_tabs=SALE_SOURCE_SHEETS,
        fieldnames=SALE_FIELDS,
    )
    save_sheet(
        sheets_service=sheets_service,
        spreadsheet_id=SHEET_ID,
        sheet_name=COMBINED_SALE_SHEET,
        rows=combined_sale_rows,
        fieldnames=SALE_FIELDS,
    )
    print(f"Updated {COMBINED_SALE_SHEET} with {len(combined_sale_rows)} rows")

    combined_collection_rows = collect_combined_rows(
        sheets_service=sheets_service,
        spreadsheet_id=SHEET_ID,
        source_tabs=COLLECTION_SOURCE_SHEETS,
        fieldnames=COLLECTION_FIELDS,
    )
    save_sheet(
        sheets_service=sheets_service,
        spreadsheet_id=SHEET_ID,
        sheet_name=COMBINED_COLLECTION_SHEET,
        rows=combined_collection_rows,
        fieldnames=COLLECTION_FIELDS,
    )
    print(f"Updated {COMBINED_COLLECTION_SHEET} with {len(combined_collection_rows)} rows")


def prompt_scraper_choice():
    while True:
        print("Choose scraper to run:")
        print("1. Camilla")
        print("2. Czarina")
        print("3. Kaftan")
        print("4. Aribella")
        print("5. North Beach")
        print("6. Run All")
        choice = input("Enter 1, 2, 3, 4, 5, or 6: ").strip()

        if choice in {"1", "2", "3", "4", "5", "6"}:
            return choice

        print("Invalid choice. Please enter 1, 2, 3, 4, 5, or 6.\n")


def prompt_max_products():
    while True:
        raw = input(
            "How many products do you want to scrape? "
            "(Enter a positive number, or 'all' for no limit): "
        ).strip().lower()

        if raw in {"all", "a", "none", "no-limit", "nolimit", ""}:
            return None

        try:
            value = int(raw)
            if value > 0:
                return value
            print("Please enter a number greater than 0, or 'all'.\n")
        except ValueError:
            print("Invalid input. Enter a positive number or 'all'.\n")


def run_selected_scraper(choice, max_products):
    if choice == "1":
        print("\nRunning Camilla scraper...")
        camilla_scraper.MAX_PRODUCTS_PER_COLLECTION = max_products
        camilla_scraper.main()
        return

    if choice == "2":
        print("\nRunning Czarina scraper...")
        czarina_scraper.MAX_PRODUCTS_PER_COLLECTION = max_products
        czarina_scraper.main()
        return

    if choice == "3":
        print("\nRunning Kaftan scraper...")
        kaftan_scraper.MAX_PRODUCTS_PER_COLLECTION = max_products
        kaftan_scraper.main()
        return

    if choice == "4":
        print("\nRunning Aribella scraper...")
        aribella_scraper.MAX_PRODUCTS_PER_COLLECTION = max_products
        aribella_scraper.main()
        return

    if choice == "5":
        print("\nRunning North Beach scraper...")
        north_beach_scraper.MAX_PRODUCTS_PER_COLLECTION = max_products
        north_beach_scraper.main()
        return

    print("\nRunning all scrapers...")

    print("\n[1/5] Camilla...")
    camilla_scraper.MAX_PRODUCTS_PER_COLLECTION = max_products
    camilla_scraper.main()

    print("\n[2/5] Czarina...")
    czarina_scraper.MAX_PRODUCTS_PER_COLLECTION = max_products
    czarina_scraper.main()

    print("\n[3/5] Kaftan...")
    kaftan_scraper.MAX_PRODUCTS_PER_COLLECTION = max_products
    kaftan_scraper.main()

    print("\n[4/5] Aribella...")
    aribella_scraper.MAX_PRODUCTS_PER_COLLECTION = max_products
    aribella_scraper.main()

    print("\n[5/5] North Beach...")
    north_beach_scraper.MAX_PRODUCTS_PER_COLLECTION = max_products
    north_beach_scraper.main()


def main():
    choice = prompt_scraper_choice()
    max_products = prompt_max_products()

    if max_products is None:
        print("No product limit selected. Scraping all available products.")
    else:
        print(f"Product limit set to {max_products} per collection.")

    run_selected_scraper(choice, max_products)
    rebuild_combined_sheets()


if __name__ == "__main__":
    main()
