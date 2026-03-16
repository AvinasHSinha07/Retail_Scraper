import camilla_scraper
import czarina_scraper
import kaftan_scraper
import aribella_scraper


def prompt_scraper_choice():
    while True:
        print("Choose scraper to run:")
        print("1. Camilla")
        print("2. Czarina")
        print("3. Kaftan")
        print("4. Aribella")
        print("5. Run All")
        choice = input("Enter 1, 2, 3, 4, or 5: ").strip()

        if choice in {"1", "2", "3", "4", "5"}:
            return choice

        print("Invalid choice. Please enter 1, 2, 3, 4, or 5.\n")


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

    print("\nRunning all scrapers...")

    print("\n[1/4] Camilla...")
    camilla_scraper.MAX_PRODUCTS_PER_COLLECTION = max_products
    camilla_scraper.main()

    print("\n[2/4] Czarina...")
    czarina_scraper.MAX_PRODUCTS_PER_COLLECTION = max_products
    czarina_scraper.main()

    print("\n[3/4] Kaftan...")
    kaftan_scraper.MAX_PRODUCTS_PER_COLLECTION = max_products
    kaftan_scraper.main()

    print("\n[4/4] Aribella...")
    aribella_scraper.MAX_PRODUCTS_PER_COLLECTION = max_products
    aribella_scraper.main()


def main():
    choice = prompt_scraper_choice()
    max_products = prompt_max_products()

    if max_products is None:
        print("No product limit selected. Scraping all available products.")
    else:
        print(f"Product limit set to {max_products} per collection.")

    run_selected_scraper(choice, max_products)


if __name__ == "__main__":
    main()
