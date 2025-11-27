"""Scraper for ICES data schemas."""

import csv
import re
import sys
from pathlib import Path
from typing import Any

from playwright.async_api import Page, async_playwright

# Increase CSV field size limit to handle large fields
# (e.g., value fields with many lines)
csv.field_size_limit(sys.maxsize)


def _read_existing_variables(csv_path: Path) -> set[str]:
    """
    Read variable names from an existing CSV file.

    Args:
        csv_path: Path to the CSV file

    Returns:
        Set of variable names that have already been scraped
    """
    existing_variables = set()
    if not csv_path.exists():
        return existing_variables

    try:
        row_count = 0
        with csv_path.open("r", newline="", encoding="utf-8") as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                row_count += 1
                if "variable_name" in row and row["variable_name"]:
                    existing_variables.add(row["variable_name"].strip())
                # Show progress every 10000 rows for large files
                if row_count % 10000 == 0:
                    print(
                        f"      - Reading CSV: {row_count} rows processed, "
                        f"{len(existing_variables)} unique variables found so far..."
                    )
        print(
            f"      - Finished reading CSV: {row_count} total rows, "
            f"{len(existing_variables)} unique variables"
        )
    except Exception as e:
        print(f"  - Warning: Could not read existing CSV file: {e}")
        return existing_variables

    return existing_variables


async def _extract_text_with_br_tags(locator) -> str:
    """
    Extract text from a locator, converting <br> tags to newlines.

    Also strips other newline characters.

    Args:
        locator: Playwright locator element

    Returns:
        Processed text string
    """
    try:
        # Get inner HTML to preserve <br> tags
        html_content = await locator.inner_html()

        # Replace <br> and <br/> tags (case insensitive, with or without closing)
        # with placeholder. Use a unique placeholder that won't appear in text.
        html_content = re.sub(
            r"<br\s*/?>", "__BR_TAG__", html_content, flags=re.IGNORECASE
        )

        # Strip all remaining HTML tags
        text = re.sub(r"<[^>]+>", "", html_content)

        # Remove all actual newline characters from the original text
        text = re.sub(r"\n+", " ", text)

        # Replace placeholder with newline (these are from <br> tags)
        text = text.replace("__BR_TAG__", "\n")

        # Clean up: strip each line and remove empty lines
        lines = [line.strip() for line in text.split("\n")]
        lines = [line for line in lines if line]  # Remove empty lines
        text = "\n".join(lines)

        return text.strip()
    except Exception:
        # Fallback to text_content if inner_html fails
        try:
            text = await locator.text_content() or ""
            # Remove newlines from fallback
            text = re.sub(r"\n+", " ", text)
            return text.strip()
        except Exception:
            return ""


async def scrape_ices_data(
    library_name: str,
    dataset_name: str,
    output_csv: str = "ices_data.csv",
    headed: bool = False,
) -> None:
    """
    Scrape ICES data schema information.

    Args:
        library_name: Name of the library to scrape (e.g., "DAD")
        dataset_name: Name of the dataset to scrape
            (e.g., "a. DADyyyy: Discharge Abstract Database -DAD")
        output_csv: Path to output CSV file (default: "ices_data.csv")
        headed: If True, run browser in headed mode (visible).
            Default is False (headless).
    """
    async with async_playwright() as p:
        # Launch browser in headless mode by default, or headed if requested
        launch_options = {"headless": not headed}
        if headed:
            # Only add slow_mo when in headed mode for better visibility
            launch_options["slow_mo"] = 100
        browser = await p.chromium.launch(**launch_options)
        context = await browser.new_context(
            viewport={"width": 1280, "height": 720},  # Set a reasonable window size
        )
        page = await context.new_page()

        # Handle new tabs/pages that might open - close them immediately
        async def handle_new_page(new_page: Page) -> None:
            """Close any new tabs that open."""
            await new_page.close()

        context.on("page", handle_new_page)

        try:
            print("[STEP 1] Starting scraper with parameters:")
            print(f"  - Library: {library_name}")
            print(f"  - Dataset: {dataset_name}")
            print(f"  - Output CSV: {output_csv}")

            # Navigate to homepage
            print("\n[STEP 2] Navigating to homepage...")
            await page.goto(
                "https://datadictionary.ices.on.ca/Applications/DataDictionary/Default.aspx",
            )
            await page.wait_for_load_state("networkidle")
            print(f"  - Current URL: {page.url}")

            # Click on the library (find link in table with exact text match)
            print(f"\n[STEP 3] Clicking on library: {library_name}")
            library_link = page.get_by_role("link", name=library_name, exact=True)
            await library_link.click(button="left", modifiers=[])

            # Wait for page to load completely and wait for dataset link to appear
            # This ensures we're actually on the library page
            print("  - Waiting for library page to load...")
            await page.wait_for_load_state("networkidle")
            await page.wait_for_timeout(
                1000
            )  # Additional wait for page to fully render

            # Wait for the dataset link to be visible. This confirms we're on the
            # library page
            print("  - Waiting for dataset link to appear...")
            dataset_link = page.get_by_role("link", name=dataset_name, exact=True)
            await dataset_link.wait_for(state="visible", timeout=30000)

            # Now remember the library page URL (after confirming we're on the right
            # page)
            library_page_url = page.url
            print(f"  - Library page URL saved: {library_page_url}")

            # Click on the dataset
            print(f"\n[STEP 4] Clicking on dataset: {dataset_name}")
            await dataset_link.click(button="left", modifiers=[])

            # Wait for page to load completely
            print("  - Waiting for dataset page to load...")
            await page.wait_for_load_state("networkidle")
            await page.wait_for_timeout(
                1000
            )  # Additional wait for page to fully render
            print(f"  - Current URL: {page.url}")

            # Initialize CSV file
            csv_path = Path(output_csv)
            file_exists = csv_path.exists()
            print(f"\n[STEP 5] Initializing CSV file: {csv_path}")
            print(f"  - File exists: {file_exists}")
            fieldnames = [
                "variable_name",
                "main_description",
                "main_type",
                "label",
                "type_length",
                "available_in",
                "format",
                "value",
                "links",
            ]

            # First time: list all variables from the dataset page
            print("\n[STEP 6] Collecting all variables from dataset page...")
            variables = await _collect_all_variables(page)
            print(f"  - Found {len(variables)} variables")
            if variables:
                print(f"  - First variable: {variables[0]['name']}")
                if len(variables) > 1:
                    print(f"  - Last variable: {variables[-1]['name']}")

            # Read existing variables from CSV if file exists
            existing_variables = set()
            if file_exists:
                print("\n[STEP 6.5] Reading existing variables from CSV...")
                existing_variables = _read_existing_variables(csv_path)
                print(f"  - Found {len(existing_variables)} already-scraped variables")

            # Filter out variables that have already been scraped
            variables_to_process = [
                v for v in variables if v["name"] not in existing_variables
            ]
            skipped_count = len(variables) - len(variables_to_process)
            starting_index = skipped_count + 1  # Resume counter from where we left off

            if skipped_count > 0:
                print(f"  - Skipping {skipped_count} already-scraped variables")
                print(
                    f"  - Resuming from variable {starting_index} of {len(variables)}"
                )
            print(f"  - Will process {len(variables_to_process)} new variables")

            # Now iterate through each variable
            with csv_path.open("a", newline="", encoding="utf-8") as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

                if not file_exists:
                    writer.writeheader()
                    print("  - CSV header written")

                if len(variables_to_process) == 0:
                    print(
                        "\n[STEP 7] All variables have already been scraped. "
                        "Nothing to do."
                    )
                else:
                    print(
                        f"\n[STEP 7] Processing {len(variables_to_process)} "
                        f"variables (resuming from {starting_index}/"
                        f"{len(variables)})..."
                    )
                    for idx, variable_info in enumerate(variables_to_process, 1):
                        current_index = starting_index + idx - 1
                        print(
                            f"\n  [{current_index}/{len(variables)}] "
                            f"Processing variable: {variable_info['name']}"
                        )
                        print(
                            f"    - Description: {variable_info['description'][:50]}..."
                            if len(variable_info["description"]) > 50
                            else f"    - Description: {variable_info['description']}"
                        )
                        print(f"    - Type: {variable_info['type']}")

                        # Scrape variable details
                        detailed_data = await _scrape_variable_details(
                            page, library_page_url, dataset_name, variable_info["name"]
                        )

                        # Write to CSV
                        row_data = {
                            "variable_name": variable_info["name"],
                            "main_description": variable_info["description"],
                            "main_type": variable_info["type"],
                            "label": detailed_data.get("label", ""),
                            "type_length": detailed_data.get("type_length", ""),
                            "available_in": detailed_data.get("available_in", ""),
                            "format": detailed_data.get("format", ""),
                            "value": detailed_data.get("value", ""),
                            "links": detailed_data.get("links", ""),
                        }
                        writer.writerow(row_data)
                        csvfile.flush()  # Ensure data is written immediately
                        print(f"    - ✓ Saved to CSV: {csv_path}")

                    print(
                        f"\n[STEP 8] Scraping completed! "
                        f"Processed {len(variables_to_process)} new variables "
                        f"(skipped {skipped_count} already-scraped, "
                        f"total in dataset: {len(variables)})"
                    )

        finally:
            await browser.close()


async def _collect_all_variables(page: Page) -> list[dict[str, str]]:
    """Collect all variables from the dataset page."""
    variables = []

    # Wait for the variables table to be visible
    print("      - Waiting for variables table...")
    await page.wait_for_selector('table:has-text("Variable Name")', timeout=10000)

    # Find the variables table
    variables_table = page.locator('table:has-text("Variable Name")').first

    # Get all rows in the table
    all_rows = variables_table.locator("tbody tr")
    row_count = await all_rows.count()
    print(f"      - Found {row_count} rows in table")

    collected_count = 0
    print("      - Processing rows (this may take a while for large tables)...")
    for i in range(row_count):
        # Show progress every 50 rows
        if i > 0 and i % 50 == 0:
            print(
                f"      - Progress: {i}/{row_count} rows processed, "
                f"{collected_count} variables collected so far..."
            )

        try:
            row = all_rows.nth(i)
            # Check if this row has a link (data row) and not th elements (header row)
            has_link = await row.locator("td a").count() > 0
            has_header = await row.locator("th").count() > 0

            if has_link and not has_header:
                # Get the variable name link from the first cell
                variable_link = row.locator("td").first.locator("a").first
                variable_name = await _extract_text_with_br_tags(variable_link)

                # Extract main view data
                cells = row.locator("td")
                cell_count = await cells.count()

                main_description = ""
                main_type = ""

                if cell_count >= 2:
                    description_cell = cells.nth(1)
                    main_description = await _extract_text_with_br_tags(
                        description_cell
                    )
                if cell_count >= 3:
                    type_cell = cells.nth(2)
                    main_type = await _extract_text_with_br_tags(type_cell)

                variables.append(
                    {
                        "name": variable_name,
                        "description": main_description,
                        "type": main_type,
                    }
                )
                collected_count += 1
        except Exception as e:
            # Skip problematic rows
            print(f"      - Warning: Skipped row {i} due to error: {e}")
            continue

    print(
        f"      - Successfully collected {collected_count} variables "
        f"from {row_count} rows"
    )
    return variables


async def _scrape_variable_details(
    page: Page,
    library_page_url: str,
    dataset_name: str,
    variable_name: str,
) -> dict[str, Any]:
    """
    Scrape detailed information for a single variable.

    Args:
        page: Playwright page object
        library_page_url: URL of the library page
        dataset_name: Name of the dataset to click
        variable_name: Name of the variable to scrape

    Returns:
        Dictionary containing detailed variable data
    """
    # Navigate back to library page
    print(f"    - Navigating to library page: {library_page_url}")
    await page.goto(library_page_url)
    await page.wait_for_load_state("networkidle")
    await page.wait_for_timeout(1000)  # Additional wait for page to fully render

    # Click on the dataset
    print(f"    - Clicking on dataset: {dataset_name}")
    dataset_link = page.get_by_role("link", name=dataset_name, exact=True)
    await dataset_link.click(button="left", modifiers=[])
    await page.wait_for_load_state("networkidle")
    await page.wait_for_timeout(1000)  # Additional wait for page to fully render

    # Click on the variable to see detailed view
    print(f"    - Clicking on variable: {variable_name}")
    variable_link = page.get_by_role("link", name=variable_name, exact=True)
    await variable_link.click(button="left", modifiers=[])
    await page.wait_for_load_state("networkidle")
    await page.wait_for_timeout(5000)  # Additional wait for page to fully render
    print(f"    - Current URL: {page.url}")

    # Extract detailed view data
    print("    - Extracting detailed view data...")
    detailed_data = await _extract_detailed_view(page)
    print(
        f"      - Label: {detailed_data.get('label', 'N/A')[:50]}..."
        if len(detailed_data.get("label", "")) > 50
        else f"      - Label: {detailed_data.get('label', 'N/A')}"
    )
    print(f"      - Type Length: {detailed_data.get('type_length', 'N/A')}")
    print(f"      - Format: {detailed_data.get('format', 'N/A')}")

    # Check for and click "more" buttons if available
    print("    - Checking for 'more' buttons...")
    await _handle_more_buttons(page)

    # Re-extract after clicking more buttons
    print("    - Re-extracting after clicking 'more' buttons...")
    detailed_data = await _extract_detailed_view(page)

    return detailed_data


async def _extract_detailed_view(page: Page) -> dict[str, Any]:
    """Extract data from the detailed variable view."""
    data = {}

    # Find all table rows in the detailed view
    # The structure appears to be: <tr><td>Label</td><td>Value</td></tr>
    rows = page.locator("table tr")

    row_count = await rows.count()
    print(f"        - Found {row_count} rows in detailed view table")
    extracted_fields = []

    for i in range(row_count):
        try:
            row = rows.nth(i)
            cells = row.locator("td")
            cell_count = await cells.count()

            if cell_count >= 2:
                label_cell = cells.first
                value_cell = cells.nth(1)

                # Extract label (simple text, no <br> processing needed)
                # Extract value with <br> tag processing
                try:
                    label_text = (await label_cell.text_content() or "").strip()
                    value_text = await _extract_text_with_br_tags(value_cell)
                except Exception as e:
                    # Skip this row if we can't extract text
                    print(f"        - Warning: Could not extract text: {e}")
                    continue

                # Map labels to data keys
                if "Label" in label_text:
                    data["label"] = value_text
                    extracted_fields.append("label")
                elif "Type Length" in label_text:
                    data["type_length"] = value_text
                    extracted_fields.append("type_length")
                elif "Available In" in label_text:
                    data["available_in"] = value_text
                    extracted_fields.append("available_in")
                elif "Format" in label_text:
                    data["format"] = value_text
                    extracted_fields.append("format")
                elif "Value" in label_text:
                    data["value"] = value_text
                    extracted_fields.append("value")
                elif "Links" in label_text:
                    data["links"] = value_text
                    extracted_fields.append("links")
        except Exception as e:
            # Skip problematic rows
            print(f"        - Warning: Skipped row {i} due to error: {e}")
            continue

    extracted_fields_str = ", ".join(extracted_fields) if extracted_fields else "none"
    print(f"        - Extracted fields: {extracted_fields_str}")
    return data


async def _handle_more_buttons(page: Page) -> None:
    """Handle 'more' buttons to expand truncated content."""
    # Look for "more" buttons (case insensitive)
    more_selectors = [
        'a:has-text("more")',
        'a:has-text("More")',
        'button:has-text("more")',
        'button:has-text("More")',
        'input[value*="more"]',
        'input[value*="More"]',
    ]

    clicked_count = 0
    for selector in more_selectors:
        more_buttons = page.locator(selector)
        count = await more_buttons.count()

        for i in range(count):
            button = more_buttons.nth(i)
            if await button.is_visible():
                try:
                    print(f"        - Clicking 'more' button (selector: {selector})")
                    await button.click()
                    await page.wait_for_timeout(2000)  # Wait for content to expand
                    clicked_count += 1
                except Exception as e:
                    # Button might have been removed or become unclickable
                    print(f"        - Warning: Could not click 'more' button: {e}")
                    pass

    if clicked_count == 0:
        print("        - No 'more' buttons found")
    else:
        print(f"        - Clicked {clicked_count} 'more' button(s)")
