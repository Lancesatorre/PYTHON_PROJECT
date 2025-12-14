import json
import time
import re
import csv
import xlsxwriter
import xml.etree.ElementTree as ET
from urllib.parse import urljoin
import requests

# Selenium Imports
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup

# ==========================================
# COLOR CONFIGURATION (Console Output)
# ==========================================
class Colors:
    HEADER = '\033[95m'
    BLUE = '\033[94m'
    CYAN = '\033[96m'
    GREEN = '\033[92m'
    YELLOW = '\033[93m'
    RED = '\033[91m'
    RESET = '\033[0m'
    BOLD = '\033[1m'

# ==========================================
# CONFIGURATION
# ==========================================
ARCHIVE_URL = "https://kmt.vander-lingen.nl/archive"
JSON_FILENAME = "kmtOutput_Satorre&Gesim.json"
EXCEL_FILENAME = "kmtOutput_Satorre&Gesim.xlsx"
CSV_FILENAME = "kmtOutput_Satorre&Gesim.csv"
DELAY = 0
HEADLESS = True  # Set to False to see browser actions

# ==========================================
# DRIVER SETUP
# ==========================================
def get_driver():
    options = Options()
    if HEADLESS:
        options.add_argument("--headless")
        options.add_argument("--blink-settings=imagesEnabled=false")
        print(f"{Colors.YELLOW}[INFO] Running in HEADLESS mode (No Browser UI){Colors.RESET}")
    else:
        print(f"{Colors.YELLOW}[INFO] Running in VISIBLE mode (Browser UI Enabled){Colors.RESET}")

    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--window-size=1920,1080")
    options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
    
    prefs = {"profile.managed_default_content_settings.images": 2}
    options.add_experimental_option("prefs", prefs)
    
    return webdriver.Chrome(options=options)

def print_credits():
    print(f"{Colors.CYAN}{'=' * 50}")
    print(f"      {Colors.BOLD}KMT XML DATA MINER (OPTIMIZED){Colors.RESET}{Colors.CYAN}")
    print("=" * 50)
    print(f" {Colors.RESET}DEVELOPED BY:{Colors.CYAN}")
    print(f"   > {Colors.YELLOW}Lance Timothy Satorre{Colors.CYAN}")
    print(f"   > {Colors.YELLOW}Christian Dave Gesim{Colors.CYAN}")
    print("=" * 50)

# ==========================================
# HELPER: SESSION SYNC
# ==========================================
def create_synced_session(driver):
    session = requests.Session()
    selenium_user_agent = driver.execute_script("return navigator.userAgent;")
    session.headers.update({"User-Agent": selenium_user_agent})
    for cookie in driver.get_cookies():
        session.cookies.set(cookie['name'], cookie['value'])
    return session

# ==========================================
# DATA FORMATTING LOGIC
# ==========================================
def format_reaction_data(parsed_data, metadata):
    chemicals_list = parsed_data['chemicals']
    raw_smiles = parsed_data['raw_smiles']

    # Base dictionary
    output = {
        "Reaction_Data_Origin": metadata.get("Reaction_Data_Origin"), 
        "details_url": metadata.get("details_url"),
        "smiles": raw_smiles,
        "all_chemicals_raw": chemicals_list 
    }
    
    grouped_data = {}
    for item in chemicals_list:
        role = item.get('role', 'unknown').lower()
        if role not in grouped_data:
            grouped_data[role] = []
        grouped_data[role].append(item)

    for role, items in grouped_data.items():
        names = [i.get('name') for i in items if i.get('name')]
        output[role] = " + ".join(names)
        output[f"{role}_details"] = items
    
    return output

def parse_xml_data(xml_string):
    try:
        root = ET.fromstring(xml_string)
        reaction_smiles_node = root.find(".//reactionSmiles")
        raw_smiles = reaction_smiles_node.text if reaction_smiles_node is not None else ""
        participants_node = root.find(".//participants")
        chemicals_list = []

        if participants_node is not None:
            for molecule in participants_node.findall("molecule"):
                def get_text(tag):
                    node = molecule.find(tag)
                    return node.text if node is not None else ""

                entry = {
                    "name": get_text("name"),
                    "smiles": get_text("smiles"),
                    "role": get_text("role").lower(),
                    "inchiKey": get_text("inchiKey"),
                    "ratio": get_text("ratio"),
                    "notes": get_text("notes")
                }
                chemicals_list.append(entry)

        return {
            "chemicals": chemicals_list,
            "raw_smiles": raw_smiles
        }
    except Exception as e:
        return None

# ==========================================
# PAGE PROCESSOR
# ==========================================
def extract_list_items(driver):
    items = []
    soup = BeautifulSoup(driver.page_source, 'html.parser')
    detail_links = soup.find_all('a', string=re.compile("Details", re.IGNORECASE))
    
    for link in detail_links:
        item_url = link.get('href')
        if item_url:
            items.append({"url": item_url})
    return items

def process_reaction_data(driver, start_url, reaction_data_id):
    reaction_data_list = []
    current_url = start_url
    page_num = 1
    
    session = create_synced_session(driver)

    while current_url:
        print(f"{Colors.BLUE}{'-'*40}{Colors.RESET}")
        print(f"   -> {Colors.CYAN}Scanning Page {page_num}...{Colors.RESET}")
        try:
            driver.get(current_url)
            WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.TAG_NAME, "body")))
            
            for cookie in driver.get_cookies():
                session.cookies.set(cookie['name'], cookie['value'])

            found_items = extract_list_items(driver)
            if not found_items:
                break
            
            print(f"      Found {len(found_items)} reactions. Extracting data...")
            total_items = len(found_items)

            for i, item in enumerate(found_items):
                d_url = item['url']
                if not d_url.startswith("http"):
                    d_url = urljoin(current_url, d_url)

                current_index = i + 1
                
                if current_index == total_items:
                    counter_color = Colors.GREEN
                    status_msg = "Complete!"
                else:
                    counter_color = Colors.RED
                    status_msg = "Extracting..."

                print(f"      {counter_color}[{current_index}/{total_items}] {status_msg}{Colors.RESET}", end="\r")
                
                # --- FAST EXTRACTION START ---
                try:
                    detail_resp = session.get(d_url, timeout=10)
                    if detail_resp.status_code == 200:
                        detail_soup = BeautifulSoup(detail_resp.text, 'html.parser')
                        
                        xml_btn = detail_soup.find('a', href=re.compile(r'/data/transfer/export/'))
                        if not xml_btn:
                            xml_btn = detail_soup.find('a', string="XML")
                        
                        if xml_btn:
                            xml_href = xml_btn['href']
                            if not xml_href.startswith("http"):
                                xml_href = urljoin(d_url, xml_href)
                            
                            xml_resp = session.get(xml_href, timeout=10)
                            if xml_resp.status_code == 200:
                                parsed_data = parse_xml_data(xml_resp.text)
                                if parsed_data and parsed_data['chemicals']:
                                    meta = {
                                        "Reaction_Data_Origin": reaction_data_id,
                                        "details_url": d_url
                                    }
                                    final_entry = format_reaction_data(parsed_data, meta)
                                    reaction_data_list.append(final_entry)

                except Exception as e:
                    pass 
                # --- FAST EXTRACTION END ---

            if len(reaction_data_list) > 0:
                print(f"\n      {Colors.GREEN}✓ Batch Extracted Successfully!{Colors.RESET}")

            # Next Page Logic
            try:
                candidates = driver.find_elements(By.XPATH, "//a[contains(text(), 'Next') or contains(text(), '»')]")
                next_link = None
                for btn in candidates:
                    if btn.is_displayed():
                        href = btn.get_attribute("href")
                        if href and href != current_url and "#" not in href:
                            next_link = href
                            break
                if next_link:
                    current_url = next_link
                    page_num += 1
                else:
                    break
            except:
                break

        except Exception as e:
            print(f"      {Colors.RED}Page Error: {e}{Colors.RESET}")
            break
            
    return reaction_data_list

# ==========================================
# MAIN
# ==========================================
def main():
    print_credits()
    driver = get_driver()
    all_results = []
    
    try:
        print(f"Loading Archive: {Colors.BLUE}{ARCHIVE_URL}{Colors.RESET}")
        driver.get(ARCHIVE_URL)
        WebDriverWait(driver, 15).until(EC.presence_of_element_located((By.TAG_NAME, "body")))
        
        raw_links = []
        elements = driver.find_elements(By.XPATH, "//a[contains(text(), 'reaction data')]")
        for el in elements:
            href = el.get_attribute("href")
            if href: raw_links.append(href)
        
        total_found = len(raw_links)
        print(f"\n{Colors.GREEN}✓ Found {total_found} available Reaction_Data.{Colors.RESET}")
        print("-" * 50)

        # ----------------------------------------------------
        # NEW CUSTOMIZATION LOGIC
        # ----------------------------------------------------
        while True:
            try:
                # 0. Display Note
                print(f"{Colors.CYAN}NOTE: If you request more than 10 Reaction_Data, the system will automatically{Colors.RESET}")
                print(f"{Colors.CYAN}      process the first N items (e.g., 1 to 12) to save you from manual entry.{Colors.RESET}")
                print("-" * 50)

                # 1. Ask for limit
                limit_input = input(f"{Colors.YELLOW}How many Reaction_Data do you want to process? (Max {total_found}): {Colors.RESET}")
                limit_count = int(limit_input)

                if limit_count <= 0:
                    print(f"{Colors.RED}Error: Please enter a number greater than 0.{Colors.RESET}")
                    continue
                if limit_count > total_found:
                    print(f"{Colors.RED}Error: You cannot choose more than the total available Reaction_Data ({total_found}).{Colors.RESET}")
                    continue
                
                # ==========================================
                # AUTO-SELECT LOGIC (> 10)
                # ==========================================
                if limit_count > 10:
                    print(f"{Colors.CYAN}High volume selected (>10). Automatically processing the first {limit_count} Reaction_Data (1 to {limit_count}).{Colors.RESET}")
                    selected_indices = list(range(1, limit_count + 1))
                else:
                    # Manual Selection for small batches (<= 10)
                    print(f"{Colors.CYAN}You selected to process {limit_count} Reaction_Data.{Colors.RESET}")
                    print(f"{Colors.BOLD}Please enter exactly {limit_count} numbers.{Colors.RESET}")
                    
                    selection_input = input(f"{Colors.YELLOW}Which Reaction_Data do you want? Enter {limit_count} numbers (1-{total_found}) separated by commas: {Colors.RESET}")
                    
                    # Parse input into a list of integers
                    selected_indices = [int(x.strip()) for x in selection_input.split(",") if x.strip().isdigit()]

                    # Validation A: STRICT COUNT MATCH
                    # If user chose limit 4, they MUST enter exactly 4 numbers.
                    if len(selected_indices) != limit_count:
                        print(f"{Colors.RED}Error: You defined a limit of {limit_count}, but you entered {len(selected_indices)} numbers.{Colors.RESET}")
                        print(f"{Colors.RED}       Please try again and input exactly {limit_count} numbers.{Colors.RESET}")
                        continue # Restart the loop
                    
                    # Validation B: Range Check
                    invalid_indices = [x for x in selected_indices if x < 1 or x > total_found]
                    if invalid_indices:
                        print(f"{Colors.RED}Error: The following Reaction_Data numbers do not exist: {invalid_indices}. Range is 1 to {total_found}.{Colors.RESET}")
                        continue
                
                # If we get here, everything is valid
                final_links = [raw_links[i-1] for i in selected_indices]
                print(f"{Colors.GREEN}✓ Processing Reaction_Data: {selected_indices}{Colors.RESET}")
                break

            except ValueError:
                print(f"{Colors.RED}Invalid input. Please enter numbers only.{Colors.RESET}")

        # ----------------------------------------------------
        # END CUSTOMIZATION LOGIC
        # ----------------------------------------------------

        process_total = len(final_links)

        for i, link in enumerate(final_links, 1):
            # We use the index from the user's selection for display purposes
            original_index = raw_links.index(link) + 1
            print(f"\n[{i}/{process_total}] {Colors.HEADER}Reaction_Data #{original_index}: {link}{Colors.RESET}")
            
            results = process_reaction_data(driver, link, reaction_data_id=f"{original_index}_{link}")
            all_results.extend(results)
            
    finally:
        driver.quit()
        
        # ==========================================
        # 1. SAVE JSON
        # ==========================================
        if all_results:
            print(f"\n{Colors.CYAN}Saving JSON ({len(all_results)} records)...{Colors.RESET}")
            
            json_output = []
            for r in all_results:
                clean_r = r.copy()
                if 'all_chemicals_raw' in clean_r:
                    del clean_r['all_chemicals_raw']
                json_output.append(clean_r)

            with open(JSON_FILENAME, 'w', encoding='utf-8') as f:
                json.dump(json_output, f, indent=2, ensure_ascii=False)
            print(f"{Colors.GREEN}✓ JSON Saved to {JSON_FILENAME}{Colors.RESET}")

            # ==========================================
            # 2. SAVE CSV
            # ==========================================
            print(f"{Colors.CYAN}Saving CSV Report...{Colors.RESET}")
            
            with open(CSV_FILENAME, 'w', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                
                for entry in all_results:
                    # Updated Header Label
                    writer.writerow(["Reaction_Data_Origin:", entry.get('Reaction_Data_Origin', '')])
                    writer.writerow(["details_url:", entry.get('details_url', '')])
                    writer.writerow(["smiles:", entry.get('smiles', '')])
                    
                    writer.writerow([])
                    writer.writerow(["Role", "Name", "Smiles"]) 
                    
                    chemicals = entry.get('all_chemicals_raw', [])
                    if chemicals:
                        for chem in chemicals:
                            writer.writerow([
                                chem.get('role', ''),
                                chem.get('name', ''),
                                chem.get('smiles', '')
                            ])
                    else:
                        writer.writerow(["No chemicals found", "", ""])

                    writer.writerow([])
                    writer.writerow(["="*50]) 
                    writer.writerow([])

            print(f"{Colors.GREEN}✓ CSV Saved to {CSV_FILENAME}{Colors.RESET}")

            # ==========================================
            # 3. SAVE EXCEL
            # ==========================================
            print(f"{Colors.CYAN}Saving Styled Excel Report...{Colors.RESET}")
            
            workbook = xlsxwriter.Workbook(EXCEL_FILENAME)
            worksheet = workbook.add_worksheet()

            bold_label_fmt = workbook.add_format({'bold': True})
            table_header_fmt = workbook.add_format({
                'bold': True,
                'font_color': 'white',
                'bg_color': '#4472C4',
                'border': 1
            })
            data_fmt = workbook.add_format({'border': 1})
            divider_fmt = workbook.add_format({'bottom': 2})

            worksheet.set_column('A:A', 20)
            worksheet.set_column('B:B', 40)
            worksheet.set_column('C:C', 50)

            row = 0
            for entry in all_results:
                # Updated Header Label
                worksheet.write(row, 0, "Reaction_Data_Origin:", bold_label_fmt)
                worksheet.write(row, 1, entry.get('Reaction_Data_Origin', ''))
                row += 1
                
                worksheet.write(row, 0, "details_url:", bold_label_fmt)
                worksheet.write(row, 1, entry.get('details_url', ''))
                row += 1
                
                worksheet.write(row, 0, "smiles:", bold_label_fmt)
                worksheet.write(row, 1, entry.get('smiles', ''))
                row += 2 
                
                worksheet.write(row, 0, "Role", table_header_fmt)
                worksheet.write(row, 1, "Name", table_header_fmt)
                worksheet.write(row, 2, "Smiles", table_header_fmt)
                row += 1
                
                chemicals = entry.get('all_chemicals_raw', [])
                if chemicals:
                    for chem in chemicals:
                        worksheet.write(row, 0, chem.get('role', ''), data_fmt)
                        worksheet.write(row, 1, chem.get('name', ''), data_fmt)
                        worksheet.write(row, 2, chem.get('smiles', ''), data_fmt)
                        row += 1
                else:
                    worksheet.write(row, 0, "No chemicals found", data_fmt)
                    worksheet.write(row, 1, "", data_fmt)
                    worksheet.write(row, 2, "", data_fmt)
                    row += 1

                worksheet.write(row, 0, "", divider_fmt)
                worksheet.write(row, 1, "", divider_fmt)
                worksheet.write(row, 2, "", divider_fmt)
                
                row += 2 

            workbook.close()
            print(f"{Colors.GREEN}✓ Styled Excel Saved to {EXCEL_FILENAME}{Colors.RESET}")
        else:
            print(f"{Colors.RED}No results extracted.{Colors.RESET}")

        print(f"{Colors.GREEN}{Colors.BOLD}All Done.{Colors.RESET}")

if __name__ == "__main__":
    main()