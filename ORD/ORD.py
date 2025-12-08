from scraperSetUp import get_driver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import Select
from concurrent.futures import ThreadPoolExecutor, as_completed
import json
import time
from tabulate import tabulate
import threading
import re

def get_reaction_role_name(role_code):
    """
    Get reaction role name from code.
    
    Args:
        role_code: Integer code for reaction role
        
    Returns:
        String name of the reaction role
    """
    roles = {
        0: "UNSPECIFIED",
        1: "REACTANT",
        2: "REAGENT", 
        3: "SOLVENT",
        4: "CATALYST",
        5: "WORKUP",
        6: "INTERNAL_STANDARD",
        7: "AUTHENTIC_STANDARD",
        8: "PRODUCT",
        9: "BYPRODUCT",
        10: "SIDE_PRODUCT"
    }
    return roles.get(role_code, f"UNKNOWN_{role_code}")


# Thread-safe counter for progress tracking
class ProgressCounter:
    def __init__(self):
        self.lock = threading.Lock()
        self.completed = 0
        self.total = 0
    
    def increment(self):
        with self.lock:
            self.completed += 1
            return self.completed


# ============================================================
# FIXED: NO MORE CHROMEDRIVER BACKTRACE — SAFE PAGINATION
# ============================================================
def set_pagination_size(driver, page_size=100):
    """
    Attempt to set pagination size. If no pagination exists, skip silently.
    Prevents ChromeDriver backtrace dumps.
    """
    try:
        elements = driver.find_elements(By.CSS_SELECTOR, "select[name='pagination']")
        if not elements:
            # No pagination exists (most ORD pages now)
            return False

        pagination_select = elements[0]
        select = Select(pagination_select)

        available = [opt.get_attribute("value") for opt in select.options]
        if str(page_size) not in available:
            print(f"    Pagination exists but does not support {page_size}.")
            return False

        select.select_by_value(str(page_size))
        print(f"    Set pagination to {page_size} per page")
        time.sleep(1.0)
        return True

    except Exception as e:
        # Silent fallback (cleaner)
        print(f"    Pagination present but could not change: {e}")
        return False


def get_total_entries(driver):
    """Extract total number of entries from pagination text"""
    try:
        pagination_text = driver.find_element(
            By.CSS_SELECTOR, 
            "div.select"
        ).text
        # Extract number from "Showing X of Y entries"
        match = re.search(r'of (\d+) entries', pagination_text)
        if match:
            return int(match.group(1))
    except Exception as e:
        print(f"    Could not determine total entries: {e}")
    return None


def get_all_dataset_ids(page_size=100):
    """
    Get all dataset IDs from the browse page with pagination support.
    Now scans ALL pages if pagination exists.
    """
    driver = get_driver()
    try:
        print("Opening browser and navigating to browse page...")
        driver.get("https://open-reaction-database.org/browse")
        wait = WebDriverWait(driver, 15)
        
        wait.until(EC.presence_of_element_located((By.TAG_NAME, "body")))
        time.sleep(2)
        
        # Set pagination on browse page
        set_pagination_size(driver, page_size)
        time.sleep(1)  # Extra wait after setting pagination
        
        # Get total entries
        total_entries = get_total_entries(driver)
        if total_entries:
            print(f"Total dataset entries available: {total_entries}")
        
        all_dataset_ids = []
        page = 1
        prev_count = 0
        
        while True:
            print(f"Scanning browse page {page}...")
            
            # Wait for page to fully load
            time.sleep(1.5)
            
            # Find dataset links on current page
            try:
                dataset_links = driver.find_elements(By.CSS_SELECTOR, "a[href*='/dataset/ord_dataset-']")
            except:
                print("Could not find dataset links. Stopping.")
                break
            
            if not dataset_links:
                print(f"No datasets found on page {page}. Stopping pagination.")
                break
            
            # Extract dataset IDs from current page
            for link in dataset_links:
                try:
                    href = link.get_attribute('href')
                    if href:
                        dataset_id = href.split('/')[-1]
                        if dataset_id and dataset_id not in all_dataset_ids:
                            all_dataset_ids.append(dataset_id)
                except:
                    continue
            
            current_count = len(all_dataset_ids)
            print(f"  Found {len(dataset_links)} datasets on page {page} (total collected: {current_count})")
            
            # CHECK: If we've collected all entries, stop pagination
            if total_entries and current_count >= total_entries:
                print(f"✓ Collected all {total_entries} dataset entries. Stopping pagination.")
                break
            
            # CHECK: If no new datasets were added, we might be stuck
            if current_count == prev_count:
                print(f"No new datasets collected. Stopping pagination.")
                break
            
            prev_count = current_count
            
            # MORE ROBUST NEXT BUTTON DETECTION
            next_button_found = False
            
            # Try multiple approaches to find and click Next
            try:
                # Approach 1: Look for any clickable element with "next" or "Next"
                all_buttons = driver.find_elements(By.XPATH, "//*[contains(text(), 'Next') or contains(text(), 'next')]")
                
                for btn in all_buttons:
                    try:
                        if btn.is_displayed() and btn.is_enabled():
                            # Check it's not disabled
                            disabled = btn.get_attribute('disabled')
                            aria_disabled = btn.get_attribute('aria-disabled')
                            class_attr = btn.get_attribute('class') or ''
                            
                            if disabled or aria_disabled == 'true' or 'disabled' in class_attr.lower():
                                continue
                            
                            print(f"  Found Next button, clicking... (page {page} -> {page+1})")
                            # Scroll to button first
                            driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", btn)
                            time.sleep(0.5)
                            driver.execute_script("arguments[0].click();", btn)
                            time.sleep(2.5)  # Wait for page to load
                            page += 1
                            next_button_found = True
                            break
                    except:
                        continue
                
                if next_button_found:
                    continue
                    
            except Exception as e:
                print(f"  Error finding Next button: {e}")
            
            # If no next button found, we're done
            if not next_button_found:
                print(f"No more pages found. Total datasets collected: {len(all_dataset_ids)}")
                break
        
        print(f"\n✓ Found {len(all_dataset_ids)} total dataset IDs across {page} page(s)")
        return all_dataset_ids
        
    finally:
        driver.quit()


def get_all_reaction_ids_from_dataset(driver, dataset_id, max_reactions=None, page_size=100):
    """Get all reaction IDs from a dataset page with pagination support"""
    try:
        driver.get(f"https://open-reaction-database.org/dataset/{dataset_id}")
        wait = WebDriverWait(driver, 15)
        
        wait.until(EC.presence_of_element_located((By.TAG_NAME, "body")))
        time.sleep(2)
        
        set_pagination_size(driver, page_size)
        time.sleep(1)  # Extra wait after setting pagination
        
        # Get total entries
        total_entries = get_total_entries(driver)
        if total_entries:
            print(f"  Total entries in dataset: {total_entries}")
        
        all_reaction_ids = []
        page = 1
        prev_count = 0
        
        while True:
            print(f"  Scanning page {page} of dataset {dataset_id}...")
            
            # Wait for page to fully load
            time.sleep(1.5)
            
            # Find reaction links on current page
            try:
                reaction_links = driver.find_elements(By.CSS_SELECTOR, "a[href*='/id/ord-']")
                if not reaction_links:
                    reaction_links = driver.find_elements(By.XPATH, "//a[contains(@href, '/id/ord-')]")
            except:
                print("  Could not find reaction links. Stopping.")
                break
            
            if not reaction_links:
                print(f"  No reactions found on page {page}. Stopping pagination.")
                break
            
            # Extract reaction IDs from current page
            for link in reaction_links:
                try:
                    href = link.get_attribute('href')
                    if href:
                        reaction_id = href.split('/')[-1]
                        if reaction_id.startswith('ord-') and reaction_id not in all_reaction_ids:
                            all_reaction_ids.append(reaction_id)
                            
                            if max_reactions and len(all_reaction_ids) >= max_reactions:
                                print(f"  Reached reaction limit: {max_reactions}")
                                return all_reaction_ids[:max_reactions]
                except:
                    continue
            
            current_count = len(all_reaction_ids)
            print(f"    Found {len(reaction_links)} reactions on page {page} (total collected: {current_count})")
            
            # CHECK: If we've collected all entries, stop pagination
            if total_entries and current_count >= total_entries:
                print(f"  ✓ Collected all {total_entries} entries. Stopping pagination.")
                break
            
            # CHECK: If no new reactions were added, we might be stuck
            if current_count == prev_count:
                print(f"  No new reactions collected. Stopping pagination.")
                break
            
            prev_count = current_count
            
            # MORE ROBUST NEXT BUTTON DETECTION (same as dataset pagination)
            next_button_found = False
            
            # Try multiple approaches to find and click Next
            try:
                # Look for any clickable element with "next" or "Next"
                all_buttons = driver.find_elements(By.XPATH, "//*[contains(text(), 'Next') or contains(text(), 'next')]")
                
                for btn in all_buttons:
                    try:
                        if btn.is_displayed() and btn.is_enabled():
                            # Check it's not disabled
                            disabled = btn.get_attribute('disabled')
                            aria_disabled = btn.get_attribute('aria-disabled')
                            class_attr = btn.get_attribute('class') or ''
                            
                            if disabled or aria_disabled == 'true' or 'disabled' in class_attr.lower():
                                continue
                            
                            print(f"    Found Next button, clicking... (page {page} -> {page+1})")
                            # Scroll to button first
                            driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", btn)
                            time.sleep(0.5)
                            driver.execute_script("arguments[0].click();", btn)
                            time.sleep(2.5)  # Wait for page to load
                            page += 1
                            next_button_found = True
                            break
                    except:
                        continue
                
                if next_button_found:
                    continue
                    
            except Exception as e:
                print(f"    Error finding Next button: {e}")
            
            # If no next button found, we're done
            if not next_button_found:
                print(f"  No more pages. Total reactions: {len(all_reaction_ids)}")
                break
        
        print(f"Found {len(all_reaction_ids)} total reactions in dataset {dataset_id}")
        return all_reaction_ids
        
    except Exception as e:
        print(f"Error getting reactions from {dataset_id}: {e}")
        return []


def scrape_reaction_data(driver, reaction_id, max_retries=2):
    """Scrape the JSON data from a single reaction page with retries"""
    for attempt in range(max_retries):
        try:
            driver.get(f"https://open-reaction-database.org/id/{reaction_id}")
            
            WebDriverWait(driver, 10).until(
                lambda d: d.execute_script("return document.readyState") == "complete"
            )
            
            time.sleep(0.8)
            
            try:
                set_pagination_size(driver, page_size=100)
            except:
                pass
            
            button_selectors = [
                "div.full-record.button",
                "//div[contains(@class, 'full-record')]",
            ]
            
            button = None
            for selector in button_selectors:
                try:
                    if selector.startswith('//'):
                        button = WebDriverWait(driver, 5).until(
                            EC.element_to_be_clickable((By.XPATH, selector))
                        )
                    else:
                        button = WebDriverWait(driver, 5).until(
                            EC.element_to_be_clickable((By.CSS_SELECTOR, selector))
                        )
                    if button:
                        break
                except:
                    continue
            
            if not button:
                raise Exception("Could not find full-record button")
            
            driver.execute_script("arguments[0].click();", button)
            time.sleep(0.8)
            
            modal = WebDriverWait(driver, 8).until(
                EC.visibility_of_element_located((By.CSS_SELECTOR, "div.modal-container, .modal-container"))
            )
            
            data_element = WebDriverWait(driver, 8).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "div.data pre, .data pre, pre"))
            )
            
            json_text = data_element.text
            if not json_text or not json_text.strip().startswith('{'):
                raise Exception("Invalid JSON data")
            
            reaction_data = json.loads(json_text)
            
            try:
                close_button = driver.find_element(By.CSS_SELECTOR, "div.close, .close")
                driver.execute_script("arguments[0].click();", close_button)
            except:
                pass
            
            return {
                'reaction_id': reaction_id,
                'data': reaction_data,
                'success': True
            }
            
        except:
            if attempt < max_retries - 1:
                time.sleep(1)
                continue
    
    return {
        'reaction_id': reaction_id,
        'data': None,
        'success': False,
        'error': 'Max retries exceeded'
    }


def scrape_single_reaction_wrapper(reaction_id, progress_counter):
    driver = get_driver()
    try:
        result = scrape_reaction_data(driver, reaction_id)
        
        if result['success']:
            result['formatted_data'] = format_reaction_data(result)
        
        completed = progress_counter.increment()
        print(f"  [{completed}/{progress_counter.total}] {'✓' if result['success'] else '✗'} {reaction_id}")
        
        return result
    finally:
        driver.quit()


def scrape_single_dataset_parallel(dataset_id, max_reactions_per_dataset=None, max_workers=3, page_size=100):
    driver = get_driver()
    try:
        print(f"\n{'='*60}")
        print(f"Processing dataset: {dataset_id}")
        print(f"{'='*60}")
        
        reaction_ids = get_all_reaction_ids_from_dataset(driver, dataset_id, max_reactions_per_dataset, page_size)
        
        if not reaction_ids:
            print(f"No reactions found in dataset {dataset_id}")
            return {
                'dataset_id': dataset_id,
                'reactions': [],
                'total_reactions': 0,
                'successful_scrapes': 0,
                'error': 'No reactions found'
            }
        
        print(f"Found {len(reaction_ids)} reactions. Starting parallel scraping...")
        
        progress_counter = ProgressCounter()
        progress_counter.total = len(reaction_ids)
        
        reactions_data = []
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_reaction = {
                executor.submit(scrape_single_reaction_wrapper, reaction_id, progress_counter): reaction_id 
                for reaction_id in reaction_ids
            }
            
            for future in as_completed(future_to_reaction):
                try:
                    reactions_data.append(future.result())
                except Exception as e:
                    rid = future_to_reaction[future]
                    print(f"✗ Exception for {rid}: {e}")
                    reactions_data.append({
                        'reaction_id': rid,
                        'success': False,
                        'error': str(e)
                    })
        
        reaction_order = {rid: i for i, rid in enumerate(reaction_ids)}
        reactions_data.sort(key=lambda x: reaction_order.get(x['reaction_id'], 999999))
        
        successful = sum(1 for r in reactions_data if r['success'])
        
        print(f"\n✓ Dataset {dataset_id} complete: {successful}/{len(reactions_data)} reactions scraped")
        
        return {
            'dataset_id': dataset_id,
            'reactions': reactions_data,
            'total_reactions': len(reactions_data),
            'successful_scrapes': successful
        }
        
    finally:
        driver.quit()


def scrape_all_datasets_sequential(max_datasets=None, max_reactions_per_dataset=None, 
                                   max_workers_per_dataset=2, page_size=100):
    
    print("="*60)
    print("💡 REMINDER: You can customize these in main():")
    print("  • Number of datasets to scrape")
    print("  • Number of reactions per dataset")
    print("  • Parallel workers (speed)")
    print("  • Pagination size")
    print("="*60)

    print("="*60)
    print("STARTING WEB SCRAPING")
    print("="*60)


    
    print("\nStep 1: Getting all dataset IDs with pagination...")
    dataset_ids = get_all_dataset_ids(page_size=page_size)
    print(f"Found {len(dataset_ids)} total datasets\n")
    
    if max_datasets:
        dataset_ids = dataset_ids[:max_datasets]
        print(f"Limiting to first {max_datasets} Dataset IDs")
    
    all_results = []
    
    for i, dataset_id in enumerate(dataset_ids, 1):
        print(f"\n[{i}/{len(dataset_ids)}] Starting dataset: {dataset_id}")
        try:
            result = scrape_single_dataset_parallel(
                dataset_id, 
                max_reactions_per_dataset, 
                max_workers_per_dataset,
                page_size
            )
            all_results.append(result)
            print(f"[{i}/{len(dataset_ids)}] ✓ Completed dataset: {dataset_id}")
        except Exception as e:
            print(f"[{i}/{len(dataset_ids)}] ✗ Failed dataset {dataset_id}: {e}")
            all_results.append({'dataset_id': dataset_id, 'error': str(e)})
    
    total_reactions = sum(r.get('total_reactions', 0) for r in all_results)
    total_successful = sum(r.get('successful_scrapes', 0) for r in all_results)
    
    print(f"\n{'='*60}")
    print("SCRAPING COMPLETE!")
    print(f"{'='*60}")
    print(f"Datasets processed: {len(all_results)}")
    print(f"Total reactions found: {total_reactions}")
    print(f"Successfully scraped: {total_successful}")
    print(f"Failed: {total_reactions - total_successful}")
    print(f"{'='*60}\n")
    
    with open('reaction_database_scrape.json', 'w', encoding='utf-8') as f:
        json.dump(all_results, f, indent=2, ensure_ascii=False)
    
    print("✓ Results saved to reaction_database_scrape.json")
    return all_results


def format_reaction_data(reaction_data):
    """
    Format reaction data using dynamic reaction role mapping.
    Uses official ORD schema if available.
    """
    if not reaction_data or 'data' not in reaction_data:
        return None
    
    data = reaction_data['data']
    formatted = {
        'reaction_id': data.get('reactionId'),
        'success': reaction_data['success'],
        'inputsMap': []
    }
    
    if 'inputsMap' in data:
        for input_entry in data["inputsMap"]:
            tab_name = input_entry[0]
            input_data = input_entry[1]
            
            formatted_components = []
            for component in input_data.get("componentsList", []):
                identifiers = []
                for identifier in component.get("identifiersList", []):
                    if identifier.get("type") == 2:  # SMILES
                        identifiers.append({
                            "type": "SMILES",
                            "value": identifier.get("value")
                        })
                
                # USE DYNAMIC REACTION ROLE MAPPING
                role_code = component.get("reactionRole")
                reaction_role = get_reaction_role_name(role_code)
                
                formatted_components.append({
                    "identifiers": identifiers,
                    "reaction_role": reaction_role
                })
            
            formatted['inputsMap'].append([tab_name, {"components": formatted_components}])
    
    formatted['outcomes'] = []
    if 'outcomesList' in data:
        for outcome in data['outcomesList']:
            for product in outcome.get('productsList', []):
                identifiers = []
                for identifier in product.get('identifiersList', []):
                    if identifier.get("type") == 2:
                        identifiers.append({"type": "SMILES", "value": identifier.get("value")})
                
                formatted['outcomes'].append({
                    "identifiers": identifiers,
                    "reaction_role": "PRODUCT",
                    "is_desired_product": product.get('isDesiredProduct', False)
                })
    
    return formatted


def display_results_as_table(results):
    for dataset in results:
        if dataset.get('successful_scrapes', 0) > 0:
            print(f"\n{'='*100}")
            print(f"DATASET: {dataset['dataset_id']}")
            print(f"Total Reactions: {dataset['total_reactions']} | Successful: {dataset['successful_scrapes']}")
            print(f"{'='*100}\n")
            
            for reaction in dataset['reactions']:
                if reaction.get('success') and 'formatted_data' in reaction:
                    formatted = reaction['formatted_data']
                    
                    print(f"\nReaction ID: {formatted['reaction_id']}")
                    print("-" * 100)
                    
                    table_data = []
                    
                    for input_entry in formatted.get('inputsMap', []):
                        tab_name = input_entry[0]
                        for comp in input_entry[1].get('components', []):
                            if comp['identifiers']:
                                smiles = comp['identifiers'][0]['value']
                                table_data.append([
                                    tab_name, "INPUT",
                                    smiles[:60] + "..." if len(smiles) > 60 else smiles,
                                    comp['reaction_role'], ""
                                ])
                    
                    for product in formatted.get('outcomes', []):
                        if product['identifiers']:
                            smiles = product['identifiers'][0]['value']
                            table_data.append([
                                "Products", "OUTPUT",
                                smiles[:60] + "..." if len(smiles) > 60 else smiles,
                                "PRODUCT",
                                "✓" if product.get('is_desired_product') else ""
                            ])
                    
                    headers = ["Tab", "Type", "SMILES", "Role", "Desired"]
                    print(tabulate(table_data, headers=headers, tablefmt="grid"))
                    print()


def main():
    # ===== CONFIGURATION =====
    MAX_DATASETS = 2                     # None = scan ALL datasets with pagination
    MAX_REACTIONS_PER_DATASET = 10        # None = scan ALL reactions per dataset
    MAX_WORKERS_PER_DATASET = 3             # Parallel workers (1-3 recommended)
    PAGINATION_SIZE = 100                   # Items per page (10, 25, 50, or 100) ONLY
    # ========================= 
    
    start = time.time()
    
    results = scrape_all_datasets_sequential(
        max_datasets=MAX_DATASETS,
        max_reactions_per_dataset=MAX_REACTIONS_PER_DATASET,
        max_workers_per_dataset=MAX_WORKERS_PER_DATASET,
        page_size=PAGINATION_SIZE
    )
    
    print(f"\n⏱ Total time: {time.time() - start:.2f}s ({(time.time() - start)/60:.2f} minutes)")
    
    print("\n" + "="*100)
    print("EXTRACTED DATA IN TABLE FORMAT")
    print("="*100)
    
    display_results_as_table(results)
    
    formatted_results = {}
    for dataset in results:
        formatted_results[dataset['dataset_id']] = {
            'dataset_id': dataset['dataset_id'],
            'total_reactions': dataset['total_reactions'],
            'successful_scrapes': dataset['successful_scrapes'],
            'reactions': [
                r['formatted_data'] for r in dataset.get('reactions', [])
                if r.get('success') and 'formatted_data' in r
            ]
        }
    
    with open('ord_reaction_data.json', 'w', encoding='utf-8') as f:
        json.dump(formatted_results, f, indent=2, ensure_ascii=False)
    
    print("✓ Saved formatted results to ord_reaction_data.json")


if __name__ == "__main__":
    main()