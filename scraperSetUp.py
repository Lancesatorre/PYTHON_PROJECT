from selenium import webdriver
from selenium.webdriver.chrome.options import Options

def get_driver():
    """Create a stable Chrome driver using Selenium Manager (NO webdriver-manager)."""
    options = Options()

    # optional: headless mode
    # options.add_argument("--headless=new")

    # stability settings
    options.add_argument("--disable-gpu")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")

    # optional: reduce detection
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_experimental_option("useAutomationExtension", False)

    # Selenium Manager automatically installs the correct driver
    driver = webdriver.Chrome(options=options)

    driver.set_page_load_timeout(30)
    driver.implicitly_wait(10)

    return driver
