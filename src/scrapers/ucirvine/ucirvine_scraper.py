import json
import uuid
import time
import re
import os
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager

# Função para converter tamanhos de arquivos em MB
def parse_size_to_mb(size_str):
    size_str = size_str.upper().replace(',', '.')
    number = re.search(r"(\d+\.?\d*)", size_str)
    if not number: return 0.0
    val = float(number.group(1))
    if "GB" in size_str: return val * 1024
    if "MB" in size_str: return val
    if "KB" in size_str: return val / 1024
    return val / (1024 * 1024) if "B" in size_str else val

# Função para gerar prefixo de log com timestamp
def get_log_prefix():
    return datetime.now().strftime('%Y-%m-%d %H:%M:%S')

# Função principal do scraper UCIrvine
def extrair_ucirvine(config, existing_titles):
    chrome_options = Options()

    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=chrome_options)
    driver.maximize_window()

    max_per_subject = config.get('max_datasets_per_subject', 250)
    all_extracted = []

    try:
        for subject in config.get('subjects', []):
            print(f"{get_log_prefix()} - INFO - Scraper Iniciado: UCIrvine | Area: {subject}")

            subject_url = f"https://archive.ics.uci.edu/datasets?subjectArea={subject.replace(' ', '+')}"
            driver.get(subject_url)

            wait = WebDriverWait(driver, 20)
            time.sleep(5)

            links = []
            while len(links) < max_per_subject:
                current_cards = driver.find_elements(By.CSS_SELECTOR, "h2.text-primary a")
                for card in current_cards:
                    link = card.get_attribute("href")
                    if link not in links:
                        links.append(link)

                if len(links) >= max_per_subject: break

                try:
                    next_button = driver.find_element(By.XPATH, "//button[@aria-label='Next Page']")
                    if next_button.is_enabled():
                        next_button.click()
                        time.sleep(3)
                    else:
                        break
                except:
                    break

            links_to_process = links[:max_per_subject]

            for link in links_to_process:
                driver.get(link)
                time.sleep(2)

                try:
                    title_element = driver.find_element(By.TAG_NAME, "h1")
                    title = title_element.text.strip()

                    if title.lower().strip() in existing_titles:

                        continue

                    info_elements = driver.find_elements(By.CSS_SELECTOR, "p.svelte-1xc1tf7")
                    dataset_info = " ".join([el.text.strip() for el in info_elements if el.text.strip()])

                    # Donated Year
                    try:
                        donation_text = driver.find_element(By.CSS_SELECTOR, "h2.text-primary-content").text
                        year = donation_text.split('/')[-1] if '/' in donation_text else "N/A"
                    except:
                        year = "N/A"

                    try:
                        citations_el = driver.find_element(By.XPATH, "//span[contains(text(), 'citations')]")
                        citations = citations_el.text.split()[0]
                    except:
                        citations = "0"

                    total_size_mb = 0.0
                    try:
                        size_cells = driver.find_elements(By.CSS_SELECTOR, "table tbody tr td:nth-child(2)")
                        for cell in size_cells:
                            total_size_mb += parse_size_to_mb(cell.text)
                        file_size_display = f"{round(total_size_mb, 2)} MB" if total_size_mb > 0 else "N/A"
                    except:
                        file_size_display = "N/A"

                    # Grid Meta
                    metadata = {}
                    grid_items = driver.find_elements(By.CSS_SELECTOR, "div.grid-cols-8 > div, div.grid-cols-12 > div")
                    for item in grid_items:
                        try:
                            lbl = item.find_element(By.TAG_NAME, "h1").text.strip()
                            val = item.find_element(By.TAG_NAME, "p").text.strip()
                            metadata[lbl] = val
                        except:
                            continue

                    dataset_obj = {
                        "id_webscarping": "UCIrvine",
                        "url": link,
                        "subject_Area": subject,
                        "title": title,
                        "dataset_information": dataset_info,
                        "creatores": [p.text for p in
                                      driver.find_elements(By.CSS_SELECTOR, "div.flex.flex-col p.font-semibold")],
                        "year_publicacion": year,
                        "keywords": ", ".join(
                            [a.text for a in driver.find_elements(By.CSS_SELECTOR, "div.flex.flex-wrap.gap-2 a")]),
                        "dataset_characteristics": metadata.get("Dataset Characteristics", "N/A"),
                        "associated_tasks": metadata.get("Associated Tasks", "N/A"),
                        "feature_type": metadata.get("Feature Type", "N/A"),
                        "n_instances": metadata.get("# Instances", "N/A"),
                        "n_features": metadata.get("# Features", "N/A"),
                        "cites": int(citations) if citations.isdigit() else 0,
                        "size": file_size_display,
                        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    }

                    all_extracted.append(dataset_obj)
                    existing_titles.add(title.lower().strip())

                    print(f"{get_log_prefix()} - INFO - Dataset adicionado. Fonte: UCIrvine | Area: {subject} | URL: {link}")

                except Exception as e:
                    pass

    finally:
        driver.quit()
        return all_extracted