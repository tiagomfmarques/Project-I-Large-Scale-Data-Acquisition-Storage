import json
import uuid
import time
import re
import os
from datetime import datetime
from selenium import webdriver
import hashlib
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

# Função para obter o prefixo de log com timestamp
def get_log_prefix():
    return datetime.now().strftime('%Y-%m-%d %H:%M:%S')

# Função principal para extrair datasets do UCIrvine
def extrair_ucirvine(config, existing_titles):
    chrome_options = Options()
    chrome_options.add_argument("--headless=new")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--window-size=1920,1080")

    try:
        service = Service(ChromeDriverManager().install())
        driver = webdriver.Chrome(service=service, options=chrome_options)
    except Exception as e:
        print(f"DEBUG: Falha na inicialização do WebDriver: {e}")
        driver = webdriver.Chrome(options=chrome_options)

    target_new = 100
    all_extracted = []

    try:
        for subject in config.get('subjects', []):
            print(f"{get_log_prefix()} - INFO - Scraper Iniciado: UCIrvine | Area: {subject}")

            subject_url = f"https://archive.ics.uci.edu/datasets?subjectArea={subject.replace(' ', '+')}"
            driver.get(subject_url)
            time.sleep(7)

            new_in_subject = 0

            while new_in_subject < target_new:

                current_cards = driver.find_elements(By.CSS_SELECTOR, "h2.text-primary a")
                links_na_pagina = [card.get_attribute("href") for card in current_cards if card.get_attribute("href")]

                for link in links_na_pagina:
                    if new_in_subject >= target_new:
                        break

                    driver.get(link)
                    time.sleep(2)

                    try:
                        title_element = driver.find_element(By.TAG_NAME, "h1")
                        title = title_element.text.strip()

                        if title.lower().strip() in existing_titles:
                            driver.back()
                            time.sleep(1)
                            continue

                        info_elements = driver.find_elements(By.CSS_SELECTOR, "p.svelte-1xc1tf7")
                        dataset_info = " ".join([el.text.strip() for el in info_elements if el.text.strip()])

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

                        # Cálculo de tamanho
                        total_size_mb = 0.0
                        try:
                            size_cells = driver.find_elements(By.CSS_SELECTOR, "table tbody tr td:nth-child(2)")
                            for cell in size_cells:
                                total_size_mb += parse_size_to_mb(cell.text)
                            file_size_display = f"{round(total_size_mb, 2)} MB" if total_size_mb > 0 else "N/A"
                        except:
                            file_size_display = "N/A"

                        metadata = {}
                        grid_items = driver.find_elements(By.CSS_SELECTOR,"div.grid-cols-8 > div, div.grid-cols-12 > div")
                        for item in grid_items:
                            try:
                                lbl = item.find_element(By.TAG_NAME, "h1").text.strip()
                                val = item.find_element(By.TAG_NAME, "p").text.strip()
                                metadata[lbl] = val
                            except:
                                continue
                        sha256_id = hashlib.sha256(link.encode('utf-8')).hexdigest()

                        dataset_obj = {
                            "id": sha256_id,
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
                        new_in_subject += 1

                        print(
                            f"{get_log_prefix()} - INFO - Dataset adicionado ({new_in_subject}/{target_new}). Area: {subject} | URL: {link}")

                    except Exception as e:
                        print(f"Erro ao processar dataset {link}: {e}")
                    finally:
                        driver.back()
                        time.sleep(2)

                if new_in_subject < target_new:
                    try:
                        next_button = WebDriverWait(driver, 10).until(
                            EC.element_to_be_clickable((By.CSS_SELECTOR, 'button[aria-label="Next Page"]'))
                        )
                        is_disabled = next_button.get_attribute("disabled") or "disabled" in next_button.get_attribute(
                            "class")

                        if is_disabled:
                            break

                        driver.execute_script("arguments[0].click();", next_button)
                        time.sleep(5)
                    except:
                        break

    finally:
        driver.quit()
        return all_extracted