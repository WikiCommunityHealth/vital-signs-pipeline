from scripts import config

import os
import shutil
import requests
from datetime import datetime
from dateutil.relativedelta import relativedelta
from bs4 import BeautifulSoup
from urllib.parse import urljoin

import logging

logger = logging.getLogger(__name__)


def download_dumps():
    def get_wiki_directories(base_url):
        response = requests.get(base_url)
        if response.status_code != 200:
            return []

        soup = BeautifulSoup(response.text, 'html.parser')
        directories = [urljoin(base_url, a['href']) for a in soup.find_all('a', href=True) if a['href'].endswith('wiki/')]
        return directories

    def get_dump_links(wiki_url):
        response = requests.get(wiki_url)
        if response.status_code != 200:
            return []

        soup = BeautifulSoup(response.text, 'html.parser')
        links = [urljoin(wiki_url, a['href']) for a in soup.find_all('a', href=True) if a['href'].endswith('.bz2')]
        return links

    def download_file(url, save_path):
        local_filename = os.path.join(save_path, url.split('/')[-1])

        with requests.get(url, stream=True) as response:
            response.raise_for_status()
            with open(local_filename, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)

        logger.info(f"Downloaded {local_filename}")
        return local_filename
    
    
    if os.path.exists(config.dumps_path):
        shutil.rmtree(config.dumps_path)

    
    YYYY_MM = (datetime.now() - relativedelta(months=1)).strftime("%Y-%m")
    base_url = "https://dumps.wikimedia.org/other/mediawiki_history/"
    
    # Controllo se la directory del mese corrente esiste, altrimenti prendo quella del mese precedente
    if requests.head(f"{base_url}{YYYY_MM}/").status_code == 200:
        correct_url = f"{base_url}{YYYY_MM}/"
    else:
        YYYY_MM = (datetime.now() - relativedelta(months=2)).strftime("%Y-%m")
        correct_url = f"{base_url}{YYYY_MM}/"

    logger.info(f"Found dumps: {correct_url}")    

    os.makedirs(config.dumps_path, exist_ok=True)

    # Ottieni tutte le sottodirectory delle wiki
    wiki_dirs = get_wiki_directories(correct_url)
    for wiki_dir in wiki_dirs:
        wiki_name = wiki_dir.rstrip('/').split('/')[-1]  # Estrai il nome della wiki
        wiki_save_path = os.path.join(config.dumps_path, wiki_name)
        os.makedirs(wiki_save_path, exist_ok=True) 
        links = get_dump_links(wiki_dir)
        if not links:
            logger.warning(f"No file found in {wiki_dir}")
            continue
        
        for link in links:
            download_file(link, wiki_save_path)
        

   