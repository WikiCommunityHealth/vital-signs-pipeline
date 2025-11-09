import time
import os
import shutil
import requests
from datetime import datetime
from dateutil.relativedelta import relativedelta
from bs4 import BeautifulSoup
from urllib.parse import urljoin

import logging

logger = logging.getLogger(__name__)


def download_file(url, save_path):
    local_filename = os.path.join(save_path, url.split('/')[-1])

    with requests.get(url, stream=True) as response:
        response.raise_for_status()
        total_size = int(response.headers.get('content-length', 0))
        downloaded_size = 0

        with open(local_filename, 'wb') as f:
            for chunk in response.iter_content(chunk_size=1024*1024):
                f.write(chunk)
                downloaded_size += len(chunk)

                if total_size > 0:
                    print(
                        f"Downloaded {downloaded_size}/{total_size} bytes", end="\r")
    print("\nDownload complete.")
    return local_filename


def get_dump_links(url):
    response = requests.get(url)
    if response.status_code != 200:
        return []

    soup = BeautifulSoup(response.text, 'html.parser')
    links = [urljoin(url, a['href']) for a in soup.find_all(
        'a', href=True) if a['href'].endswith('.bz2')]
    return links


def main():
    wiki_names = ['pmswiki/', 'lmowiki/', 'lijwiki/',
                  'vecwiki/', 'scwiki/', 'scnwiki/', 'napwiki/']

    dumps_path = "./mediawiki_history_dumps/"

    if os.path.exists(dumps_path):
        shutil.rmtree(dumps_path)

    os.makedirs(dumps_path, exist_ok=True)

    YYYY_MM = (datetime.now() - relativedelta(months=1)).strftime("%Y-%m")
    base_url = "https://dumps.wikimedia.org/other/mediawiki_history/"

    # Controllo se la directory del mese corrente esiste, altrimenti prendo quella del mese precedente
    if requests.head(f"{base_url}{YYYY_MM}/").status_code == 200:
        correct_url = f"{base_url}{YYYY_MM}/"
    else:
        YYYY_MM = (datetime.now() - relativedelta(months=2)).strftime("%Y-%m")
        correct_url = f"{base_url}{YYYY_MM}/"

    for wiki_name in wiki_names:
        start = time.time()

        url = correct_url + wiki_name + "/"

        print(f"Downloading wiki dumps for: {wiki_name}")

        wiki_save_path = os.path.join(dumps_path, wiki_name)

        os.makedirs(wiki_save_path, exist_ok=True)

        links = get_dump_links(url)
        if not links:
            print("No .bz2 files found at the specified URL.")

        print(f"Found {len(links)} files to download")

        for link in links:
            print(f"Downloading {link}")
            downloaded_file = download_file(link, wiki_save_path)
            print(f"Saved as: {downloaded_file}")

        end = time.time() - start

        print(f"Total execution time: {end:.2f} seconds")


if __name__ == "__main__":
    main()
