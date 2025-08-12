import os

import requests
import json
from datetime import datetime

from border_equeue_stats.constants import EQUEUE_JSON_PATH


def beautiful_soup_parser():
    from bs4 import BeautifulSoup

    URL = "https://mon.declarant.by/zone/brest-bts"
    page = requests.get(URL)

    soup = BeautifulSoup(page.content, "html.parser")

    # results = soup.find(id="ResultsContainer")
    # print(results.prettify())
    print()


def direct_parser():
    results = requests.get(EQUEUE_JSON_PATH)
    equeue = json.loads(results.text)
    equeue['datetime'] = str(datetime.now())
    with open('../data/brest_border_equeue.txt', 'a') as f:
        f.write(str(equeue) + '\n')


def parse_equeue(url: str) -> dict:
    results = requests.get(url)
    data = json.loads(results.text)
    data['datetime'] = str(datetime.now())
    return data
