import logging
import time

import requests

logger = logging.getLogger(__name__)


class Scrapper(object):
    headers = {'User-Agent': 'BigDataTest/0.1 (damu4@yandex.ru)'}

    def __init__(self, limit, per_page, area, specialization, skip_objects=None):
        self.pages = limit // per_page
        self.per_page = per_page
        self.area = area
        self.skip_objects = skip_objects
        self.specialization = specialization

    def scrap_process(self, storage):

        # You can iterate over ids, or get list of objects
        # from any API, or iterate throught pages of any site
        # Do not forget to skip already gathered data
        # Here is an example for you
        base_url = 'https://api.hh.ru/vacancies/?period=30&currency=RUB&only_with_salary=true' \
                   '&page={0}&per_page={1}&specialization={2}&area={3}&area={4}'
        pages = range(self.pages)
        for page in pages:
            url = base_url.format(page, self.per_page, self.specialization, self.area[0], self.area[1])
            response = requests.get(url, self.headers)

            if not response.ok:
                logger.error(response.text)
                # then continue process, or retry, or fix your code

            else:
                # Note: here json can be used as response.json
                items = response.json()['items']
                # save scrapped objects here
                # you can save url to identify already scrapped objects
                storage.append_data(items)
            time.sleep(0.2)
