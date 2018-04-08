"""
Зачем нужны __init__.py файлы
https://stackoverflow.com/questions/448271/what-is-init-py-for

Про документирование в Python проекте
https://www.python.org/dev/peps/pep-0257/

Про оформление Python кода
https://www.python.org/dev/peps/pep-0008/


Примеры сбора данных:
https://habrahabr.ru/post/280238/

Для запуска тестов в корне проекта:
python3 -m unittest discover

Для запуска проекта из корня проекта:
python3 -m gathering gather
или
python3 -m gathering transform
или
python3 -m gathering stats


Для проверки стиля кода всех файлов проекта из корня проекта
pep8 .


ЗАДАНИЕ

Выбрать источник данных и собрать данные по некоторой предметной области.

Цель задания - отработать навык написания программ на Python.
В процессе выполнения задания затронем области:
- организация кода в виде проекта, импортирование модулей внутри проекта
- unit тестирование
- работа с файлами
- работа с протоколом http
- работа с pandas
- логирование

Требования к выполнению задания:

- собрать не менее 1000 объектов

- в каждом объекте должно быть не менее 5 атрибутов
(иначе просто будет не с чем работать.
исключение - вы абсолютно уверены что 4 атрибута в ваших данных
невероятно интересны)

- сохранить объекты в виде csv файла

- считать статистику по собранным объектам


Этапы:

1. Выбрать источник данных.

Это может быть любой сайт или любое API

Примеры:
- Пользователи vk.com (API)
- Посты любой популярной группы vk.com (API)
- Фильмы с Кинопоиска
(см. ссылку на статью выше)
- Отзывы с Кинопоиска
- Статьи Википедии
(довольно сложная задача,
можно скачать дамп википедии и распарсить его,
можно найти упрощенные дампы)
- Статьи на habrahabr.ru
- Объекты на внутриигровом рынке на каком-нибудь сервере WOW (API)
(желательно англоязычном, иначе будет сложно разобраться)
- Матчи в DOTA (API)
- Сайт с кулинарными рецептами
- Ebay (API)
- Amazon (API)
...

Не ограничивайте свою фантазию. Это могут быть любые данные,
связанные с вашим хобби, работой, данные любой тематики.
Задание специально ставится в открытой форме.
У такого подхода две цели -
развить способность смотреть на задачу широко,
пополнить ваше портфолио (вы вполне можете в какой-то момент
развить этот проект в стартап, почему бы и нет,
а так же написать статью на хабр(!) или в личный блог.
Чем больше у вас таких активностей, тем ценнее ваша кандидатура на рынке)

2. Собрать данные из источника и сохранить себе в любом виде,
который потом сможете преобразовать

Можно сохранять страницы сайта в виде отдельных файлов.
Можно сразу доставать нужную информацию.
Главное - постараться не обращаться по http за одними и теми же данными много раз.
Суть в том, чтобы скачать данные себе, чтобы потом их можно было как угодно обработать.
В случае, если обработать захочется иначе - данные не надо собирать заново.
Нужно соблюдать "этикет", не пытаться заддосить сайт собирая данные в несколько потоков,
иногда может понадобиться дополнительная авторизация.

В случае с ограничениями api можно использовать time.sleep(seconds),
чтобы сделать задержку между запросами

3. Преобразовать данные из собранного вида в табличный вид.

Нужно достать из сырых данных ту самую информацию, которую считаете ценной
и сохранить в табличном формате - csv отлично для этого подходит

4. Посчитать статистики в данных
Требование - использовать pandas (мы ведь еще отрабатываем навык использования инструментария)
То, что считаете важным и хотели бы о данных узнать.

Критерий сдачи задания - собраны данные по не менее чем 1000 объектам (больше - лучше),
при запуске кода командой "python3 -m gathering stats" из собранных данных
считается и печатается в консоль некоторая статистика

Код можно менять любым удобным образом
Можно использовать и Python 2.7, и 3

"""

import logging
import sys

import pandas as pd

from parsers.parser import VacancyParser
from scrappers.scrapper import Scrapper
from storages.file_storage import FileStorage

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

SCRAPPED_FILE = 'scrapped_data.txt'
TABLE_FORMAT_FILE = 'data.csv'
# Number of vacancies we would like to get, int 0 - 2000
VACANCIES_LIMIT = 2000
# Number of vacancies per request, int 0 - 100
VACANCIES_PER_PAGE = 100
# hh.ru area code from https://api.hh.ru/areas
VACANCIES_SEARCH_AREA = [1438, 2114]
# hh.ru specializations from https://api.hh.ru/specializations
SPECIALIZATION = 22
COLUMNS = ['name', 'salary_from', 'salary_to', 'days', 'company', 'city']


def gather_process():
    logger.info("gather")
    storage = FileStorage(SCRAPPED_FILE)

    # You can also pass a storage
    scrapper = Scrapper(limit=VACANCIES_LIMIT, per_page=VACANCIES_PER_PAGE, area=VACANCIES_SEARCH_AREA,
                        specialization=SPECIALIZATION)
    scrapper.scrap_process(storage)


def convert_data_to_table_format():
    logger.info("transform")
    storage = FileStorage(SCRAPPED_FILE)
    data = storage.read_data()
    parser = VacancyParser(COLUMNS)
    # Your code here
    # transform gathered data from txt file to pandas DataFrame and save as csv
    with open(TABLE_FORMAT_FILE, encoding='utf-8', mode='w') as f:
        df = pd.DataFrame(columns=COLUMNS)
        for vacancy in data:
            df = df.append(pd.DataFrame(parser.parse(vacancy), columns=COLUMNS), ignore_index=True)
        df.to_csv(f, encoding='utf-8')
    pass


def stats_of_data():
    logger.info("stats")

    # Your code here
    # Load pandas DataFrame and print to stdout different statistics about the data.
    # Try to think about the data and use not only describe and info.
    # Ask yourself what would you like to know about this data (most frequent word, or something else)
    df = pd.read_csv(TABLE_FORMAT_FILE)
    # clear vacancies which appear less than
    counts = df['name'].value_counts()
    df = df[df['name'].isin(counts[counts > 5].index)]
    # concatenate min and max offers of salaries
    df1 = df.rename(columns={'salary_to': 'salary'})
    df2 = df.rename(columns={'salary_from': 'salary'})
    df3 = pd.concat([df1[['name', 'salary']].dropna(), df2[['name', 'salary']].dropna()])
    mean_salary = df3['salary'].mean()
    top_vacancies = df['name'].value_counts()[:5]
    top_cities = df['city'].value_counts()[:5]
    top_salary_vacancies = df3.groupby('name')['salary'].agg(['mean']).sort_values('mean', ascending=False)[:10]
    top_salary_vacancies = top_salary_vacancies['mean'].map("{:.0f}".format)
    top_days_vacancies = df.groupby('name')['days'].agg(['max']).sort_values('max', ascending=False)[:5]
    top_company_vacancies = df['company'].value_counts()[:5]

    print('-- Домашнаяя работа №1. Курс "BigData" от 27.03.2018 --')
    print('  Исследование вакансий в сфере туризма и сервиса ')
    print('  в Краснодарском крае и Республике Крым на основе данных hh.ru')
    print()
    print('1) Средняя предлагаемая зарплата:')
    print('%d' % mean_salary)
    print()
    print('2) Самые востребованные профессии:')
    print(top_vacancies)
    print()
    print('3) Города нуждающиеся в кадрах:')
    print(top_cities)
    print()
    print('4) Наиболее оплачиваемые профессии:')
    print(top_salary_vacancies)
    print()
    print('5) Вакансии, которые дольше закрываются (в днях):')
    print(top_days_vacancies)
    print()
    print('6) Компании, наиболее активно ищущие кадры:')
    print(top_company_vacancies)


if __name__ == '__main__':
    """
    why main is so...?
    https://stackoverflow.com/questions/419163/what-does-if-name-main-do
    """
    logger.info("Work started")

    if sys.argv[1] == 'gather':
        gather_process()

    elif sys.argv[1] == 'transform':
        convert_data_to_table_format()

    elif sys.argv[1] == 'stats':
        stats_of_data()

    logger.info("work ended")
