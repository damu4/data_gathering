import abc
from datetime import datetime
import pytz


def parse_name(data):
    """ hh.ru vacancy's names are long and have many unnecessary words """
    s = data.partition('(')[0].partition('г.')[0]
    if len(s.split(' ')) > 1:
        first_word, second_word = s.split(' ')[:2]
        if first_word.lower() in ['управляющий', 'администратор', 'менеджер']:
            vacancy_name = first_word
        elif first_word.lower() in ['старший']:
            vacancy_name = second_word
        elif len(second_word) > 2:
            vacancy_name = first_word + ' ' + second_word
        else:
            vacancy_name = first_word
    else:
        vacancy_name = s
    return vacancy_name.replace('"', '').rstrip().lower()


class Parser(object):
    """Abstract class for data parsing to exact attributes (fields)"""
    __metaclass__ = abc.ABCMeta

    def __init__(self, fields):
        self.fields = fields
        self.fields_set = set(fields)

    @abc.abstractmethod
    def parse(self, data):
        """
        Override this method for fields extraction from data
        :param data: data can be in any appropriate format
        (text, json or other)
        :return: list of dictionaries where key is
        one of defined fields and value is this field's value
        """
        return [{f: None for f in self.fields}]


class VacancyParser(Parser):

    def parse(self, data):
        tz = pytz.timezone('Europe/Moscow')
        now = datetime.now(tz)
        created_datetime = datetime.strptime(data['created_at'], "%Y-%m-%dT%H:%M:%S%z")
        values = {'name': parse_name(data['name'])}
        if data['salary']:
            values['salary_from'] = data['salary']['from']
            values['salary_to'] = data['salary']['to']
        else:
            values['salary_from'] = 0
            values['salary_to'] = 0
        values['days'] = (now - created_datetime).days
        values['company'] = data['employer']['name']
        if data['address']:
            values['city'] = data['area']['name']
        else:
            values['city'] = ''
        return [{f: values[f] for f in self.fields}]
