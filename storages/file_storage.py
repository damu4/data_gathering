import os
import json

from storages.storage import Storage


class FileStorage(Storage):

    def __init__(self, file_name):
        self.file_name = file_name

    def read_data(self):
        if not os.path.exists(self.file_name):
            raise StopIteration
        with open(self.file_name) as f:
            for line in f:
                yield json.loads(line.strip())

    def write_data(self, data_array):
        """
        :param data_array: collection of strings that
        should be written as lines
        """
        with open(self.file_name, encoding='utf-8', mode='w') as f:
            for line in data_array:
                f.write(json.dumps(line) + '\n')

    def append_data(self, data):
        """
        :param data: string
        """
        with open(self.file_name, encoding='utf-8', mode='a') as f:
            for line in data:
                f.write(json.dumps(line) + '\n')
