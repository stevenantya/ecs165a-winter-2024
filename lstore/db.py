from .table import Table
from .page import Page
from . import config
import os
import struct
import json

class Database():

    def __init__(self):
        self.file_directory = ""

        self.tables = []
        self.bufferpool = []
        self.page_stack = []
        self.page_table = {}

    # Not required for milestone1
    def open(self, path):
        self.file_directory = path

        if not os.path.exists(path):
            os.makedirs(path)

        self.load_page_table()

    def close(self):
        for i, page in enumerate(self.bufferpool):
            if page.dirty:
                self.evict_page(i)

        self.save_page_table()

    """
    # Creates a new table
    :param name: string         #Table name
    :param num_columns: int     #Number of Columns: all columns are integer
    :param key: int             #Index of table key in columns
    """
    def create_table(self, name, num_columns, key_index):
        table = Table(self, name, num_columns, key_index)
        self.tables.append(table)
        return table
 
    """
    # Deletes the specified table
    """
    def drop_table(self, name):
        for i in range(len(self.tables)):
            if self.tables[i].name == name: 
                self.tables.remove(self.tables[i])
    
    """
    # Returns table with the passed name
    """
    def get_table(self, name):
        for i in range(len(self.tables)):
            if self.tables[i].name == name: 
                return self.tables[i]
            
    def get_page(self, page_range, page_index, column_index):
        # Check if requested page is in bufferpool
        if self.page_table[str(page_range)][str(page_index)][str(column_index)] != -1:
            bufferpool_index = self.page_table[str(page_range)][str(page_index)][str(column_index)]
            page = self.bufferpool[bufferpool_index]
            page.pin += 1
            self.page_stack.remove(bufferpool_index)
            self.page_stack.append(bufferpool_index)
            return page
        else:
            new_page = Page()
            new_page.page_name = f"r{page_range}p{page_index}c{column_index}"
            new_page.pin += 1

            file_path = self.file_directory + "/" + new_page.page_name + ".bin"
            if os.path.exists(file_path):
                # There is existing file of the page
                with open(file_path, 'rb') as file:
                    binary_data = file.read()
                num_records = len(binary_data) // struct.calcsize('q')
                for i in range(num_records):
                    record = struct.unpack('q', binary_data[i * struct.calcsize('q'):(i + 1) * struct.calcsize('q')])[0]
                    new_page.add_record(record)
                new_page.dirty = 0

        # Insert new page into bufferpool
        if len(self.bufferpool) < config.BUFFERPOOL_SIZE:
            # Bufferpool has empty frame
            self.bufferpool.append(new_page)
            bufferpool_index = len(self.bufferpool) - 1
        else:
            # Eviction
            # Find LRU unpinned page
            for bufferpool_index in self.page_stack:
                if self.bufferpool[bufferpool_index].pin == 0:
                    break
            if self.bufferpool[bufferpool_index].dirty:
                self.evict_page(bufferpool_index)
            self.bufferpool[bufferpool_index] = new_page
            
        self.page_table[str(page_range)][str(page_index)][str(column_index)] = bufferpool_index
        return new_page

    def evict_page(self, buffer_index):
        # Store the page in bufferpool into a binary file
        target_page = self.bufferpool[buffer_index]

        file_path = self.file_directory + "/" + target_page.page_name + ".bin"

        with open(file_path, 'wb') as file:
            binary_data = struct.pack('q' * target_page.num_records, *target_page.rows[:target_page.num_records])
            file.write(binary_data)

        self.page_table[target_page.page_name[1:target_page.page_name.index('p')]][target_page.page_name[target_page.page_name.index('p')+1:target_page.page_name.index('c')]][target_page.page_name[target_page.page_name.index('c')+1:]] = -1

    def save_page_table(self):
        file_path = self.file_directory + "/" + "page_table.json"

        with open(file_path, "w") as file:
            data = json.dumps(self.page_table, indent=4)
            file.write(data)

    def load_page_table(self):
        file_path = self.file_directory + "/" + "page_table.json"

        if os.path.exists(file_path):
            with open(file_path, "r") as file:
                self.page_table = json.loads(file.read())