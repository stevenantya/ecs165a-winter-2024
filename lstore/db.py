from .table import Table
from .page import Page
from . import config
import os
import struct
import json
import pickle

class Database():

    def __init__(self):
        self.file_directory = ""

        self.tables = []
        self.bufferpool = []
        self.page_stack = []
        self.page_table = {}
        self.system_catalog = {}

    # Not required for milestone1
    def open(self, path):
        self.file_directory = path

        if not os.path.exists(path):
            os.makedirs(path)

        if not os.path.exists(path + "/Pages"):
            os.makedirs(path + "/Pages")

        if not os.path.exists(path + "/Indexes"):
            os.makedirs(path + "/Indexes")

        self.load_page_table()
        self.load_system_catalog()
        for name, info in self.system_catalog.items():
            self.create_table(name, info[0], info[1])

    def close(self):
        for i, page in enumerate(self.bufferpool):
            if page.dirty:
                self.evict_page(i)
            self.reset_page_table_entry(i)

        self.save_page_table()
        self.save_system_catalog()
        self.save_indexes()

    """
    # Creates a new table
    :param name: string         #Table name
    :param num_columns: int     #Number of Columns: all columns are integer
    :param key: int             #Index of table key in columns
    """
    def create_table(self, name, num_columns, key_index):
        table = Table(self, name, num_columns, key_index)

        # Load the indexes for this table if they exists
        has_indexes = self.load_indexes(table)

        # Initialize the indexes for a brand new table
        if not has_indexes:
            table.index.create_index(key_index)

        self.tables.append(table)

        self.system_catalog = {name: [num_columns, key_index]}
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
        in_bufferpool = False

        if page_index < config.PAGE_RANGE:
            if self.page_table[str(page_range)]["base_pages"][str(page_index)][str(column_index)] != -1:
                in_bufferpool = True
        else:
            if self.page_table[str(page_range)]["tail_pages"][str(page_index - config.PAGE_RANGE)][str(column_index)] != -1:
                in_bufferpool = True

        if in_bufferpool:
            if page_index < config.PAGE_RANGE:
                bufferpool_index = self.page_table[str(page_range)]["base_pages"][str(page_index)][str(column_index)]
            else:
                bufferpool_index = self.page_table[str(page_range)]["tail_pages"][str(page_index - config.PAGE_RANGE)][str(column_index)]

            page = self.bufferpool[bufferpool_index]
            page.pin += 1
            self.page_stack.remove(bufferpool_index)
            self.page_stack.append(bufferpool_index)
            return page
        else:
            new_page = Page()
            new_page.page_name = f"r{page_range}p{page_index}c{column_index}"
            new_page.pin += 1

            file_path = self.file_directory + "/Pages/" + new_page.page_name + ".bin"
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

            # Reset the existing page's bufferpool index in page table to -1
            self.reset_page_table_entry(bufferpool_index)
        
            # Overwrite existing file if evicting dirty page
            if self.bufferpool[bufferpool_index].dirty:
                self.evict_page(bufferpool_index)

            self.bufferpool[bufferpool_index] = new_page
            self.page_stack.remove(bufferpool_index)
        self.page_stack.append(bufferpool_index)

        if page_index < config.PAGE_RANGE:
            self.page_table[str(page_range)]["base_pages"][str(page_index)][str(column_index)] = bufferpool_index
        else:
            self.page_table[str(page_range)]["tail_pages"][str(page_index - config.PAGE_RANGE)][str(column_index)] = bufferpool_index

        return new_page

    def evict_page(self, buffer_index):
        # Store the page in bufferpool into a binary file
        target_page = self.bufferpool[buffer_index]

        file_path = self.file_directory + "/Pages/" + target_page.page_name + ".bin"

        with open(file_path, 'wb') as file:
            binary_data = struct.pack('q' * target_page.num_records, *target_page.rows[:target_page.num_records])
            file.write(binary_data)

    def reset_page_table_entry(self, bufferpool_index):
        existing_page_name = self.bufferpool[bufferpool_index].page_name
        if int(existing_page_name[existing_page_name.index('p')+1:existing_page_name.index('c')]) < config.PAGE_RANGE:
            self.page_table[existing_page_name[1:existing_page_name.index('p')]]["base_pages"][existing_page_name[existing_page_name.index('p')+1:existing_page_name.index('c')]][existing_page_name[existing_page_name.index('c')+1:]] = -1
        else:
            self.page_table[existing_page_name[1:existing_page_name.index('p')]]["tail_pages"][str(int(existing_page_name[existing_page_name.index('p')+1:existing_page_name.index('c')]) - config.PAGE_RANGE)][existing_page_name[existing_page_name.index('c')+1:]] = -1

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

    def save_system_catalog(self):
        file_path = self.file_directory + "/" + "system_catalog.json"

        with open(file_path, "w") as file:
            data = json.dumps(self.system_catalog, indent=4)
            file.write(data)

    def load_system_catalog(self):
        file_path = self.file_directory + "/" + "system_catalog.json"

        if os.path.exists(file_path):
            with open(file_path, "r") as file:
                self.system_catalog = json.loads(file.read())

    def save_indexes(self):
        index_directory = self.file_directory + "/Indexes/" 

        # Assume one table only
        for table in self.tables:
            for i, index in enumerate(table.index.indices):
                if index:
                    with open(index_directory + "/index_" + str(i) + ".pickle", "wb") as file:
                        data = pickle.dumps(index)
                        file.write(data)

    def load_indexes(self, table):
        index_directory = self.file_directory + "/Indexes/" 
        index_loaded = False

        # Assume one table only
        if os.path.exists(index_directory):
            file_list = os.listdir(index_directory)
            for file_name in file_list:
                with open(index_directory + file_name, "rb") as file:
                    index_num = int(file_name[file_name.index('_')+1:file_name.index('_')+2])
                    data = file.read()
                    table.index.indices[index_num] = pickle.loads(data)
                    index_loaded = True

        return index_loaded
            