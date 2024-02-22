import sys
from .index import Index
from time import time
from .page import Page
from . import config
import math

class Record:

    def __init__(self, rid, key, columns):
        self.rid = rid
        self.key = key
        self.columns = columns

class Table:
    def __init__(self, parent, name, num_columns, key):
        self.db = parent
        self.name = name
        self.key = key
        self.num_columns = num_columns

        self.index = Index(self)

    def __del__(self):
        pass  # Destructor logic here if needed

    def get_time(self):
        return int(time())

    def add_record(self, input_data):
        # Existing record with the same key, so forbid adding a duplicate record
        if input_data[self.key] in self.index.indices[self.key]:
            return False

        # Get to the first empty record inside a bottomost page range's bottomost base page
        final_page_range = len(self.db.page_table) - 1

        if final_page_range >= 0:
            final_base_page_index = len(self.db.page_table[str(final_page_range)]) - 1
            final_row_page = self.db.get_page(final_page_range, final_base_page_index, config.INDIRECTION_COLUMN)
            final_row_num = final_row_page.get_num_record() 

        # Add a new base page if there is no page range or current bottomost base page is full
        if final_page_range < 0 or final_row_num == config.PAGE_MAX_ROWS:
            self.add_base_page()
            final_page_range = len(self.db.page_table) - 1
            final_base_page_index = len(self.db.page_table[str(final_page_range)]) - 1
            final_row_num = 0

        self.db.get_page(final_page_range, final_base_page_index, config.INDIRECTION_COLUMN).add_record(self.encode_indirection(final_base_page_index, final_row_num))  # Indirection
        self.db.get_page(final_page_range, final_base_page_index, config.TIMESTAMP_COLUMN).add_record(self.get_time())
        self.db.get_page(final_page_range, final_base_page_index, config.SCHEMA_ENCODING_COLUMN).add_record(0)

        # Add the data value to respective columns
        for i in range(config.METACOLUMN_NUM, self.num_columns + config.METACOLUMN_NUM):
            self.db.get_page(final_page_range, final_base_page_index, i).add_record(input_data[i - config.METACOLUMN_NUM])

        # Add the new record's rid to index
        self.index.indices[self.key][input_data[self.key]] = self.encode_RID(final_page_range, final_base_page_index, final_row_num)

        return True

    def update_record(self, rid, input_data):
        # Existing record with the same key, so forbid adding a duplicate record
        if input_data[self.key] in self.index.indices[self.key]:
            return False

        page_range_index = self.parsePageRangeRID(rid)
        base_page_index = self.parseBasePageRID(rid)
        page_offset = self.parseRecord(rid)

        target_page_range = self.page_ranges[page_range_index]

        # Adds a new tail page if there isn't an existing one or the existing one is full
        if not target_page_range['tail_pages'] or target_page_range['tail_pages'][-1][config.INDIRECTION_COLUMN].get_num_record() == config.PAGE_MAX_ROWS:
            self.add_tail_page(page_range_index)

        target_base_page = target_page_range['base_pages'][base_page_index]
        target_tail_page = target_page_range['tail_pages'][-1]

        # The original indirection stored in the base record
        previous_indirection = target_base_page[config.INDIRECTION_COLUMN][page_offset]
        indirection_index = self.parseIndirection(previous_indirection)

        if (indirection_index < config.PAGE_RANGE):
            # First update
            previous_tail_page = target_page_range['base_pages'][indirection_index]
        else:
            # Subsequent updates
            previous_tail_page = target_page_range['tail_pages'][indirection_index - config.PAGE_RANGE]

        # Update base record's indirection to points to the new tail record to be added
        target_base_page[config.INDIRECTION_COLUMN][page_offset] = self.encode_indirection(len(target_page_range['tail_pages']) - 1 + config.PAGE_RANGE, target_tail_page[config.INDIRECTION_COLUMN].get_num_record())

        # Makes the latest tail record indirection point to the previus version
        target_tail_page[config.INDIRECTION_COLUMN].add_record(previous_indirection)

        target_tail_page[config.TIMESTAMP_COLUMN].add_record(self.get_time())  # Timestamp

        schema_encoding = 0
        for i in range(config.METACOLUMN_NUM, self.num_columns + config.METACOLUMN_NUM):
            field = input_data[i - config.METACOLUMN_NUM]

            # If no need to update field
            if field == None:
                # If there is previous update to this column
                if self.extract_bit(target_base_page[config.SCHEMA_ENCODING_COLUMN][page_offset], self.num_columns - (i - config.METACOLUMN_NUM) - 1):
                    # Copy value from previous update
                    target_tail_page[i].add_record(previous_tail_page[i][self.parseRecord(previous_indirection)])
                    schema_encoding += 1 << (self.num_columns - (i - config.METACOLUMN_NUM) - 1)
                else:
                    # Add null value
                    target_tail_page[i].add_record(field)
            else:
                schema_encoding += 1 << (self.num_columns - (i - config.METACOLUMN_NUM) - 1)
                target_tail_page[i].add_record(field)

        # Update scheme encoding in base record
        target_base_page[config.SCHEMA_ENCODING_COLUMN][page_offset] = schema_encoding
        # Add the schema encoding for the latest tail record
        target_tail_page[config.SCHEMA_ENCODING_COLUMN].add_record(schema_encoding)

        return True

    def get_record(self, rid, projected_columns_index, version):
        page_range_index = self.parsePageRangeRID(rid)
        base_page_index = self.parseBasePageRID(rid)
        page_offset = self.parseRecord(rid)

        target_page_range = self.page_ranges[page_range_index]

        target_base_page = target_page_range['base_pages'][base_page_index]
        # Points to the latest tail record
        curr_indirection = target_base_page[config.INDIRECTION_COLUMN][page_offset]

        for i in range(0 , version, -1):
            if self.parseIndirection(curr_indirection) < config.PAGE_RANGE:
                break
            curr_indirection = target_page_range['tail_pages'][self.parseIndirection(curr_indirection) - config.PAGE_RANGE][config.INDIRECTION_COLUMN][self.parseRecord(curr_indirection)]

        # Find the target record's page
        if self.parseIndirection(curr_indirection) < config.PAGE_RANGE:
            target_page = target_base_page
        else:
            target_page = target_page_range['tail_pages'][self.parseIndirection(curr_indirection) - config.PAGE_RANGE]

        rtn_record = []
        # Forms the return based on the projected_columns_index, only if it is 1 will it be appended
        for i in range(self.num_columns):
            if projected_columns_index[i]:
                val = target_page[i + config.METACOLUMN_NUM][self.parseRecord(curr_indirection)]
                                                           
                if val != None:
                    rtn_record.append(target_page[i + config.METACOLUMN_NUM][self.parseRecord(curr_indirection)])
                else:
                    rtn_record.append(target_base_page[i + config.METACOLUMN_NUM][page_offset])

        return rtn_record

    def delete_record(self, rid):
        page_range_index = self.parsePageRangeRID(rid)
        base_page_index = self.parseBasePageRID(rid)
        page_offset = self.parseRecord(rid)

        target_page_range = self.page_ranges[page_range_index]
        target_base_page = target_page_range['base_pages'][base_page_index]

        # Sets indirection of base record to NULL_VAL to imply deletion
        target_base_page[config.INDIRECTION_COLUMN][page_offset] = config.NULL_VAL

        # Remove the rid of this record from index
        del self.index.indices[self.key][target_base_page[self.key + config.METACOLUMN_NUM][page_offset]]

    def add_base_page(self):
        # If no existing page range or current page range is full of base page, create new page range
        if not self.db.page_table or len(self.db.page_table[str(len(self.db.page_table)-1)]) == config.PAGE_RANGE:
            self.db.page_table[str(len(self.db.page_table))] = {}

        self.db.page_table[str(len(self.db.page_table)-1)]["0"] = {str(i) : -1 for i in range(self.num_columns + config.METACOLUMN_NUM)}

    def add_tail_page(self, page_range_index):
        target_page_range = self.page_ranges[page_range_index]
        new_tail_page = [Page() for _ in range(self.num_columns + config.METACOLUMN_NUM)]
        target_page_range['tail_pages'].append(new_tail_page)

    def display(self):
        for pr in self.page_ranges:
            for base_page in pr['base_pages']:
                for i in range(base_page[config.INDIRECTION_COLUMN].get_num_record()):  # Assuming get_num_record() method exists
                    print('    '.join(f'{base_page[j][i]:016x}' for j in range(self.num_columns + config.METACOLUMN_NUM)))
                print('-' * 150)
            for tail_page in pr['tail_pages']:
                for i in range(tail_page[config.INDIRECTION_COLUMN].get_num_record()):  # Assuming get_num_record() method exists
                    print('    '.join(f'{tail_page[j][i]:016x}' for j in range(self.num_columns + config.METACOLUMN_NUM)))
                print('-' * 150)

    # Extract the leftmost 48 bits of rid to get page range
    def parsePageRangeRID(self, rid):
        bitmask = (2 ** (64 - int(math.log(config.PAGE_RANGE, 2)) - int(math.log(config.PAGE_MAX_ROWS, 2))) - 1) << (int(math.log(config.PAGE_RANGE, 2)) + int(math.log(config.PAGE_MAX_ROWS, 2)))
        page_range_idx = (rid & bitmask) >> (int(math.log(config.PAGE_RANGE, 2)) + int(math.log(config.PAGE_MAX_ROWS, 2)))
        return page_range_idx

    # Extract the in between 7 bits of rid to get base page index
    def parseBasePageRID(self, rid):
        bitmask = (2 ** int(math.log(config.PAGE_RANGE, 2)) - 1) << int(math.log(config.PAGE_MAX_ROWS, 2))
        base_page_idx = (rid & bitmask) >> int(math.log(config.PAGE_MAX_ROWS, 2))
        return base_page_idx

    # Extract the rightmost 9 bits to get page offset
    def parseRecord(self, rid):
        bitmask = (1 << int(math.log(config.PAGE_MAX_ROWS, 2))) - 1
        record_idx = rid & bitmask
        return record_idx

    #Extract the leftmost 55 bits from indirection to get page number inside page range
    def parseIndirection(self, rid):
        bitmask = (2 ** (64 - int(math.log(config.PAGE_MAX_ROWS, 2))) - 1) << int(math.log(config.PAGE_MAX_ROWS, 2))
        tail_page_idx = (rid & bitmask) >> int(math.log(config.PAGE_MAX_ROWS, 2))
        return tail_page_idx

    # Encodes indirection in the form of 55 + 9 bits, page index and page offset respectively
    def encode_indirection(self, page_index, page_offset):
        indirection = page_offset
        indirection += (page_index << int(math.log(config.PAGE_MAX_ROWS, 2)))
        return indirection
    
    def encode_RID(self, page_range, page_index, page_offset):
        rid = page_offset
        rid += (page_index << int(math.log(config.PAGE_MAX_ROWS, 2)))
        rid += (page_range << (int(math.log(config.PAGE_RANGE, 2)) + int(math.log(config.PAGE_MAX_ROWS, 2))))

        return rid

    # Checks if the i-th position bit is 0 or 1 
    def extract_bit(self, schema_encoding, position):
        bitmask = 1 << position
        return (schema_encoding & bitmask) != 0