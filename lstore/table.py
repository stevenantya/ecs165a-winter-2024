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
            final_base_page_index = len(self.db.page_table[str(final_page_range)]["base_pages"]) - 1
            final_row_page = self.db.get_page(final_page_range, final_base_page_index, config.INDIRECTION_COLUMN)
            final_row_num = final_row_page.get_num_record() 
            final_row_page.pin -= 1

        # Add a new base page if there is no page range or current bottomost base page is full
        if final_page_range < 0 or final_row_num == config.PAGE_MAX_ROWS:
            self.add_base_page()
            final_page_range = len(self.db.page_table) - 1
            final_base_page_index = len(self.db.page_table[str(final_page_range)]["base_pages"]) - 1
            final_row_num = 0

        # Indirection
        indirection_page = self.db.get_page(final_page_range, final_base_page_index, config.INDIRECTION_COLUMN)
        indirection_page.add_record(self.encode_indirection(final_base_page_index, final_row_num))  
        indirection_page.pin -= 1

        # Timestamp
        timestamp_page = self.db.get_page(final_page_range, final_base_page_index, config.TIMESTAMP_COLUMN)
        timestamp_page.add_record(self.get_time())
        timestamp_page.pin -= 1
        
        # Schema encoding
        schema_page = self.db.get_page(final_page_range, final_base_page_index, config.SCHEMA_ENCODING_COLUMN)
        schema_page.add_record(0)
        schema_page.pin -= 1

        # Add the data value to respective columns
        for i in range(config.METACOLUMN_NUM, self.num_columns + config.METACOLUMN_NUM):
            data_page = self.db.get_page(final_page_range, final_base_page_index, i)
            data_page.add_record(input_data[i - config.METACOLUMN_NUM])
            data_page.pin -= 1

        # Add the new record's rid to index
        self.index.indices[self.key][input_data[self.key]] = self.encode_RID(final_page_range, final_base_page_index, final_row_num)
        
        # Create inserted record and create index for that record 
        # print(self.index.indices[self.key][input_data[self.key]])
        # newRecord = Record(self.index.indices[self.key][input_data[self.key]], self.key, input_data[1:])
        self.index.insert_record(self.index.indices[self.key][input_data[self.key]], input_data)
        return True

    def update_record(self, rid, input_data, layer = 0):
        # Existing record with the same key, so forbid adding a duplicate record
        if input_data[self.key] in self.index.indices[self.key] and self.index.indices[self.key][input_data[self.key]] != rid:
            return False

        page_range_index = self.parsePageRangeRID(rid)
        base_page_index = self.parseBasePageRID(rid)
        page_offset = self.parseRecord(rid)

        # Backup the original data if updated for the first time
        base_schema_page = self.db.get_page(page_range_index, base_page_index, config.SCHEMA_ENCODING_COLUMN)
        old_values = []
        for i in range(config.METACOLUMN_NUM, self.num_columns + config.METACOLUMN_NUM):
            field = input_data[i - config.METACOLUMN_NUM]
            if field is not None and not self.extract_bit(base_schema_page[page_offset], self.num_columns - (i - config.METACOLUMN_NUM) - 1):
                base_data_page = self.db.get_page(page_range_index, base_page_index, i)
                old_values.append(base_data_page[page_offset])
                base_data_page.pin -= 1
            else:
                old_values.append(None)

        if layer == 0 and old_values != [None] * self.num_columns:
            self.update_record(rid, old_values, 1)

        final_tail_page_index = len(self.db.page_table[str(page_range_index)]["tail_pages"]) - 1

        # Adds a new tail page if there isn't an existing one or the existing one is full
        if final_tail_page_index >= 0:
            final_tail_page = self.db.get_page(page_range_index, final_tail_page_index + config.PAGE_RANGE, config.INDIRECTION_COLUMN)
            if final_tail_page.get_num_record() == config.PAGE_MAX_ROWS:
                self.add_tail_page(page_range_index)
                final_tail_page_index += 1
                final_tail_page.pin -= 1
                final_tail_page = self.db.get_page(page_range_index, final_tail_page_index + config.PAGE_RANGE, config.INDIRECTION_COLUMN)
        else:
            self.add_tail_page(page_range_index)
            final_tail_page_index += 1
            final_tail_page = self.db.get_page(page_range_index, final_tail_page_index + config.PAGE_RANGE, config.INDIRECTION_COLUMN)


        # The original indirection stored in the base record
        base_indirection_page = self.db.get_page(page_range_index, base_page_index, config.INDIRECTION_COLUMN)
        previous_indirection = base_indirection_page[page_offset]

        # Update base record's indirection to point to the new tail record to be added
        base_indirection_page[page_offset] = self.encode_indirection(final_tail_page_index + config.PAGE_RANGE, final_tail_page.get_num_record())
        final_tail_page.pin -= 1
        base_indirection_page.pin -= 1

        # Makes the latest tail record indirection point to the previus version
        target_indirection_page = self.db.get_page(page_range_index, final_tail_page_index + config.PAGE_RANGE, config.INDIRECTION_COLUMN)
        target_indirection_page.add_record(previous_indirection)
        target_indirection_page.pin -= 1

        # Timestamp
        target_timestamp_page = self.db.get_page(page_range_index, final_tail_page_index + config.PAGE_RANGE, config.TIMESTAMP_COLUMN)
        target_timestamp_page.add_record(self.get_time())  
        target_timestamp_page.pin -= 1

        
        schema_encoding = 0
        for i in range(config.METACOLUMN_NUM, self.num_columns + config.METACOLUMN_NUM):
            field = input_data[i - config.METACOLUMN_NUM]

            target_data_page = self.db.get_page(page_range_index, final_tail_page_index + config.PAGE_RANGE, i)

            # If no need to update field
            if field is None:
                # If there is previous update to this column
                if self.extract_bit(base_schema_page[page_offset], self.num_columns - (i - config.METACOLUMN_NUM) - 1):
                    # Copy value from previous update
                    previous_data_page = self.db.get_page(page_range_index, self.parseIndirection(previous_indirection), i)
                    target_data_page.add_record(previous_data_page[self.parseRecord(previous_indirection)]) 
                    schema_encoding += 1 << (self.num_columns - (i - config.METACOLUMN_NUM) - 1)
                    previous_data_page.pin -= 1
                else:
                    # Add null value
                    target_data_page.add_record(config.NULL_VAL)
            else:
                schema_encoding += 1 << (self.num_columns - (i - config.METACOLUMN_NUM) - 1)
                target_data_page.add_record(field)

            target_data_page.pin -= 1

        # Update scheme encoding in base record
        if base_schema_page[page_offset] != schema_encoding:
            base_schema_page[page_offset] = schema_encoding
        base_schema_page.pin -= 1

        # Add the schema encoding for the latest tail record
        target_schema_page = self.db.get_page(page_range_index, final_tail_page_index + config.PAGE_RANGE, config.SCHEMA_ENCODING_COLUMN)
        target_schema_page.add_record(schema_encoding)
        target_schema_page.pin -= 1

        return True

    def get_record(self, rid, projected_columns_index, version):
        page_range_index = self.parsePageRangeRID(rid)
        base_page_index = self.parseBasePageRID(rid)
        page_offset = self.parseRecord(rid)

        # Points to the latest tail record
        base_indirection_page = self.db.get_page(page_range_index, base_page_index, config.INDIRECTION_COLUMN)
        curr_indirection = base_indirection_page[page_offset]
        base_indirection_page.pin -= 1

        # Scan from latest version until specified version or base record
        for i in range(0 , version, -1):
            if self.parseIndirection(curr_indirection) < config.PAGE_RANGE:
                break
            tail_indirection_page = self.db.get_page(page_range_index, self.parseIndirection(curr_indirection), config.INDIRECTION_COLUMN)
            curr_indirection = tail_indirection_page[self.parseRecord(curr_indirection)]
            tail_indirection_page.pin -= 1

        rtn_record = []
        # Forms the return based on the projected_columns_index, only if it is 1 will it be appended
        for i in range(self.num_columns):
            if projected_columns_index[i]:
                target_data_page = self.db.get_page(page_range_index, self.parseIndirection(curr_indirection), i + config.METACOLUMN_NUM)
                val = target_data_page[self.parseRecord(curr_indirection)]
                target_data_page.pin -= 1
                if val != config.NULL_VAL:
                    rtn_record.append(val)
                else:
                    base_data_page = self.db.get_page(page_range_index, base_page_index, i + config.METACOLUMN_NUM)
                    rtn_record.append(base_data_page[page_offset])
                    base_data_page.pin -= 1

        return rtn_record

    def delete_record(self, rid):
        page_range_index = self.parsePageRangeRID(rid)
        base_page_index = self.parseBasePageRID(rid)
        page_offset = self.parseRecord(rid)

        # Sets indirection of base record to NULL_VAL to imply deletion
        base_indirection_page = self.db.get_page(page_range_index, base_page_index, config.INDIRECTION_COLUMN)
        base_indirection_page[page_offset] = config.NULL_VAL
        base_indirection_page.pin -= 1

        # Remove the rid of this record from index
        base_key_page = self.db.get_page(page_range_index, base_page_index, self.key + config.METACOLUMN_NUM)
        del self.index.indices[self.key][base_key_page[page_offset]]
        base_key_page.pin -= 1

    def add_base_page(self):
        # If no existing page range or current page range is full of base page, create new page range
        if not self.db.page_table or len(self.db.page_table[str(len(self.db.page_table)-1)]) == config.PAGE_RANGE:
            self.db.page_table[str(len(self.db.page_table))] = {"base_pages": {}, "tail_pages": {}}

        self.db.page_table[str(len(self.db.page_table)-1)]["base_pages"][str(len(self.db.page_table[str(len(self.db.page_table)-1)]["base_pages"]))] = {str(i) : -1 for i in range(self.num_columns + config.METACOLUMN_NUM)}

    def add_tail_page(self, page_range_index):
        self.db.page_table[str(page_range_index)]["tail_pages"][str(len(self.db.page_table[str(page_range_index)]["tail_pages"]))] = {str(i) : -1 for i in range(self.num_columns + config.METACOLUMN_NUM)}

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