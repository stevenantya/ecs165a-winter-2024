import sys
from .index import Index
from time import time
from .page import Page

INDIRECTION_COLUMN = 0
RID_COLUMN = 1
TIMESTAMP_COLUMN = 2
SCHEMA_ENCODING_COLUMN = 3


class Record:

    def __init__(self, rid, key, columns):
        self.rid = rid
        self.key = key
        self.columns = columns

class Table:
    def __init__(self, name, key, num_columns):
        self.PAGE_RANGE = 128
        self.METACOLUMN_NUM = 3
        self.NULL_VAL = 2 ** 64 - 1

        self.name = name
        self.key = key
        self.num_columns = num_columns
        self.page_ranges = []
        self.add_base_page()

    def __del__(self):
        pass  # Destructor logic here if needed

    def get_time(self):
        return int(time())

    def add_record(self, input_data):
        # Get to the first empty record inside a bottomost page range's bottomost basae page
        final_row = self.page_ranges[-1]['base_pages'][-1]

        # Add a new base page if current bottomost base page is full
        if final_row[0].get_num_record() == 512:
            self.add_base_page()
            final_row = self.page_ranges[-1]['base_pages'][-1]

        final_row[0].add_record(self.encode_indirection(len(self.page_ranges[-1]['base_pages']) - 1, final_row[0].get_num_record()))  # Indirection
        final_row[1].add_record(self.get_time())  # Timestamp
        final_row[2].add_record(0)  # Schema Encoding

        # Add the data value to respective columns
        for i in range(self.METACOLUMN_NUM, self.num_columns + self.METACOLUMN_NUM):
            final_row[i].add_record(input_data[i - self.METACOLUMN_NUM])

    def update_record(self, rid, input_data):
        page_range_index = self.parsePageRangeRID(rid)
        base_page_index = self.parseBasePageRID(rid)
        page_offset = self.parseRecord(rid)

        target_page_range = self.page_ranges[page_range_index]

        # Adds a new tail page if there isn't an existing one or the existing one is full
        if not target_page_range['tail_pages'] or target_page_range['tail_pages'][-1][0].get_num_record() == 512:
            self.add_tail_page(page_range_index)

        target_base_page = target_page_range['base_pages'][base_page_index]
        target_tail_page = target_page_range['tail_pages'][-1]

        # The original indirection stored in the base record
        previous_indirection = target_base_page[0][page_offset]
        indirection_index = self.parseIndirection(previous_indirection)

        if (indirection_index < 128):
            # First update
            previous_tail_page = target_page_range['base_pages'][indirection_index]
        else:
            # Subsequent updates
            previous_tail_page = target_page_range['tail_pages'][indirection_index - 128]

        # Update base record's indirection to points to the new tail record to be added
        target_base_page[0][page_offset] = self.encode_indirection(len(target_page_range['tail_pages']) - 1 + 128, target_tail_page[0].get_num_record())

        # Makes the latest tail record indirection point to the previus version
        target_tail_page[0].add_record(previous_indirection)

        target_tail_page[1].add_record(self.get_time())  # Timestamp

        schema_encoding = 0
        for i in range(self.METACOLUMN_NUM, self.num_columns + self.METACOLUMN_NUM):
            field = input_data[i - self.METACOLUMN_NUM]

            # If no need to update field
            if field == self.NULL_VAL:
                # If there is previous update to this column
                if self.extract_bit(target_base_page[2][page_offset], self.num_columns - (i - self.METACOLUMN_NUM) - 1):
                    # Copy value from previous update
                    target_tail_page[i].add_record(previous_tail_page[i][self.parseRecord(previous_indirection)])
                    schema_encoding += 1 << (self.num_columns - (i - self.METACOLUMN_NUM) - 1)
                else:
                    # Add null value
                    target_tail_page[i].add_record(field)
            else:
                schema_encoding += 1 << (self.num_columns - (i - self.METACOLUMN_NUM) - 1)
                target_tail_page[i].add_record(field)

        # Update scheme encoding in base record
        target_base_page[2][page_offset] = schema_encoding
        # Add the schema encoding for the latest tail record
        target_tail_page[2].add_record(schema_encoding)

    def get_record(self, rid, projected_columns_index, version):
        page_range_index = self.parsePageRangeRID(rid)
        base_page_index = self.parseBasePageRID(rid)
        page_offset = self.parseRecord(rid)

        target_page_range = self.page_ranges[page_range_index]

        target_base_page = target_page_range['base_pages'][base_page_index]
        # Points to the latest tail record
        curr_indirection = target_base_page[0][page_offset]

        for i in range(0, version, -1):
            # Arrive at the base record so can stop moving
            if self.parseIndirection(curr_indirection) < 128:
                break
            curr_indirection = target_page_range['tail_pages'][self.parseIndirection(curr_indirection) - 128][0][self.parseRecord(curr_indirection)]

        # Find the target record's page
        if self.parseIndirection(curr_indirection) < 128:
            target_page = target_base_page
        else:
            target_page = target_page_range['tail_pages'][self.parseIndirection(curr_indirection) - 128]

        rtn_record = []
        # Forms the return based on the projected_columns_index, only if it is 1 will it be appended
        for i in range(self.num_columns):
            if projected_columns_index[i]:
                rtn_record.append(target_page[i + self.METACOLUMN_NUM][self.parseRecord(curr_indirection)])

        return rtn_record

    def delete_record(self, rid):
        page_range_index = self.parsePageRangeRID(rid)
        base_page_index = self.parseBasePageRID(rid)
        page_offset = self.parseRecord(rid)

        target_page_range = self.page_ranges[page_range_index]
        target_base_page = target_page_range['base_pages'][base_page_index]

        # Sets indirection of base record to NULL_VAL to imply deletion
        target_base_page[0][page_offset] = self.NULL_VAL

    def add_base_page(self):
        # If current page range is full of base page, create new page range
        if not self.page_ranges or len(self.page_ranges[-1]['base_pages']) == 128:
            self.page_ranges.append({'base_pages': [], 'tail_pages': []})

        new_base_page = [Page() for _ in range(self.num_columns + self.METACOLUMN_NUM)]
        self.page_ranges[-1]['base_pages'].append(new_base_page)

    def add_tail_page(self, page_range_index):
        target_page_range = self.page_ranges[page_range_index]
        new_tail_page = [Page() for _ in range(self.num_columns + self.METACOLUMN_NUM)]
        target_page_range['tail_pages'].append(new_tail_page)

    def display(self):
        for pr in self.page_ranges:
            for base_page in pr['base_pages']:
                for i in range(base_page[0].get_num_record()):  # Assuming get_num_record() method exists
                    print('    '.join(f'{base_page[j][i]:016x}' for j in range(self.num_columns + self.METACOLUMN_NUM)))
                print('-' * 150)
            for tail_page in pr['tail_pages']:
                for i in range(tail_page[0].get_num_record()):  # Assuming get_num_record() method exists
                    print('    '.join(f'{tail_page[j][i]:016x}' for j in range(self.num_columns + self.METACOLUMN_NUM)))
                print('-' * 150)

    # Extract the leftmost 48 bits of rid to get page range
    def parsePageRangeRID(self, rid):
        bitmask = 0xFFFFFFFFFFFF0000
        page_range_idx = (rid & bitmask) >> 16
        return page_range_idx

    # Extract the in between 7 bits of rid to get base page
    def parseBasePageRID(self, rid):
        bitmask = 0xFE00
        base_page_idx = (rid & bitmask) >> 9
        return base_page_idx

    # Extract the rightmost 9 bits to get page offset
    def parseRecord(self, rid):
        bitmask = 0x1FF
        record_idx = rid & bitmask
        return record_idx

    #Extract the leftmost 55 bits from indirection to get page number inside page range
    def parseIndirection(self, rid):
        bitmask = 0xFFFFFFFFFFFFFE00
        tail_page_idx = (rid & bitmask) >> 9
        return tail_page_idx

    # Encodes indirection in the form of 55 + 9 bits, page index and page offset respectively
    def encode_indirection(self, page_index, page_offset):
        indirection = page_offset
        indirection += (page_index << 9)
        return indirection

    # Checks if the i-th position bit is 0 or 1 
    def extract_bit(self, schema_encoding, position):
        bitmask = 1 << position
        return (schema_encoding & bitmask) != 0
