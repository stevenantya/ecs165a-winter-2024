import sys
from index import Index
from time import time
from page import Page

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
        self.NULL_VAL = sys.maxsize

        self.name = name
        self.key = key
        self.num_columns = num_columns
        self.page_ranges = []
        self.add_base_page()

    def __del__(self):
        pass  # Destructor logic here if needed

    def get_time(self):
        return int(time() * 1000)

    def add_record(self, input_data):
        final_row = self.page_ranges[-1]['base_pages'][-1]

        if final_row[0].get_num_record() == 512:
            self.add_base_page()

        final_row[0].add_record(self.encode_indirection(len(self.page_ranges[-1]['base_pages']) - 1, final_row[0].get_num_record()))  # Indirection
        final_row[1].add_record(self.get_time())  # Timestamp
        final_row[2].add_record(0)  # Schema Encoding

        for i in range(self.METACOLUMN_NUM, self.num_columns + self.METACOLUMN_NUM):
            final_row[i].add_record(input_data[i - self.METACOLUMN_NUM])

    def update_record(self, rid, input_data):
        page_range_index = self.parsePageRangeRID(rid)
        base_page_index = self.parseBasePageRID(rid)
        page_offset = self.parseRecord(rid)

        target_page_range = self.page_ranges[page_range_index]

        if not target_page_range['tail_pages'] or target_page_range['tail_pages'][-1][0].get_num_record() == 512:
            self.add_tail_page(page_range_index)

        target_base_page = target_page_range['base_pages'][base_page_index]
        target_tail_page = target_page_range['tail_pages'][-1]

        previous_indirection = target_base_page[0][page_offset]
        # todo: fix bug
        indirection_index = self.parseIndirection(previous_indirection)

        if (indirection_index < 128):
            # First update
            previous_tail_page = target_page_range['base_pages'][indirection_index]
        else:
            # Subsequent updates
            previous_tail_page = target_page_range['tail_pages'][indirection_index - 128]

        target_base_page[0][page_offset] = self.encode_indirection(len(target_page_range['tail_pages']) - 1 + 128, target_tail_page[0].get_num_record())

        # if previous_indirection == self.NULL_VAL:
        #     target_tail_page[0].add_record(self.encode_indirection(base_page_index, page_offset))
        # else:
        #     target_tail_page[0].add_record(previous_indirection)

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
                    # Base value
                    target_tail_page[i].add_record(field)
            else:
                schema_encoding += 1 << (self.num_columns - (i - self.METACOLUMN_NUM) - 1)
                target_tail_page[i].add_record(field)

        target_base_page[2][page_offset] = schema_encoding
        target_tail_page[2].add_record(schema_encoding)

    def get_record(self, rid, projected_columns_index, version):
        page_range_index = self.parsePageRangeRID(rid)
        base_page_index = self.parseBasePageRID(rid)
        page_offset = self.parseRecord(rid)

        target_page_range = self.page_ranges[page_range_index]

        target_base_page = target_page_range['base_pages'][base_page_index]

        curr_indirection = target_base_page[0][page_offset]

        for i in range(version, 0, -1):
            if self.parseIndirection(curr_indirection) < 128:
                break
            curr_indirection = target_page_range['tail_pages'][self.parseIndirection(curr_indirection) - 128][0][self.parseRecord(curr_indirection)]

        if self.parseIndirection(curr_indirection) < 128:
            target_page = target_base_page
        else:
            target_page = target_page_range['tail_pages'][self.parseIndirection(curr_indirection) - 128]

        rtn_record = []
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

        target_base_page[0][page_offset] = self.NULL_VAL

    def add_base_page(self):
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

    def parsePageRangeRID(self, rid):
        bitmask = 0xFFFFFFFFFFFF0000
        page_range_idx = (rid & bitmask) >> 16
        return page_range_idx

    def parseBasePageRID(self, rid):
        bitmask = 0xFE00
        base_page_idx = (rid & bitmask) >> 9
        return base_page_idx

    def parseRecord(self, rid):
        bitmask = 0x1FF
        record_idx = rid & bitmask
        return record_idx

    def parseIndirection(self, rid):
        bitmask = 0xFFFFFFFFFFFFFE00
        tail_page_idx = (rid & bitmask) >> 9
        return tail_page_idx

    def encode_indirection(self, page_index, page_offset):
        indirection = page_offset
        indirection += (page_index << 9)
        return indirection

    def extract_bit(self, schema_encoding, position):
        bitmask = 1 << position
        return (schema_encoding & bitmask) != 0
