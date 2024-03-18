import sys
from .index import Index
from time import time
from .page import Page
from . import config
import math
import threading
from . rwlock import RWLock

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
        self.tail_page_merge_stack = {}
        self.index = Index(self)
        self.lock_manager = {}
        self.lock = threading.Lock()

    def __del__(self):
        pass  # Destructor logic here if needed

    def get_time(self):
        return int(time())
    
    def undo_delete(self, b_rid, l_tid):
        b_pi = self.parsePageRangeRID(b_rid)
        b_p  = self.parseBasePageRID(b_rid)
        b_pg = self.parseRecord(b_rid)
        base_indirection_page = self.db.get_page(b_pi, b_p, 0)
        base_indirection_page[b_pg] = l_tid
    
    def undo_update(self, b_rid, o_tid, n_tid):
        b_pi = self.parsePageRangeRID(b_rid)
        b_p  = self.parseBasePageRID(b_rid)
        b_pg = self.parseRecord(b_rid)

        base_indirection_page = self.db.get_page(b_pi, b_p, 0)
        
        base_indirection_page[b_pg] = o_tid

        t_pi = self.parsePageRangeRID(n_tid)
        t_p  = self.parseBasePageRID(n_tid)
        t_pg = self.parseRecord(n_tid)

        tail_indirection_page = self.db.get_page(t_pi, t_p, 0)
        tail_indirection_page[t_pg] = None

        # find the record with the correspond b_rid
        # change the indirection column to point to o_tid
        # change the n_tid to point to NULL


    def add_record(self, transaction, input_data): #This will write to a page in the bufferpool
        # Existing record with the same key, so forbid adding a duplicate record
        if input_data[self.key] in self.index.indices[self.key]:
            return False

        # Get to the first empty record inside a bottomost page range's bottomost base page
        final_page_range = len(self.db.page_table) - 1

        # self.lock.acquire() #was for merging

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

        # Getting the page in the bufferpool
        # Indirection
        indirection_page = self.db.get_page(final_page_range, final_base_page_index, config.INDIRECTION_COLUMN)
        indirection_page.add_record(self.encode_indirection(final_base_page_index, final_row_num))  
        # indirection_page.pin -= 1

        # Timestamp
        timestamp_page = self.db.get_page(final_page_range, final_base_page_index, config.TIMESTAMP_COLUMN)
        timestamp_page.add_record(self.get_time())
        # timestamp_page.pin -= 1
        
        # Schema encoding
        schema_page = self.db.get_page(final_page_range, final_base_page_index, config.SCHEMA_ENCODING_COLUMN)
        schema_page.add_record(0)
        # schema_page.pin -= 1

        # Add the data value to respective columns
        for i in range(config.METACOLUMN_NUM, self.num_columns + config.METACOLUMN_NUM):
            data_page = self.db.get_page(final_page_range, final_base_page_index, i)
            data_page.add_record(input_data[i - config.METACOLUMN_NUM])
            # data_page.pin -= 1

        # self.lock.release() #was for merging

        rid = self.encode_RID(final_page_range, final_base_page_index, final_row_num)
        transaction.logger[-1].append(rid)
        self.index.insert_record(rid, input_data)

        # Add new record to lock manager
        self.lock_manager[rid] = RWLock()
        return True

    def update_record(self, transaction, rid, input_data, layer = 0):
        # Existing record with the same key, so forbid adding a duplicate record
        if input_data[self.key] in self.index.indices[self.key] and self.index.indices[self.key][input_data[self.key]] != rid:
            return False
        
        # Checks if lock can be accessed
        if (rid not in self.lock_manager):
                self.lock_manager[rid] = RWLock()
                self.lock_manager[rid].acquire_exclusive_lock()
        elif (self.lock_manager[rid] == None):
                self.lock_manager[rid] = RWLock()
                self.lock_manager[rid].acquire_exclusive_lock()
        else:
            lock_status = self.lock_manager[rid].acquire_exclusive_lock()
            if lock_status == False:
                return False

        page_range_index = self.parsePageRangeRID(rid)
        base_page_index = self.parseBasePageRID(rid)
        page_offset = self.parseRecord(rid)

        transaction.logger[-1].append(rid) # append base_rid to logger

        #self.lock.acquire() #was for merging

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

        #self.lock.release() #was for merging

        if layer == 0 and old_values != [None] * self.num_columns:
            self.update_record(rid, old_values, 1)

        #self.lock.acquire() #was for merging

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

        transaction.logger[-1].append(previous_indirection) # append old_tail_rid to logger

        # Update base record's indirection to point to the new tail record to be added
        base_indirection_page[page_offset] = self.encode_indirection(final_tail_page_index + config.PAGE_RANGE, final_tail_page.get_num_record())
        
        updated_base_indirection_page = self.db.get_page(page_range_index, base_page_index, config.INDIRECTION_COLUMN)
        latest_indirection = updated_base_indirection_page[page_offset]
        transaction.logger[-1].append(latest_indirection) # append latest_tail_rid to logger
        
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

        # BaseID
        base_ID_page = self.db.get_page(page_range_index, final_tail_page_index + config.PAGE_RANGE , config.BASE_ID_COLUMN)
        base_ID_page.add_record(rid)
        base_ID_page.pin -= 1

        
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
        target_tail_num_records = target_schema_page.get_num_record()
        target_schema_page.pin -= 1

        #self.lock.release() #was for merging

        # Update the indexes to reflect changes to this record
        self.index.delete_rid(rid, input_data)
        self.index.insert_record(rid, input_data)

        # # Append the full tail page to the queue and initiate merging once size of queue pass max size
        # if target_tail_num_records == config.PAGE_MAX_ROWS:
        #     if page_range_index in self.tail_page_merge_stack:
        #         self.tail_page_merge_stack[page_range_index].append(final_tail_page_index + config.PAGE_RANGE)
        #     else:
        #         self.tail_page_merge_stack[page_range_index] = [final_tail_page_index + config.PAGE_RANGE]

        #     if len(self.tail_page_merge_stack[page_range_index]) == config.MERGE_STACK_SIZE:
        #         merge_thread = threading.Thread(target = self.merge, args=(page_range_index,))
        #         merge_thread.start()
        #         # self.merge(page_range_index)

        return True

    def get_record(self, rid, projected_columns_index, version):
        # Checks if lock can be accessed
                # Checks if lock can be accessed
        if (rid not in self.lock_manager):
                self.lock_manager[rid] = RWLock()
                self.lock_manager[rid].acquire_shared_lock()
        elif (self.lock_manager[rid] == None):
                self.lock_manager[rid] = RWLock()
                self.lock_manager[rid].acquire_shared_lock()
        else:
            lock_status = self.lock_manager[rid].acquire_shared_lock()
            if lock_status == False:
                return False
            
        page_range_index = self.parsePageRangeRID(rid)
        base_page_index = self.parseBasePageRID(rid)
        page_offset = self.parseRecord(rid)

        #self.lock.acquire() #was for merging

        # Points to the latest tail record
        base_indirection_page = self.db.get_page(page_range_index, base_page_index, config.INDIRECTION_COLUMN)
        curr_indirection = base_indirection_page[page_offset]
        base_indirection_page.pin -= 1

        # Set target to base page if latest version is already in base page after merging
        if version == 0 and curr_indirection <= self.db.page_TPS[str(page_range_index)][str(base_page_index)]:
            curr_indirection = self.encode_indirection(base_page_index, page_offset)

        # Scan from latest version until specified version or reach first version in tail page
        for i in range(0 , version, -1):
            tail_indirection_page = self.db.get_page(page_range_index, self.parseIndirection(curr_indirection), config.INDIRECTION_COLUMN)
            prev = curr_indirection
            curr_indirection = tail_indirection_page[self.parseRecord(curr_indirection)]
            tail_indirection_page.pin -= 1

            if self.parseIndirection(curr_indirection) < config.PAGE_RANGE:
                curr_indirection = prev
                break

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

        #self.lock.release() #was for merging

        return rtn_record

    def delete_record(self, transaction, rid):
        # Checks if lock can be accessed
        if (rid not in self.lock_manager):
                self.lock_manager[rid] = RWLock()
                self.lock_manager[rid].acquire_exclusive_lock()
        elif (self.lock_manager[rid] == None):
                self.lock_manager[rid] = RWLock()
                self.lock_manager[rid].acquire_exclusive_lock()
        else:
            lock_status = self.lock_manager[rid].acquire_exclusive_lock()
            if lock_status == False:
                return False
            
        page_range_index = self.parsePageRangeRID(rid)
        base_page_index = self.parseBasePageRID(rid)
        page_offset = self.parseRecord(rid)

        #self.lock.acquire() #was for merging

        transaction.logger[-1].append(rid) # append base_rid to logger

        # Sets indirection of base record to NULL_VAL to imply deletion
        base_indirection_page = self.db.get_page(page_range_index, base_page_index, config.INDIRECTION_COLUMN)
        
        transaction.logger[-1].append(base_indirection_page[page_offset]) # append latest_tail_rid to logger
        base_indirection_page[page_offset] = config.NULL_VAL # set indirection to null indicating deletion
        base_indirection_page.pin -= 1

        #self.lock.release() #was for merging

        # Remove the rid of this record from index
        self.index.delete_rid(rid, [1] * self.num_columns)

    def add_base_page(self):
        # If no existing page range or current page range is full of base page, create new page range
        if not self.db.page_table or len(self.db.page_table[str(len(self.db.page_table)-1)]["base_pages"]) == config.PAGE_RANGE:
            self.db.page_table[str(len(self.db.page_table))] = {"base_pages": {}, "tail_pages": {}}
            self.db.page_TPS[str(len(self.db.page_table) - 1)] = {}

        self.db.page_table[str(len(self.db.page_table)-1)]["base_pages"][str(len(self.db.page_table[str(len(self.db.page_table)-1)]["base_pages"]))] = {str(i) : -1 for i in range(self.num_columns + config.METACOLUMN_NUM)}
        self.db.page_TPS[str(len(self.db.page_table)-1)][str(len(self.db.page_table[str(len(self.db.page_table)-1)]["base_pages"]) - 1)] = -1

    def add_tail_page(self, page_range_index):
        self.db.page_table[str(page_range_index)]["tail_pages"][str(len(self.db.page_table[str(page_range_index)]["tail_pages"]))] = {str(i) : -1 for i in range(self.num_columns + config.METACOLUMN_NUM)}

    def merge(self, page_range_index):
        base_rid_set = set()
        base_page_set = set()

        self.lock.acquire() 

        # Obtain list of rids and base pages to merge
        for i in range(config.MERGE_STACK_SIZE):
            tail_page_index = self.tail_page_merge_stack[page_range_index][i]
            tail_baseID_page = self.db.get_page(page_range_index, tail_page_index, config.BASE_ID_COLUMN)
            for baseID in tail_baseID_page.rows:
                base_rid_set.add(baseID)
                base_page_set.add(self.parseBasePageRID(baseID))
            tail_baseID_page.pin -= 1
        
        # Load copies of needed base_pages
        base_page_copies = {}
        for base_page_index in base_page_set:
            base_page_copies[base_page_index] = []
            for i in range(self.num_columns + config.METACOLUMN_NUM):
                if self.db.page_table[str(page_range_index)]["base_pages"][str(base_page_index)][str(i)] == -1:
                    base_page_copy = self.db.read_page(page_range_index, base_page_index, i)
                else:
                    base_page_copy = self.db.bufferpool[self.db.page_table[str(page_range_index)]["base_pages"][str(base_page_index)][str(i)]].copy()
                base_page_copy.pin -= 1
                base_page_copies[base_page_index].append(base_page_copy)

        tail_page_stack_size = len(self.tail_page_merge_stack[page_range_index])
        page_TPS = self.db.page_TPS.copy()
        # Merge tail records to base pages copies from bottom up
        for i in range(tail_page_stack_size - 1, -1, -1):
            tail_page_index = self.tail_page_merge_stack[page_range_index][i]
            tail_baseID_page = self.db.get_page(page_range_index, tail_page_index, config.BASE_ID_COLUMN)
            for r in range(config.PAGE_MAX_ROWS - 1, -1, -1):
                base_rid = tail_baseID_page[r]
                if base_rid in base_rid_set:
                    base_page_index = self.parseIndirection(base_rid)
                    page_offset = self.parseRecord(base_rid)
                    schema = base_page_copies[base_page_index][config.SCHEMA_ENCODING_COLUMN][page_offset]

                    # # Timestamp
                    # base_page_copies[base_page_index][config.TIMESTAMP_COLUMN][page_offset] = self.get_time()
                    # # Schema encoding
                    # base_page_copies[base_page_index][config.SCHEMA_ENCODING_COLUMN][page_offset] = 0

                    for c in range(config.METACOLUMN_NUM, self.num_columns + config.METACOLUMN_NUM):
                        if self.extract_bit(schema, self.num_columns - (c - config.METACOLUMN_NUM) - 1):
                            tail_data_page = self.db.get_page(page_range_index, tail_page_index, c)
                            base_page_copies[base_page_index][c][page_offset] = tail_data_page[r]
                            tail_data_page.pin -= 1

                    # Update TPS
                    page_TPS[str(page_range_index)][str(base_page_index)] = max(page_TPS[str(page_range_index)][str(base_page_index)], self.encode_indirection(tail_page_index, r))

                    base_rid_set.remove(base_rid)

            tail_baseID_page.pin -= 1
            self.tail_page_merge_stack[page_range_index].remove(tail_page_index)

        # Look for dirty pages in base_page_copies
        for base_page_index in base_page_copies:
            for i, base_page_page in enumerate(base_page_copies[base_page_index]):
                if base_page_page.dirty:
                    bufferpool_index = self.db.page_table[str(page_range_index)]["base_pages"][str(base_page_index)][str(i)]
                    # Evict LRU 
                    if bufferpool_index == -1:
                        # Eviction
                        # Find LRU unpinned page
                        for bufferpool_index in self.db.page_stack:
                            if self.db.bufferpool[bufferpool_index].pin == 0:
                                break

                        # Reset the existing page's bufferpool index in page table to -1
                        self.db.reset_page_table_entry(bufferpool_index)
                    
                        # Overwrite existing file if evicting dirty page
                        if self.db.bufferpool[bufferpool_index].dirty:
                            self.db.evict_page(bufferpool_index)

                    self.db.bufferpool[bufferpool_index] = base_page_page
                    self.db.page_table[str(page_range_index)]["base_pages"][str(base_page_index)][str(i)] = bufferpool_index

        # Replace the outdated TPS
        self.db.page_TPS = page_TPS

        self.lock.release()

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