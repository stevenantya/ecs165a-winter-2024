# B tree library: https://btrees.readthedocs.io/en/latest/index.html
# B tree structure documentation: https://btrees.readthedocs.io/en/latest/api.html#module-BTrees.Interfaces
# pip3 install BTrees
# from BTrees._OOBTree import OOBTree
"""
A data structure holding indices for various columns of a table. Key column should be indexd by default, other columns can be indexed through this object. Indices are usually B-Trees, but other data structures can be used as well.
"""
'''
INDIRECTION_COLUMN = 0
RID_COLUMN = 1
TIMESTAMP_COLUMN = 2
SCHEMA_ENCODING_COLUMN = 3
'''
class Index:

    def __init__(self, table):
        self.table = table
        # One index for each table. All our empty initially.
        # index corresponds with column number
        self.indices = [None] *  table.num_columns

    """
    # returns the location of all records with the given value on column "column"
    """

    def locate(self, column, value):
        if self.indices[column] is not None:
            if value in self.indices[column]:
                return self.indices[column][value]
            else:
                return False
        else:
            rid_list = self.scan_rids()
            rtn_list = []
            for rid in rid_list:
                val = self.table.get_record(rid, [1 if i == column else None for i in range(self.table.num_columns)], 0)[0]
                if val == value:
                    rtn_list.append(rid)

            return rtn_list

    """
    # Returns the RIDs of all records with values in column "column" between "begin" and "end"
    """

    # def locate_range(self, begin, end, column):
    #     returnRIDs = []
    #     for key in range(begin, end + 1):
    #         if key in self.indices[column]:
    #             returnRIDs += self.indices[column][key]

    #     return returnRIDs

    """
    # optional: Create index on specific column
    """

    def create_index(self, column_number):
        self.indices[column_number] = {}

        if column_number != self.table.key:
            rid_list = self.scan_rids()

            for rid in rid_list:
                val = self.table.get_record(rid, [1 if i == column_number else None for i in range(self.table.num_columns)], 0)[0]
                self.insert_record(rid, [val if i == column_number else None for i in range(self.table.num_columns)])

    """
    # optional: Drop index of specific column
    """

    def drop_index(self, column_number):
        #Am I allowed to drop the index of the primary key??
        if column_number != self.table.key:
            self.indices[column_number] = None 
        else:
            return
        
    """
    Inserts a record into the index
    Index is a list of dictionaries, for value k: <k, list of rids of data records with search key k> 
    """ 
    def insert_record(self, rid, data):
        for i in range(self.table.num_columns):
            if data[i] is not None and self.indices[i] != None:
                    if data[i] in self.indices[i]:
                        self.indices[i][data[i]].append(rid)
                    else:
                        self.indices[i][data[i]] = [rid]
                
    """
    delete all rids in dict with a specified rid value. Ex. 1: [1,2,3,4] becomes 1:[2,3,4] if specified 1
    Used in conjunction with insert_record to update_record:
    1. remove all mentions of a specific rid (removed all mentionf of a record)
    2. inserts the record with new values using insert_record, simulating an update
    not implemented in table class yet of update_record, WIP
    """
    def delete_rid(self, rid, columns):
        for i in range(self.table.num_columns):
            if columns[i] is not None and self.indices[i] is not None:
                for key in self.indices[i]:
                    if rid in self.indices[i][key]:
                        self.indices[i][key].remove(rid)

    def scan_rids(self):
        rid_list = []
        for _, value in self.indices[self.table.key].items():
            rid_list += value

        return rid_list