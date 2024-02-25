# B tree library: https://btrees.readthedocs.io/en/latest/index.html
# B tree structure documentation: https://btrees.readthedocs.io/en/latest/api.html#module-BTrees.Interfaces
# pip3 install BTrees
# from BTrees._OOBTree import OOBTree
"""
A data structure holding indices for various columns of a table. Key column should be indexd by default, other columns can be indexed through this object. Indices are usually B-Trees, but other data structures can be used as well.
Index is a list of dictionaries, for value k: <k, list of rids of data records with search key k> 
"""
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
        if value in self.indices[column]:
            return self.indices[column][value]
        
        return False

    """
    # Returns the RIDs of all records with values in column "column" between "begin" and "end"
    """

    def locate_range(self, begin, end, column):
        returnRIDs = []
        for key in range(begin, end + 1):
            if key in self.indices[column]:
                returnRIDs += self.indices[column][key]

        return returnRIDs

    """
    # optional: Create index on specific column
    """

    def create_index(self, column_number):
        self.indices[column_number] = {}

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
        for i in range(1, self.table.num_columns):
            #print(self.indices)
            #print(self.indices[i])
            if self.indices[i] != None:
                if data[i] in self.indices[i]:
                    self.indices[i][data[i]].append(rid)
                else:
                    self.indices[i][data[i]] = [rid]
            else:
                self.create_index(i)
                self.indices[i][data[i]]= [rid]
                #print(rid)
        #print(self.indices)
    """
    delete all rids in dict with a specified rid value. Ex. 1: [1,2,3,4] becomes 1:[2,3,4] if specified 1
    Used in conjunction with insert_record to update_record:
    1. remove all mentions of a specific rid (removed all mentionf of a record)
    2. inserts the record with new values using insert_record, simulating an update
    not implemented in table class yet of update_record
    """
    def delete_rid(self, rid):
        for i in range(1, self.table.num_columns):
            for key in self.indices[i]:
                if rid in self.indices[i][key]:
                    self.indices[i][key].remove(rid)
                