# import data structure documentation: https://btrees.readthedocs.io/en/latest/index.html
# pip3 install BTrees
from BTrees._OOBTree import OOBTree
"""
A data strucutre holding indices for various columns of a table. Key column should be indexd by default, other columns can be indexed through this object. Indices are usually B-Trees, but other data structures can be used as well.
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
        # key column index which is indexed by default
        self.create_index(self.table.key)

    """
    # returns the location of all records with the given value on column "column"
    """

    def locate(self, column, value):
        if self.indices[column].has_key(value):
            return self.indices[column][value]
        return []

    """
    # Returns the RIDs of all records with values in column "column" between "begin" and "end"
    """

    def locate_range(self, begin, end, column):
        returnRIDs = []
        search = self.indices[column].values(min = begin, max = end)
        for element in search:
            returnRIDs.append(element)
        return returnRIDs

    """
    # optional: Create index on specific column
    """

    def create_index(self, column_number):
        """
        self.indices[column_number] = OOBTree()
        columnRecords = self.table. 
        for record in columnRecords:
            self.indices[column_number].insert(record)
        pass
        """

    """
    # optional: Drop index of specific column
    """

    def drop_index(self, column_number):
        #Am I allowed to drop the index of the primary key??
        if column_number != self.table.key:
            self.indices[column_number] =None 
        else:
            return