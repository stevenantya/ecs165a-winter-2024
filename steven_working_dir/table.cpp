#include <iostream>
#include <vector>
#include <map>
#include "table.hpp"

// Constants for column indices
const int INDIRECTION_COLUMN = 0;
const int RID_COLUMN = 1;
const int TIMESTAMP_COLUMN = 2;
const int SCHEMA_ENCODING_COLUMN = 3;

class Record {
public:
    int rid; // Record ID
    int key; // Key for accessing this record
    std::vector<int> columns; // Data columns;

    // Constructor
    Record(int rid, int key, const std::vector<int>& columns) : rid(rid), key(key), columns(columns) {}
};

class Table {
private:
    
    std::string name; // Table name
    int num_columns; // Number of data columns
    int key; // Index of table key in columns
    std::map<int, Record*> page_directory; // Mapping of RIDs to Records
    Index index; // Index for the table, assuming Index is defined

    // Private method for merging (implementation not shown)
    void merge() {
        std::cout << "merge is happening" << std::endl;
    }

    public:
    // Constructor
    Table(const std::string& name, int num_columns, int key) : name(name), num_columns(num_columns), key(key), index(this) {}

    // Other public methods would go here
    /*
    std::string getName() {
        return name;
    }
    */
};