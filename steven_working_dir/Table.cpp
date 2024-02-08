#include "Table.h"

class Table;

Table::Table(std::string name, int key, int num_columns) : name{name}, key{key}, num_columns{num_columns} {
    Page* new_base_page = new Page[num_columns + 4];
    base_pages.push_back(new_base_page);
}

Table::~Table() {}

void Table::add_record(int64_t* input_data) {
    auto final_row = base_pages.back();

    if (final_row[0].get_num_record() == 512) {
        // New base page
        Page* new_base_page = new Page[num_columns + 4];
        base_pages.push_back(new_base_page);
    }

    for (int i = 0; i < num_columns; i++) {
        final_row[i].add_record(input_data[i]);
    }
}

void Table::display() {
    for (auto base_page : base_pages) {
        for (int i = 0; i < base_page->get_num_record(); i++) {
            for (int j = 0; j < num_columns; j++) {
                std::cout << base_page[j][i] << " ";
            }
            std::cout << "\n";
        }
    }
}

int64_t Table::parsePageRangeRID(int64_t *rid) {
    int64_t bitmask = 0b11111111111111111111111111111111111111111111111;
    int64_t page_range_idx = *rid & bitmask;
    return page_range_idx;
}

int Table::parseBasePageRID(int64_t *rid) {
    int64_t bitmask = 0b1111111;
    int base_page_idx = *rid & bitmask;
    *rid >>= 7;
    return base_page_idx;
}

int Table::parseRecordRID(int64_t *rid) {
    int64_t bitmask = 0b111111111;
    int record_idx = *rid & bitmask;
    *rid >>= 9;
    return record_idx;
}

int64_t* Table::getRecord(int64_t rid_static) {
    int64_t rid = rid_static;
    // "page_range_idx(48 bits) base_page_idx(7 bits) record_idx (9 bits)"

    int record_idx = parseRecordRID(&rid);
    int base_page_idx = parseBasePageRID(&rid);
    int page_range_idx = parsePageRangeRID(&rid);

    //todo: page range not implemented yet
    
    Page* base_page = base_pages[base_page_idx];
    int64_t* records = new int64_t[num_columns];

    for (int i = 0; i < num_columns; i++) {
        Page page = base_page[i];
        records[i] = page[record_idx];
    }
    return records;
}
