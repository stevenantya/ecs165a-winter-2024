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

int Table::parseBasePageRID(std::string rid) {
    int base_page_idx = std::stoi(rid.substr(0, rid.find(":")));
    return base_page_idx;
}

int Table::parseRecordRID(std::string rid) {
    int record_idx = std::stoi(rid.substr(rid.find(":") + 1));
    return record_idx;
}

int64_t* Table::getRecord(std::string rid) {

    // "base_page_idx:record_idx. For instance 123:456"
    int base_page_idx = parseBasePageRID(rid);
    int record_idx = parseRecordRID(rid);

    Page* base_page = base_pages[base_page_idx];
    int64_t* records = new int64_t[num_columns];

    for (int i = 0; i < num_columns; i++) {
        Page page = base_page[i];
        records[i] = page[record_idx];
    }
    return records;
}
