#include "Table.h"

class Table;

Table::Table(std::string name, int key, int num_columns) : name{name}, key{key}, num_columns{num_columns} {
    add_base_page();
}

Table::~Table() {}

int64_t Table::get_time() {
    std::chrono::time_point<std::chrono::system_clock> currentTime = std::chrono::system_clock::now();
    std::time_t currentTime_t = std::chrono::system_clock::to_time_t(currentTime);
    return static_cast<int64_t>(currentTime_t);
}

void Table::add_record(int64_t* input_data) {
    auto final_row = page_ranges.back().base_pages.back();

    if (final_row[0].get_num_record() == 512) {
        add_base_page();
    }

    final_row[0].add_record(reinterpret_cast<int64_t>(&final_row));
    final_row[1].add_record(get_time());
    final_row[2].add_record(0);

    for (int i = 3; i < num_columns + 3; i++) {
        final_row[i].add_record(input_data[i-3]);
    }
}

void Table::add_base_page() {
    if ((page_ranges.size() == 0) || (page_ranges.back().base_pages.size() == 16)) {
        page_ranges.push_back(PageRange());
    }

    PageRange& final_page_range = page_ranges.back();
    Page* new_base_page = new Page[num_columns + 3];
    final_page_range.base_pages.push_back(new_base_page);
}

void Table::add_tail_page(int page_range_index) {
    PageRange target_page_range = page_ranges[page_range_index];
    Page* new_tail_page = new Page[num_columns + 3];
    target_page_range.tail_pages.push_back(new_tail_page);
}

void Table::display() {
    for (auto pr : page_ranges) {
        for (auto base_page : pr.base_pages) {
            for (int i = 0; i < base_page->get_num_record(); i++) {
                for (int j = 0; j < num_columns+3; j++) {
                    std::cout << base_page[j][i] << " ";
                }
                std::cout << "\n";
            }
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
        int64_t record = base_page[i][record_idx];
        records[i] = record;
    }
    return records;
}
