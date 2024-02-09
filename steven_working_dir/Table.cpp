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
    Page* final_row = page_ranges.back().base_pages.back();

    if (final_row[0].get_num_record() == 512) {
        add_base_page();
    }

    // 64 bits of all 1 has special meaning of null
    final_row[0].add_record(encode_indirection(page_ranges.back().base_pages.size() - 1, final_row[0].get_num_record()));  // Indirection
    final_row[1].add_record(get_time()); // Timestamp
    final_row[2].add_record(0); // Schema Encoding

    for (int i = METACOLUMN_NUM; i < num_columns + METACOLUMN_NUM; i++) {
        final_row[i].add_record(input_data[i - METACOLUMN_NUM]);
    }
}

void Table::update_record(const uint64_t& rid, int64_t* input_data) {
    uint64_t page_range_index = parsePageRangeRID(rid);
    int base_page_index = parseBasePageRID(rid);
    int page_offset = parseRecord(rid);

    PageRange& target_page_range = page_ranges[page_range_index];
    
    if ((target_page_range.tail_pages.size() == 0 ) || (target_page_range.tail_pages.back()[0].get_num_record() == 512)) {
        add_tail_page(page_range_index);
    }

    
    Page* target_base_page = target_page_range.base_pages[base_page_index];
    Page* target_tail_page = target_page_range.tail_pages.back();

    int64_t previous_indirection = target_base_page[0][page_offset];
    Page* previous_tail_page = target_page_range.tail_pages[parseIndirection(previous_indirection)];

    // Update base record to latest version tail record
    target_base_page[0][page_offset] = encode_indirection(target_page_range.tail_pages.size() - 1 + 128, target_tail_page[0].get_num_record());

    // indirection
    if (previous_indirection == NULL_VAL)
        target_tail_page[0].add_record(encode_indirection(base_page_index, page_offset));
    else 
        target_tail_page[0].add_record(previous_indirection);  


    target_tail_page[1].add_record(get_time()); // Timestamp
    
    int64_t schema_encoding = 0;
    for (int i = METACOLUMN_NUM; i < num_columns + METACOLUMN_NUM; i++) {
        int64_t& field = input_data[i - METACOLUMN_NUM];

        // if (field != NULL_VAL) 
        //     schema_encoding += 1 << (num_columns - (i - METACOLUMN_NUM) - 1);
        // target_tail_page[i].add_record(field);

        if (field == NULL_VAL) {
            if (extract_bit(target_base_page[2][page_offset], (num_columns - (i - METACOLUMN_NUM) - 1))) {  // Retrive last tail
                target_tail_page[i].add_record(previous_tail_page[i][parseRecord(previous_indirection)]);
                schema_encoding += 1 << (num_columns - (i - METACOLUMN_NUM) - 1);
            } else {
                target_tail_page[i].add_record(field);
            }
        } else {
            schema_encoding += 1 << (num_columns - (i - METACOLUMN_NUM) - 1);
            target_tail_page[i].add_record(field);
        }
    }

    target_base_page[2][page_offset] = schema_encoding;
    target_tail_page[2].add_record(schema_encoding);
}

std::vector<int64_t> Table::get_record(const uint64_t& rid, const std::vector<int>& projected_columns_index, const int& version) {
    uint64_t page_range_index = parsePageRangeRID(rid);
    int base_page_index = parseBasePageRID(rid);
    int page_offset = parseRecord(rid);

    PageRange& target_page_range = page_ranges[page_range_index];

    Page* target_base_page = target_page_range.base_pages[base_page_index];
  
    int64_t curr_indirection = target_base_page[0][page_offset];

    for (int i = 0; i > version; i--) {
        if (parseIndirection(curr_indirection) < 128)
            break;

        curr_indirection = target_page_range.tail_pages[parseIndirection(curr_indirection) - 128][0][parseRecord(curr_indirection)];        
    }

    Page* target_page;
    if (parseIndirection(curr_indirection) < 128) {
        target_page = target_base_page;
    } else {
        target_page = target_page_range.tail_pages[parseIndirection(curr_indirection) - 128];
    }

    std::vector<int64_t> rtn_record;
    for (int i = 0; i < num_columns; i++) {
        if (projected_columns_index[i]) {
            rtn_record.push_back(target_page[i + METACOLUMN_NUM][parseRecord(curr_indirection)]);
        }
    }

    return rtn_record;
}

void Table::delete_record(const uint64_t& rid) {
    uint64_t page_range_index = parsePageRangeRID(rid);
    int base_page_index = parseBasePageRID(rid);
    int page_offset = parseRecord(rid);

    PageRange& target_page_range = page_ranges[page_range_index];

    Page* target_base_page = target_page_range.base_pages[base_page_index];

    target_base_page[0][page_offset] = NULL_VAL;
}

void Table::add_base_page() {
    if ((page_ranges.size() == 0) || (page_ranges.back().base_pages.size() == 128)) {
        page_ranges.push_back(PageRange());
    }

    PageRange& final_page_range = page_ranges.back();
    Page* new_base_page = new Page[num_columns + METACOLUMN_NUM];
    final_page_range.base_pages.push_back(new_base_page);
}

void Table::add_tail_page(int page_range_index) {
    PageRange& target_page_range = page_ranges[page_range_index];
    Page* new_tail_page = new Page[num_columns + METACOLUMN_NUM];
    target_page_range.tail_pages.push_back(new_tail_page);
}

void Table::display() {
    std::cout << std::hex;

    for (auto pr : page_ranges) {
        for (auto base_page : pr.base_pages) {
            for (int i = 0; i < base_page->get_num_record(); i++) {
                for (int j = 0; j < num_columns + METACOLUMN_NUM; j++) {
                    std::cout << std::setw(16) << base_page[j][i] << "    ";
                }
                std::cout << "\n";
            }
        }
        std::cout << "--------------------------------------------------------------------------------------------------------------------------------------------------\n";
        for (auto base_page : pr.tail_pages) {
            for (int i = 0; i < base_page->get_num_record(); i++) {
                for (int j = 0; j < num_columns + METACOLUMN_NUM; j++) {
                    std::cout << std::setw(16) << base_page[j][i] << "    ";
                }
                std::cout << "\n";
            }
        }
    }
}

uint64_t Table::parsePageRangeRID(const uint64_t& rid) {
    int64_t bitmask = 0xFFFFFFFFFFFF0000;
    uint64_t page_range_idx = (rid & bitmask) >> 16;

    return page_range_idx;
}

int Table::parseBasePageRID(const uint64_t& rid) {
    int64_t bitmask = 0xFE00;
    int base_page_idx = (rid & bitmask) >> 9;

    return base_page_idx;
}

int Table::parseRecord(const uint64_t& rid) {
    int64_t bitmask = 0x1FF;
    int record_idx = rid & bitmask;
    
    return record_idx;
}

uint64_t Table::parseIndirection(const int64_t& rid) {
    int64_t bitmask = 0xFFFFFFFFFFFFFE00;
    uint64_t tail_page_idx = (rid & bitmask) >> 9;

    return tail_page_idx;
}

uint64_t Table::encode_indirection(const uint64_t& page_index, const int& page_offset) {
    uint64_t indirection = page_offset;
    indirection += page_index << 9;

    return indirection;
}

bool Table::extract_bit(const int64_t& schema_encoding, const int& position) {
    int64_t bitmask = 1LL << position;

    return (schema_encoding & bitmask) != 0;
}

// int64_t* Table::getRecord(int64_t rid_static) {
//     int64_t rid = rid_static;
//     // "page_range_idx(48 bits) base_page_idx(7 bits) record_idx (9 bits)"

//     int record_idx = parseRecordRID(&rid);
//     int base_page_idx = parseBasePageRID(&rid);
//     int page_range_idx = parsePageRangeRID(&rid);

//     //todo: page range not implemented yet

//     Page* base_page = base_pages[base_page_idx];
//     int64_t* records = new int64_t[num_columns];

//     for (int i = 0; i < num_columns; i++) {
//         int64_t record = base_page[i][record_idx];
//         records[i] = record;
//     }
//     return records;
// }
