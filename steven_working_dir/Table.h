#include <string>
#include <vector>
#include "Page.h"
#include <chrono>
#include <ctime>

#include <iostream>
#include <iomanip>
#include <bitset>

class Table {
    private:    
        const int PAGE_RANGE = 16;
        const int METACOLUMN_NUM = 3;
        const int64_t NULL_VAL = std::numeric_limits<uint64_t>::max();

        std::string name;
        int key;
        int num_columns;

        struct PageRange {
            std::vector<Page*> base_pages;
            std::vector<Page*> tail_pages;
        };

        std::vector<PageRange> page_ranges;

        void add_base_page();
        void add_tail_page(int page_range_index);

        int64_t get_time();
        
        uint64_t parsePageRangeRID(const uint64_t& rid);
        int parseBasePageRID(const uint64_t& rid);
        uint64_t parseIndirection(const int64_t& rid);
        int parseRecord(const uint64_t& rid);

        uint64_t encode_indirection(const uint64_t& tail_page_index, const int& page_offset);
        bool extract_bit(const int64_t& schema_encoding, const int& position);

    public:
        Table(std::string name, int key, int num_columns);
        ~Table();

        void add_record(int64_t* input_data);
        void update_record(const uint64_t& rid, int64_t* input_data);
        std::vector<int64_t> get_record(const uint64_t& rid, const std::vector<int>& projected_columns_index, const int& version);
        void delete_record(const uint64_t& rid);

        int64_t* getRecord(int64_t rid);
        void display();
};