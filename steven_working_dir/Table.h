#include <string>
#include <vector>
#include "Page.h"
#include <iostream>
#include <chrono>
#include <ctime>

class Table {
    private:    
        const int PAGE_RANGE = 16;

        std::string name;
        int key;
        int num_columns;

        struct PageRange {
            std::vector<Page*> base_pages;
            std::vector<Page*> tail_pages;
        };

        std::vector<PageRange> page_ranges;

        int64_t get_time();

    public:
        Table(std::string name, int key, int num_columns);
        ~Table();

        void add_record(int64_t* input_data);
        void add_base_page();
        void add_tail_page(int page_range_index);

        void display();
};