#include <string>
#include <vector>
#include "Page.h"
#include <iostream>

class Table {
    private:    
        std::string name;
        int key;
        int num_columns;

        std::vector<Page*> base_pages;
        std::vector<Page*> tail_pages;
        int64_t parsePageRangeRID(int64_t *rid);
        int parseBasePageRID(int64_t *rid);
        int parseRecordRID(int64_t *rid);

    public:
        Table(std::string name, int key, int num_columns);
        ~Table();

        void add_record(int64_t* input_data);
        int64_t* getRecord(int64_t rid);
        void display();
};