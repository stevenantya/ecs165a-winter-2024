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

    public:
        Table(std::string name, int key, int num_columns);
        ~Table();

        void add_record(int64_t* input_data);

        void display();
};