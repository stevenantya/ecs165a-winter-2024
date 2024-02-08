#include "Table.h"

class Table;

Table::Table(std::string name, int key, int num_columns) : name{name}, key{key}, num_columns{num_columns} {
    Page** new_base_page = new Page*[num_columns + 4];

    for (int i = 0; i < num_columns + 4; i++) {
        new_base_page[i] = new Page();
    }
}

Table::~Table() {}

