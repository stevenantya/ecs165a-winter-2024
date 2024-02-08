#include <string>
#include <vector>
#include "Page.h"

class Table {
    private:    
        std::string name;
        int key;
        int num_columns;

        std::vector<Page**> base_pages;
        std::vector<Page**> tail_pages;
        Page** tail_pages;

    public:
        Table(std::string name, int key, int num_columns);
        ~Table();
};