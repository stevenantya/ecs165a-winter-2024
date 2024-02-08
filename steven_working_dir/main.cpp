#include "Table.h"

int main() {
    Table t = Table("Test", 0, 5);

    int64_t data[] = {1, 2, 3, 4, 5};
    t.add_record(data);
    t.add_record(data);
    t.add_record(data);
    t.add_record(data);

    printf("%s", t.getRecord("0:0"));

    // t.display();
}