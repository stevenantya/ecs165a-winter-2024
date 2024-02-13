#include "Table.h"
const int64_t NULL_VAL = std::numeric_limits<uint64_t>::max();

extern "C" {
    void testfunc() {
    printf("Hello World\n");
}
}

int main() {
    Table t = Table("Test", 0, 5);

    int64_t data[] = {1, 2, 3, 4, 5};
    t.add_record(data);
    t.add_record(data);
    t.add_record(data);
    t.add_record(data);

    // printf("%s", t.getRecord(0b0000000000000000));

    int64_t udata1[] = {1, NULL_VAL, NULL_VAL, 1, 1};
    int64_t udata2[] = {1, NULL_VAL, 2, 2, 2};
    t.update_record(0, udata1);
    t.update_record(0, udata2);
    t.update_record(1, udata2);

    t.delete_record(3);

    t.display();

    std::cout << std::endl << std::endl;

    std::vector<int> arg = {1, 1, 1, 1, 1};
    auto result = t.get_record(0, arg, 0);

    std::cout << "Row 0 version 0: ";
    for (auto i : result) {
        std::cout << i << " ";
    }
    std::cout << std::endl;

    result = t.get_record(0, arg, -1);
    std::cout << "Row 0 version -1: ";
    for (auto i : result) {
        std::cout << i << " ";
    }
    std::cout << std::endl;

    result = t.get_record(0, arg, -2);
    std::cout << "Row 0 version -2: ";
    for (auto i : result) {
        std::cout << i << " ";
    }
    std::cout << std::endl;

    std::cout << "END";
}