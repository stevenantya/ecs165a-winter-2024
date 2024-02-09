#include "Page.h"

class page;

Page::Page() {}
Page::~Page() {}

int Page::get_num_record() {
    return num_records;
}

void Page::add_record(int64_t data) {
    rows[num_records++] = data;
}

int64_t& Page::operator[](int r) {
    return rows[r];
}