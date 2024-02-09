#include <cstdint>

class Page {
    private:
        int num_records = 0;
        int64_t rows[512];

    public:
        Page();
        ~Page();

        int get_num_record();

        void add_record(int64_t data);

        int64_t& operator[](int r);
};

