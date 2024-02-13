class Page:
    def __init__(self):
        self.num_records = 0
        self.rows = {}

    def get_num_record(self):
        return self.num_records

    def add_record(self, data):
        self.rows[self.num_records] = data
        self.num_records += 1

    def __getitem__(self, r):
        return self.rows[r]

    def __setitem__(self, r, value):
        self.rows[r] = value
