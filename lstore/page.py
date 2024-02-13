class Page:
    def __init__(self):
        self.num_records = 0
        self.rows = [0] * 512  # Initialize a list of 512 zeros

    def get_num_record(self):
        return self.num_records

    def add_record(self, data):
        if self.num_records < 512:
            self.rows[self.num_records] = data
            self.num_records += 1
        else:
            print("Error: Page is full.")

    def __getitem__(self, r):
        if r < self.num_records:
            return self.rows[r]
        else:
            print("Error: Index out of range.")
            return None

    def __setitem__(self, r, value):
        if r < 512:
            self.rows[r] = value
            if r >= self.num_records:
                self.num_records = r + 1
        else:
            print("Error: Index out of range.")

    def __del__(self):
        # Python has automatic garbage collection,
        # so explicit cleanup is not necessary.
        pass
