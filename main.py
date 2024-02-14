from time import process_time
from lstore.table import Table
from lstore.page import Page

NULL_VAL = 2**64 - 1

t = Table("Test", 0, 5)

insert_time_0 = process_time()
data = [1, 2, 3, 4, 5]
for _ in range(10000):
    t.add_record(data)
insert_time_1 = process_time()

print("Inserting 10k records took:  \t\t\t", insert_time_1 - insert_time_0)

# Assuming NULL_VAL is defined elsewhere
udata1 = [1, NULL_VAL, NULL_VAL, 1, 1]
udata2 = [1, NULL_VAL, 2, 2, 2]
t.update_record(0, udata1)
t.update_record(0, udata2)
t.update_record(1, udata2)

t.delete_record(3)

t.display()

print("\n\n")

arg = [1, 1, 1, 1, 1]
result = t.get_record(0, arg, 0)
print("Row 0 version 0: ", " ".join(str(i) for i in result))

result = t.get_record(0, arg, -1)
print("Row 0 version -1: ", " ".join(str(i) for i in result))

result = t.get_record(0, arg, -2)
print("Row 0 version -2: ", " ".join(str(i) for i in result))

print("END")
