from lstore.db import Database
from lstore.query import Query

from random import choice, randint, sample, seed

db = Database()
db.open("./ECS165")
# Create a table  with 5 columns
#   Student Id and 4 grades
#   The first argument is name of the table
#   The second argument is the number of columns
#   The third argument is determining the which columns will be primay key
#       Here the first column would be student id and primary key
grades_table = db.create_table('Grades', 5, 0)

# create a query class for the grades table
query = Query(grades_table)

query.insert(1, 2, 3, 4, 5)

query.update(1, *[0, 0, 0, 0, 0])

print(query.select(1, 0, [1, 1, 1, 1, 1])[0].columns)
print(query.select_version(1, 0, [1, 1, 1, 1, 1], -1)[0].columns)

db.close()