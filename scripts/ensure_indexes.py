import csv
import sys
import json

import re

schema = json.loads(open(sys.argv[2]).read())
cast_fns = {
    'boolean': lambda x: True if x.upper() == "Y" else False,
    "float": lambda x: float(x) if x else None,
    "int": lambda x: int(x) if x and re.match("^-?\d+$", x) else None,
    "string": lambda x: x,
    }

fp = open(sys.argv[1])
data = csv.reader(fp)#, dialect="excel-tab")

header = data.next()

cols_vals = (zip(header, row) for row in data)
cols_vals_typed = (
    [ (col, cast_fns[schema.get(col, 'string')](val))
      for col, val in col_val]
    for col_val in cols_vals)

import pymongo
conn = pymongo.Connection()
db = conn[sys.argv[3]]
collection = db.frisks

for col in schema:
    if schema[col] == "point":
        collection.ensure_index([(col, pymongo.GEO2D)])
    else:
        collection.ensure_index(col)
