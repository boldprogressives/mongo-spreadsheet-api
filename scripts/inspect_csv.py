import csv
import sys

fp = open(sys.argv[1])
data = csv.reader(fp, dialect="excel-tab")

header = data.next()

values = {}

for row in data:
    for col, cell in zip(header, row):
        values.setdefault(col, set()).add(cell)

schema = {}

def looks_boolish(vals):
    return all(x.upper() in ("Y", "N") for x in vals)
def looks_floatish(vals):
    import re
    return all(re.match("^-?\d*\.?\d*$", x) and x != "." for x in vals)
def looks_intish(vals):
    import re
    return all(re.match("^-?\d+$", x) or x == "" for x in vals)

for col, vals in values.iteritems():
    if looks_boolish(vals):
        schema[col] = "boolean"
    elif looks_intish(vals):
        schema[col] = "int"
    elif looks_floatish(vals):
        schema[col] = "float"
    else:
        schema[col] = "string"

import json
print json.dumps(schema, indent=2)
