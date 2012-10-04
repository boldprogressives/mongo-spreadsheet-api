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
collection.drop()

for col in [ "race", "forceuse", "frisk", "pct",
             "datetime_dayofweek", "datetime_month", "datetime", "datetime_time"]:
    collection.ensure_index(col)
    collection.ensure_index([("loc", pymongo.GEO2D)])

for row in cols_vals_typed:
    obj = dict(row)
    x = obj.get('x')
    y = obj.get('y')
    if x is not None and y is not None:
        obj['loc'] = [x, y]
    else:
        obj['loc'] = None
    date = obj.get('datestop')
    time = obj.get('timestop')
    if date is not None and time is not None:
        if len(time) == 0:
            time = obj['timestop'] = "0000"
        if len(time) == 1:
            time = obj['timestop'] = "000%s" % time
        if len(time) == 2:
            time = obj['timestop'] = "00%s" % time
        if len(time) == 3:
            time = obj['timestop'] = "0%s" % time
        if len(date) == 7:
            date = obj['datestop'] = "0%s" % date
        datetimestop = "%s%s" % (date, time)

        dows = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
        import datetime
        
        try:
            datetimestop = datetime.datetime.strptime(datetimestop, "%m%d%Y%H%M%S")
        except ValueError:
            datetimestop = datetime.datetime.strptime(datetimestop, "%Y-%m-%d%H%M%S")
        obj['datetime_dayofweek'] = dows[datetimestop.weekday()]
        obj['datetime_month'] = datetimestop.month
        obj['datetime'] = datetimestop
        obj['datetime_time'] = int(datetimestop.strftime("%H%M"))

    else:
        obj['datetime_dayofweek'] = None
        obj['datetime_month'] = None
        obj['datetime'] = None
        obj['datetime_time'] = None

    collection.insert(obj)
