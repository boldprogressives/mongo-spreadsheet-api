from bson import SON
from dateutil.parser import parse as parse_date
import json
import datetime

def _parse_date(x):
    if x.isdigit() and len(x) == len("1136908800000"):
        x = int(x)
        x = x / 1000
        return datetime.datetime.fromtimestamp(x)
    return parse_date(x)

schema_path = "/home/chris/api/data/schema.json"
schema = json.loads(open(schema_path).read())
cast_fns = {
    'boolean': lambda x: True if x.upper() in ["Y", "YES", "T", "TRUE", "1"] else False,
    "float": lambda x: float(x) if x else None,
    "int": lambda x: int(x) if x else None,
    "string": lambda x: x,
    "datetime": lambda x: _parse_date(x),
    }

class NotAnOperator(Exception):
    pass

def parse_operator(column, col_val):
    if not col_val.startswith("$"):
        raise NotAnOperator
    if not col_val.count('=') in [0,1]:
        raise TypeError("Malformed operator %s" % col_val)
    try:
        operator, params = col_val.split("=")
    except ValueError:
        operator, params = col_val, None
    if operator not in available_operators(column):
        raise TypeError("Operator %s not allowed for field %s" % (operator, column))
    return operators[operator], params

def default_operator(column, col_val):
    pass

def cast_input(col, val):
    return cast_fns[schema.get(col, 'string')](val)

def available_operators(col):
    return operators_by_type[schema.get(col, 'string')]

def get_type(col):
    return schema.get(col, 'string')

def lt(col, params):
    if params is None  or ':' in params:
        raise TypeError("$lt requires one argument.  Use like /%s/$lt=5/ to return all records where %s is less than 5." % (col, col))
    input = cast_input(col, params)
    return {"$lt": input}

def gt(col, params):
    if params is None or ':' in params:
        raise TypeError("$gt requires one argument.  Use like /%s/$gt=5/ to return all records where %s is greater than 5." % (col, col))
    input = cast_input(col, params)
    return {"$gt": input}

def between(col, params):
    if params is None or params.count(':') != 1:
        raise TypeError("$between requires two arguments.  Use like /%s/$between=5:10/ to return all records where %s is between 5 and 10 (inclusive)" % (col, col))
    params = params.split(":")
    params = [cast_input(col, param) for param in params]
    return { "$gte": params[0], "$lte": params[1] }

def notnull(col, params):
    if params is not None:
        raise TypeError("$notnull requires zero arguments.  Use like /%s/$notnull/ to return all records with any value for %s" % (col, col))
    return {"$exists": True, "$ne": None}

def near(col, params):
    if params is None or params.count(':') != 2:
        raise TypeError("$near requires three arguments.  Use like /%s/$near=-73.10:42.18:0.5/ to return all records within a 0.5-mile radius of %s" % (col, col))
    params = params.split(":")
    params[0] = float(params[0])
    params[1] = float(params[1])
    params[2] = float(params[2]) / 69.0
    near_dict = {"$near": [params[0], params[1]]}
    dist_dict = {"$maxDistance": params[2]}
    q = SON(near_dict)
    q.update(dist_dict)
    return q

operators = {
    "$lt": lt,
    "$gt": gt,
    "$between": between,
    "$notnull": notnull,
    "$near": near,
    }

operators_by_type = {
    "boolean": ["$notnull"],
    "float": ["$notnull", "$gt", "$lt", "$between"],
    "int": ["$notnull", "$gt", "$lt", "$between"],
    "string": ["$notnull"],
    "datetime": ["$notnull", "$gt", "$lt", "$between"],
    "point": ["$near"],
    }
