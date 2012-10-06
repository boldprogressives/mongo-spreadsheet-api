from webob import Request, Response
from webob import exc
import json
from bson import json_util

import pymongo
conn = pymongo.Connection()
db = conn["sf_full"]
collection = db.frisks

from lib import cast_input, operators, available_operators, get_type, NotAnOperator, parse_operator

NUM_PER_PAGE = 1000

class APIV1(object):
    def __call__(self, environ, start_response):
        resp = self.dispatch_request(environ)
        if isinstance(resp, Response):
            return resp(environ, start_response)
        resp = json.dumps(resp, default=json_util.default)
        req = Request(environ)
        if 'jsonp' in req.GET:
            callback = req.GET['jsonp']
        elif 'callback' in req.GET:
            callback = req.GET['callback']
        else:
            callback = None
        if callback is None:
            resp = Response(resp, content_type="application/json")
        else:
            resp = "%s(%s)" % (callback, resp)
            resp = Response(resp, content_type="text/javascript")
        return resp(environ, start_response)

    def dispatch_request(self, environ):
        req = Request(environ)

        filter_params = {}

        return self.browse(filter_params, {}, req)

    def browse(self, filter_params, args, req):
        column = req.path_info_pop()
        if not column:
            include_results = 'results' in req.GET
            if include_results:
                values = req.GET.getall("value")
                if values:
                    args["fields"] = values
            query = collection.find(filter_params, **args)
            query_count = collection.find(filter_params, **args)
            resp = {
                'records': query_count.count(),
                }
            if filter_params == {}:
                resp['columns'] = [index['key'].keys()[0] for index in db.system.indexes.find()]
            if include_results:
                page = req.GET.get("page", "1")
                try:
                    page = int(page)
                except:
                    page = 1
                page = page - 1
                if page < 0:
                    page = 0
                results_per_page = req.GET.get("results_per_page", NUM_PER_PAGE)
                try:
                    results_per_page = int(results_per_page)
                except:
                    results_per_page = NUM_PER_AGE
                query = query.skip(page * results_per_page).limit(results_per_page)
                resp['results'] = list(query)
            return resp

        col_val = req.path_info_pop()
        if not col_val:
            query = collection.find(filter_params)
            page = req.GET.get("page", "1")
            try:
                page = int(page)
            except:
                page = 1
            page = page - 1
            if page < 0:
                page = 0
            ## stupid mongodb bug means we can't just use .distinct with .limit
            ## https://jira.mongodb.org/browse/SERVER-2130
            values = query.distinct(column)
            values = values[page * NUM_PER_PAGE:page * NUM_PER_PAGE + NUM_PER_PAGE]
            resp = {'values': values}
            resp['type'] = get_type(column)
            resp['operators'] = available_operators(column)
            return resp

        try:
            operator, params = parse_operator(column, col_val)
        except NotAnOperator:
            operator, params = None, None
        except (TypeError, ValueError), e:
            return exc.HTTPBadRequest(str(e))
            
        if operator is None and params is None:
            return self.browse_no_operator(filter_params, args, req, column, col_val)

        try:
            filter_params[column] = operator(column, params)
        except (TypeError, ValueError), e:
            return exc.HTTPBadRequest(str(e))

        return self.browse(filter_params, args, req)

    def browse_no_operator(self, filter_params, args, req, column, col_val):
        col_val = cast_input(column, col_val)
        if column in filter_params:
            if isinstance(filter_params[column], dict):
                assert filter_params[column].keys() == ["$in"]
                filter_params[column]["$in"].append(col_val)
            else:
                filter_params[column] = {"$in": [filter_params[column], col_val]}
        else:
            filter_params[column] = col_val
        return self.browse(filter_params, args, req)
        
class API(object):
    def __init__(self):
        self.apis = {'v1': APIV1()}

    def __call__(self, environ, start_response):
        req = Request(environ)
        apiv = req.path_info_pop()
        if not apiv or apiv not in self.apis:
            return exc.HTTPBadRequest("You should prefix all request paths with /v1/")(environ, start_response)
        return self.apis[apiv](environ, start_response)

app = API()

if __name__ == '__main__':
    from paste.httpserver import serve
    serve(app, host="0.0.0.0", port="8000")
