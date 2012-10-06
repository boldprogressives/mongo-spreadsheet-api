from webob import Request, Response
from webob import exc
import json
from bson import json_util

import pymongo
conn = pymongo.Connection()
db = conn["sf_full"]
collection = db.frisks

from lib import cast_input, operators, available_operators, get_type, NotAnOperator, parse_operator, schema, operator_examples

NUM_PER_PAGE = 1000

import textwrap
import tempita

class APIV1(object):

    column_template = textwrap.dedent("""
    <html>
      <body>
        <h1>Options for browsing/searching column {{ column }} ({{ type }})</h1>
        <form method="GET">
          Search for exact value: <input id="exact_value" type="text" />
          <input type="submit" onclick="var val=exact_value.value; window.location.pathname+=val+'/'; return false;" />
        </form>
        {{for operator in operators}}
        <form method="GET">
          {{operator}}: <input id="{{operator}}" type="text" />
          <input type="submit" onclick="var val={{operator}}.value; val='{{operator}}='+val; window.location.pathname+=val+'/'; return false;" /> (e.g. {{examples[operator]}})
        </form>
        {{endfor}}
      </body>
    </html>
    """)

    results_template = textwrap.dedent("""
    <html>
      <body>
        <h1><span class="count">{{ records }}</span> total records match your query</h1>
        <div>
          {{if prevpage is not None}}<a href="{{ prevpage }}">Previous Page</a>{{endif}}
          {{if nextpage is not None}}<a href="{{ nextpage }}">Next Page</a>{{endif}}
        </div>
        <table class="results">
          <thead>
            <tr>
              {{for column in columns}}
                {{if column in allowed_columns}}
                <th><a href="{{add_path(column)}}">{{column}}</a></th>
                {{else}}
                <th>{{ column }}</th>
                {{endif}}
              {{endfor}}
            </tr>
          </thead>
          <tbody>
            {{for result in results}}
            <tr>
              {{for column in columns}}
              <td class="{{ column }}">
                {{if column in allowed_columns}}
                <a href="{{add_path(column, result[column])}}">{{ result[column] }}</a>
                {{else}}
                {{result[column]}}
                {{endif}}
              </td>
              {{endfor}}
            </tr>
            {{endfor}}
          </tbody>
        </table>
      </body>
    </html>
    """)

    def __call__(self, environ, start_response):
        resp = self.dispatch_request(environ)
        if isinstance(resp, Response):
            return resp(environ, start_response)
        req = Request(environ)
        if 'html' in req.GET:
            return Response(self.build_html(req, resp), content_type="text/html")(
                environ, start_response)

        resp = json.dumps(resp, default=json_util.default)
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

    

    def build_html(self, req, json):

        def add_path(*path):
            path_info = req.path_info
            path_info += '/'.join(str(i) for i in path) + '/'
            return req.script_name + path_info + '?' + req.query_string

        if 'results' in json:
            ctx = {}
            ctx['add_path'] = add_path
            ctx['records'] = json['records']
            ctx['allowed_columns'] = json['columns']
            ctx['columns'] = json['results'][0].keys() if len(json['results']) else []
            ctx['results'] = json['results']
            _req = req.copy()
            _req.GET['page'] = str(int(_req.GET['page']) + 1 if 'page' in _req.GET else 1)
            qs = "?" + '&'.join('='.join(x) for x in _req.GET.items())
            ctx['nextpage'] = _req.path_url + qs
            page = req.GET.get('page', '0')
            _req = req.copy()
            _req.GET['page'] = str(int(_req.GET['page']) - 1 if 'page' in _req.GET else -1)
            if int(_req.GET['page']) > -1 and _req.GET['page'] != page:
                qs = "?" + '&'.join('='.join(x) for x in _req.GET.items())
                ctx['prevpage'] = _req.path_url + qs
            else:
                ctx['prevpage'] = None
            return tempita.Template(self.results_template).substitute(**ctx)
        else:
            ctx = {}
            ctx['column'] = json['name']
            ctx['type'] = json['type']
            ctx['operators'] = json['operators']
            ctx['examples'] = operator_examples
            return tempita.Template(self.column_template).substitute(**ctx)            

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
            else:
                columns = schema.keys()
                columns = [col for col in columns 
                           if col not in filter_params
                           or not isinstance(filter_params[col], dict)
                           or filter_params[col].keys() == ['$in']]
                resp['columns'] = columns
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
            resp = {}
            resp['type'] = get_type(column)
            resp['name'] = column
            if column not in filter_params:
                resp['operators'] = available_operators(column)
            else:
                resp['operators'] = []
            return resp

        try:
            operator, params = parse_operator(column, col_val)
        except NotAnOperator:
            operator, params = None, None
        except (TypeError, ValueError), e:
            return exc.HTTPBadRequest(str(e))
        if operator is not None and column in filter_params:
            return exc.HTTPBadRequest("Column %s is already in use with an exact-value match; you cannot use both an operator and an exact-value match on the same column in a single query" % column)
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
                if filter_params[column].keys() != ["$in"]:
                    return exc.HTTPBadRequest("Column %s is already in use with an operator; you cannot use both an operator and an exact-value match on the same column in a single query" % column)
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
