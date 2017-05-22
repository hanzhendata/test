#!flask/bin/python

"""Alternative version of the ToDo RESTful server implemented using the
Flask-RESTful extension."""
#from __future__ import absolute_import
from flask import Flask, jsonify, abort, make_response
from flask.ext.restful import Api, Resource, reqparse, fields, marshal
from flask.ext.httpauth import HTTPBasicAuth
import mysql.connector

from flask import current_app, json, request


def __pad(strdata):
    '''Pads `strdata` with a Request's callback argument, if specified, or does
    nothing.'''

    if request.args.get('callback'):
        return "%s(%s);" % (request.args.get('callback'), strdata)
    else:
        return strdata


def __mimetype():
    if request.args.get('callback'):
        return 'application/javascript'
    else:
        return 'application/json'


def __dumps(*args, **kwargs):
    """ Serializes `args` and `kwargs` as JSON. Supports serializing an array
    as the top-level object, if it is the only argument.
    """
    indent = None
    if (current_app.config.get('JSONIFY_PRETTYPRINT_REGULAR', False)
            and not request.is_xhr):
        indent = 2
    return json.dumps(args[0] if len(args) is 1 else dict(*args, **kwargs),
                      indent=indent)


def jsonpify(*args, **kwargs):
    """
    Creates a :class:`~flask.Response` with the JSON or JSON-P
    representation of the given arguments with an `application/json`
    or `application/javascript` mimetype, respectively.  The arguments
    to this function are the same as to the :class:`dict` constructor,
    but also accept an array. If a `callback` is specified in the
    request arguments, the response is JSON-Padded.

    Example usage::

        @app.route('/_get_current_user')
        def get_current_user():
            return jsonify(username=g.user.username,
                           email=g.user.email,
                           id=g.user.id)


    GET /_get_current_user:
    This will send a JSON response like this to the browser::

        {
            "username": "admin",
            "email": "admin@localhost",
            "id": 42
        }

    or, if a callback is specified,

    GET /_get_current_user?callback=displayUsers

    Will result in a JSON response like this to the browser::
        displayUsers({
            "username": "admin",
            "email": "admin@localhost",
            "id": 42
        });

    This requires Python 2.6 or an installed version of simplejson.  For
    security reasons only objects are supported toplevel.  For more
    information about this, have a look at :ref:`json-security`.

    .. versionadded:: 0.2

    """
    return current_app.response_class(__pad(__dumps(*args, **kwargs)),
                                      mimetype=__mimetype())


jsonify = jsonpify  # allow override of Flask's jsonify.

app = Flask(__name__, static_url_path="")
api = Api(app)
auth = HTTPBasicAuth()


@auth.get_password
def get_password(username):
    if username == 'test':
        return 'test'
    return None


@auth.error_handler
def unauthorized():
    # return 403 instead of 401 to prevent browsers from displaying the default
    # auth dialog
    return make_response(jsonify({'message': 'Unauthorized access'}), 403)

tasks = [
    {
        'id': 1,
        'title': u'Buy groceries',
        'description': u'Milk, Cheese, Pizza, Fruit, Tylenol',
        'done': False
    },
    {
        'id': 2,
        'title': u'Learn Python',
        'description': u'Need to find a good Python tutorial on the web',
        'done': False
    }
]

task_fields = {
    'title': fields.String,
    'description': fields.String,
    'done': fields.Boolean,
    'uri': fields.Url('task')
}

stock_fields = {
    'windcode': fields.String,
    'name' : fields.String,
    'type' : fields.String,
}
class TaskListAPI(Resource):
    decorators = [auth.login_required]

    def __init__(self):
        self.reqparse = reqparse.RequestParser()
        self.reqparse.add_argument('title', type=str, required=True,
                                   help='No task title provided',
                                   location='json')
        self.reqparse.add_argument('description', type=str, default="",
                                   location='json')
        super(TaskListAPI, self).__init__()

    def get(self):
        return {'tasks': [marshal(task, task_fields) for task in tasks]}

    def post(self):
        args = self.reqparse.parse_args()
        task = {
            'id': tasks[-1]['id'] + 1,
            'title': args['title'],
            'description': args['description'],
            'done': False
        }
        tasks.append(task)
        return {'task': marshal(task, task_fields)}, 201


class TaskAPI(Resource):
    decorators = [auth.login_required]

    def __init__(self):
        self.reqparse = reqparse.RequestParser()
        self.reqparse.add_argument('title', type=str, location='json')
        self.reqparse.add_argument('description', type=str, location='json')
        self.reqparse.add_argument('done', type=bool, location='json')
        super(TaskAPI, self).__init__()

    def get(self, id):
        task = [task for task in tasks if task['id'] == id]
        if len(task) == 0:
            abort(404)
        return {'task': marshal(task[0], task_fields)}

    def put(self, id):
        task = [task for task in tasks if task['id'] == id]
        if len(task) == 0:
            abort(404)
        task = task[0]
        args = self.reqparse.parse_args()
        for k, v in args.items():
            if v is not None:
                task[k] = v
        return {'task': marshal(task, task_fields)}

    def delete(self, id):
        task = [task for task in tasks if task['id'] == id]
        if len(task) == 0:
            abort(404)
        tasks.remove(task[0])
        return {'result': True}

class StockListAPI(Resource):
    decorators = [auth.login_required]

    def __init__(self):
        self.reqparse = reqparse.RequestParser()
        self.reqparse.add_argument('title', type=str, required=True,
                                   help='No task title provided',
                                   location='json')
        self.reqparse.add_argument('description', type=str, default="",
                                   location='json')
        super(StockListAPI, self).__init__()



    def get(self):        
        return {'stocks': [marshal(task, stock_fields) for task in stocks]}

    # def post(self):
    #     args = self.reqparse.parse_args()
    #     task = {
    #         'id': tasks[-1]['id'] + 1,
    #         'title': args['title'],
    #         'description': args['description'],
    #         'done': False
    #     }
    #     tasks.append(task)
    #     return {'task': marshal(task, task_fields)}, 201

class StockAPI(Resource):
    decorators = [auth.login_required]

    def __init__(self):
        # self.reqparse = reqparse.RequestParser()
        # self.reqparse.add_argument('title', type=str, location='json')
        # self.reqparse.add_argument('description', type=str, location='json')
        # self.reqparse.add_argument('done', type=bool, location='json')
        super(StockAPI, self).__init__()

    def get(self, code):
        code = code.strip().upper()
        task = [task for task in stocks if task['windcode'] == code]
        if len(task) == 0:
            abort(404)
        return {'stock': marshal(task[0], stock_fields)}

    # def put(self, id):
    #     task = [task for task in tasks if task['id'] == id]
    #     if len(task) == 0:
    #         abort(404)
    #     task = task[0]
    #     args = self.reqparse.parse_args()
    #     for k, v in args.items():
    #         if v is not None:
    #             task[k] = v
    #     return {'task': marshal(task, task_fields)}

stockboard_fields = {
    'windcode': fields.String,
    'name' : fields.String,
    'type' : fields.String,
    'update_date' : fields.DateTime,
}

class StockBoardListAPI(Resource):
    decorators = [auth.login_required]

    def __init__(self):
        # self.reqparse = reqparse.RequestParser()
        # self.reqparse.add_argument('type', type=str, required=True,
        #                            help='No Stock board type provided',
        #                            location='json')
        # self.reqparse.add_argument('description', type=str, default="",
        #                            location='json')
        super(StockBoardListAPI, self).__init__()



    def get(self):
        return {'stocksBoard': [marshal(task, stockboard_fields) for task in stocksBoard]}

    # def post(self):
    #     args = self.reqparse.parse_args()
    #     task = {
    #         'id': tasks[-1]['id'] + 1,
    #         'title': args['title'],
    #         'description': args['description'],
    #         'done': False
    #     }
    #     tasks.append(task)
    #     return {'task': marshal(task, task_fields)}, 201

class StockBoardAPI(Resource):
    decorators = [auth.login_required]

    def __init__(self):
        # self.reqparse = reqparse.RequestParser()
        # self.reqparse.add_argument('title', type=str, location='json')
        # self.reqparse.add_argument('description', type=str, location='json')
        # self.reqparse.add_argument('done', type=bool, location='json')
        super(StockBoardAPI, self).__init__()

    def get(self, code):
        code = code.strip().upper()
        task = [task for task in stocksBoard if task['windcode'] == code]
        if len(task) == 0:
            abort(404)
        return {'stockboard': marshal(task[0], stockboard_fields)}

    # def put(self, id):
    #     task = [task for task in tasks if task['id'] == id]
    #     if len(task) == 0:
    #         abort(404)
    #     task = task[0]
    #     args = self.reqparse.parse_args()
    #     for k, v in args.items():
    #         if v is not None:
    #             task[k] = v
    #     return {'task': marshal(task, task_fields)}

stock_select_fields = {
    'id': fields.Integer(default=-1),
    'windcode': fields.String,
    'name' : fields.String,
}

stockboard_select_fields = {
    'id': fields.Integer(default=-1),
    'windcode': fields.String,
    'name' : fields.String,
    'type' : fields.String,
    'update_date' : fields.DateTime(dt_format='iso8601'),
    'stockslist' : fields.List(fields.Nested(stock_select_fields)),
}

class  StockBoardStrongAPI(Resource):
    # decorators = [auth.login_required]

    def __init__(self):
        super(StockBoardStrongAPI, self).__init__()



    def get(self):
        return jsonify(data=[marshal(task, stockboard_select_fields) for task in stocksBoardStrong])
        # return {'data': [marshal(task, stockboard_select_fields) for task in stocksBoardStrong]} 

class  StockBoardReadyAPI(Resource):
    decorators = [auth.login_required]

    def __init__(self):
        super(StockBoardReadyAPI, self).__init__()



    def get(self):
        return {'stockboardstrong': [marshal(task, stockboard_select_fields) for task in stocksBoardReady]}

api.add_resource(TaskListAPI,   '/api/v1.0/tasks', endpoint='tasks')
api.add_resource(TaskAPI,       '/api/v1.0/tasks/<int:id>', endpoint='task')
api.add_resource(StockListAPI,  '/api/v1.0/stocks/', endpoint='stocks')
api.add_resource(StockAPI,       '/api/v1.0/stocks/<string:code>', endpoint='stock')
api.add_resource(StockBoardListAPI,  '/api/v1.0/stocksboard/', endpoint='stocksboard')
api.add_resource(StockBoardAPI,      '/api/v1.0/stocksboard/<string:code>', endpoint='stockboard')
api.add_resource(StockBoardStrongAPI, '/api/v1.0/stocksboardstrong/', endpoint='stockboardstrong')
api.add_resource(StockBoardReadyAPI,  '/api/v1.0/stocksboardReady/', endpoint='stockboardready')


stocks = []
stocksBoard = []
stocksBoardStrong = []
stocksBoardReady = []

def Init():
    cnx = mysql.connector.connect(**Config.config)
    if len(stocks)==0:
        sql = "select  finance_mic, prod_code, prod_name, hq_type_code from prodcode"
        cursor = cnx.cursor()
        
        cursor.execute(sql)
        for mic,prod_code,sec_name,hqtype in cursor:
            stocks.append({    
                'windcode': prod_code+'.'+mic,
                'name' : sec_name,
                'type' : hqtype,
                })

        cursor.close()
    if len(stocksBoard) == 0 :
        cursor = cnx.cursor()
        sql = "select name,windcode,type,update_date from stocks_board"
        cursor.execute(sql)
        for bname,windcode,btype,update_date in cursor:
            stocksBoard.append({    
                'windcode': windcode,
                'name' : bname,
                'type' : btype,
                'update_date' : update_date,
                })
        cursor.close()
    if len(stocksBoardStrong) == 0 :
        cursor = cnx.cursor()
        cursor_list = cnx.cursor()
        sql = "select id,sectorid,name,windcode,type,update_date from stocks_board_sr where isStrong=True order by id"
        cursor.execute(sql)
        sql_list = "select id,windcode,name from stocks_boardlist_sr where isStrong=True and sectorid=%(sectorid)s"
        for sid,sectorid,bname,windcode,btype,update_date in cursor:
            sbl = []
            cursor_list.execute(sql_list,{'sectorid':sectorid,})
            for lid,lwindcode,lname in cursor_list:
                sbl.append({
                    'id':lid,
                    'name':lname,
                    'windcode':lwindcode,
                    })

            sbs = {
                'id': sid ,    
                'windcode': windcode,
                'name' : bname,
                'type' : btype,
                'update_date' : update_date,
                'stockslist':sbl,
                }
            stocksBoardStrong.append(sbs)
        cursor_list.close()
        cursor.close()
    cnx.close()



if __name__ == '__main__':
    if __package__ is None:
        import sys
        from os import path
        sys.path.append( path.dirname( path.dirname( path.abspath(__file__) ) ) )
        from LieFeng import Config,Stock
    else:
        from ..LieFeng import Config,Stock
    Init()
    # print stocksBoardStrong,stocksBoardReady

    app.run(host='0.0.0.0',port=3307,debug=True, use_evalex=False)
