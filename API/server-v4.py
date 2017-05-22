#!flask/bin/python

"""Alternative version of the ToDo RESTful server implemented using the
Flask-RESTful extension."""

from flask import Flask, jsonify, abort, make_response
from flask.ext.restful import Api, Resource, reqparse, fields, marshal
from flask.ext.httpauth import HTTPBasicAuth
from flask import stream_with_context,  Response
import mysql.connector
from tornado.wsgi import WSGIContainer
from tornado.web import Application , FallbackHandler
from tornado.websocket import WebSocketHandler
from tornado.ioloop import IOLoop
from flask import current_app, json, request
import requests
# import time
from datetime import date, datetime, timedelta
# from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.schedulers.tornado import TornadoScheduler
import gc

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
page_num = 10 
# auth = HTTPBasicAuth()


# @auth.get_password
# def get_password(username):
#     if username == 'test':
#         return 'test'
#     return None


# @auth.error_handler
# def unauthorized():
#     # return 403 instead of 401 to prevent browsers from displaying the default
#     # auth dialog
#     return make_response(jsonify({'message': 'Unauthorized access'}), 403)

def PageContent(Content,offset):
    if offset<=0 or len(Content) < page_num:
        stocks = Content
    else :
        stocks = [task for task in Content if task['id'] >=(offset-1)*page_num and task['id']<offset*page_num]
    return stocks


stock_fields = {
    'windcode': fields.String,
    'name' : fields.String,
    'type' : fields.String,
    'status': fields.Integer,
    'is_cyb': fields.Integer,
    'is_zz500': fields.Integer,
    'is_hs300': fields.Integer,
}
class StockListAPI(Resource):
    # decorators = [auth.login_required]

    def __init__(self):
        self.reqparse = reqparse.RequestParser()
        self.reqparse.add_argument('title', type=str, required=True,
                                   help='No task title provided',
                                   location='json')
        self.reqparse.add_argument('description', type=str, default="",
                                   location='json')
        super(StockListAPI, self).__init__()



    def get(self):
        return jsonify(data=[marshal(task, stock_fields) for task in stocks])
        # return {'data': [marshal(task, stock_fields) for task in stocks]}

stock_basiclist_fields = {
    'windcode': fields.String,
    'name' : fields.String,
    'type' : fields.String,
    # 'status': fields.Integer,
    'pct' : fields.Float,
    'cap' : fields.Float,
}

class StockBasicListAPI(Resource):
    # decorators = [auth.login_required]

    def __init__(self):
        super(StockBasicListAPI, self).__init__()

    def get(self,offset):
        tasks = PageContent(stocksBasicSelect ,offset)
        return jsonify(data=[ marshal(task, stock_basiclist_fields) for task in tasks ])
        

class StockAPI(Resource):
    # decorators = [auth.login_required]

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
        return jsonify(data=marshal(task[0], stock_fields))
        # return {'data': marshal(task[0], stock_fields)}

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

stock_basic_fields= {
    'windcode': fields.String,
    'name' : fields.String,
    'boards' : fields.String,
    'intro' : fields.String ,
    'seg_sales': fields.String,
    'stars': fields.Integer,
    'ev'    : fields.Float,
    'debt_assets' : fields.Float, 
    'or'   :fields.Float,
    'pe'   :fields.Float,
    'or_lastyear'   :fields.Float,
    'wgsd'        : fields.List( fields.Float ),
    'quarter'        : fields.List( fields.String ),
    'gross_pro_mg': fields.List( fields.Float ),
    'oper_cash_ps': fields.List( fields.Float ),
    'oper_tr'     : fields.List( fields.Float ),
    'profit'      : fields.List( fields.Float ),
    'roe_basic'   : fields.List( fields.Float ),
    
}
class StockBasicAPI(Resource):
    # decorators = [auth.login_required]

    def __init__(self):
        super(StockBasicAPI, self).__init__()

    def get(self, code):
        code = code.strip().upper()
        task = [task for task in stocksBasic if task['windcode'] == code]
        if len(task) == 0:
            abort(404)
        return jsonify(data=marshal(task[0], stock_basic_fields))
        # return {'data': marshal(task[0], stock_basic_fields)}


stockboard_fields = {
    'windcode': fields.String,
    'name' : fields.String,
    'type' : fields.String,
    'update_date' : fields.DateTime,
}

class StockBoardListAPI(Resource):
    # decorators = [auth.login_required]

    def __init__(self):
        # self.reqparse = reqparse.RequestParser()
        # self.reqparse.add_argument('type', type=str, required=True,
        #                            help='No Stock board type provided',
        #                            location='json')
        # self.reqparse.add_argument('description', type=str, default="",
        #                            location='json')
        super(StockBoardListAPI, self).__init__()



    def get(self):
        return jsonify(data=[marshal(task, stockboard_fields) for task in stocksBoard])
        # return {'data': [marshal(task, stockboard_fields) for task in stocksBoard]}

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
    # decorators = [auth.login_required]

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
        return jsonify(data=marshal(task[0], stockboard_fields))# 
        # return {'data': marshal(task[0], stockboard_fields)}


stock_select_fields = {
    'id': fields.Integer(default=-1),
    'windcode': fields.String,
    'name' : fields.String,
    'pct' : fields.Float,
    'cap' : fields.Float,
}

stockboard_select_fields = {
    'id': fields.Integer(default=-1),
    'windcode': fields.String,
    'name' : fields.String,
    'type' : fields.String,
    'update_date' : fields.DateTime(dt_format='iso8601'),
    'pct' : fields.Float,
    'cap' : fields.Float,
    'stockslist' : fields.List(fields.Nested(stock_select_fields)),
}



class  StockBoardStrongAPI(Resource):
    # decorators = [auth.login_required]

    def __init__(self):
        super(StockBoardStrongAPI, self).__init__()

    def get(self,offset):        
        tasks = PageContent(stocksBoardStrong,offset)
        return jsonify(data=[marshal(task, stockboard_select_fields) for task in tasks])
        # return jsonify(data=[marshal(task, stockboard_select_fields) for task in stocksBoardStrong])
        # return {'data': [marshal(task, stockboard_select_fields) for task in stocksBoardStrong]} 

class  StockBoardReadyAPI(Resource):
    # decorators = [auth.login_required]

    def __init__(self):
        super(StockBoardReadyAPI, self).__init__()



    def get(self,offset):
        tasks = PageContent(stocksBoardReady,offset)
        return jsonify(data=[marshal(task, stockboard_select_fields) for task in tasks])
         # return jsonify(data=[marshal(task, stockboard_select_fields) for task in stocksBoardReady])
        # return {'data': [marshal(task, stockboard_select_fields) for task in stocksBoardReady]}

class  StockStrongAPI(Resource):
    # decorators = [auth.login_required]

    def __init__(self):
        super(StockStrongAPI, self).__init__()

    def get(self,offset):
        tasks = PageContent(stocksStrong,offset)
        return jsonify(data=[marshal(task, stock_select_fields) for task in tasks])
        # return jsonify(data=[marshal(task, stock_select_fields) for task in stocksStrong])
        # return {'data': [marshal(task, stock_select_fields) for task in stocksStrong]}

class  StockReadyAPI(Resource):
    # decorators = [auth.login_required]

    def __init__(self):
        super(StockReadyAPI, self).__init__()

    def get(self,offset):
        tasks = PageContent(stocksReady ,offset)
        return jsonify(data=[marshal(task, stock_select_fields) for task in tasks])
        # return {'data': [marshal(task, stock_select_fields) for task in stocksReady]}

class  StockBreakCentralAPI(Resource):
    # decorators = [auth.login_required]

    def __init__(self):
        super(StockBreakCentralAPI, self).__init__()

    def get(self,offset):
        tasks = PageContent(stocksBreakCentral ,offset)
        return jsonify(data=[marshal(task, stock_select_fields) for task in tasks])
        # return {'data': [marshal(task, stock_select_fields) for task in stocksReady]}

class  StockBreakVolumeAPI(Resource):
    # decorators = [auth.login_required]

    def __init__(self):
        super(StockBreakVolumeAPI, self).__init__()

    def get(self,offset):
        tasks = PageContent(stocksBreakVolume ,offset)
        return jsonify(data=[marshal(task, stock_select_fields) for task in tasks])
        # return {'data': [marshal(task, stock_select_fields) for task in stocksReady]}

stock_label_fields = {    
    'windcode': fields.String,
    'is_strong': fields.Integer(default=0),
    'is_ready': fields.Integer(default=0),
    'is_bc': fields.Integer(default=0),
    'is_bv': fields.Integer(default=0),
    'is_jishu': fields.Integer(default=0),
    'is_jiben': fields.Integer(default=0),
    'is_mf': fields.Integer(default=0),
    'is_bs': fields.Integer(default=0),
    'is_br': fields.Integer(default=0),
    'stars': fields.Integer(default=0),
}

class  StockLabelAPI(Resource):
    # decorators = [auth.login_required]
    def __init__(self):
        super(StockLabelAPI, self).__init__()

    def get(self):
        codes = (request.args.getlist('windcode'))
        
        tasks = []
        for code in codes:
            code = code.strip().upper()
            # print code
            if stocksLabel.get(code) is None: 
                task = []
            else: 
                task = stocksLabel[code]
            if len(task)==0 :
                continue
            tasks.append(task)
        return jsonify(data=[marshal(task, stock_label_fields) for task in tasks])
        # return {'data': [marshal(task, stock_select_fields) for task in stocksReady]}

stock_index_fields = {
    'windcode': fields.String,
    'type': fields.String,
    'num' : fields.Integer(default=0),
    'data': fields.List( fields.Float ),
    'update_date': fields.List( fields.String ),
}

class  StockIndexAPI(Resource):
    # decorators = [auth.login_required]
    def __init__(self):
        super(StockIndexAPI, self).__init__()

    def get(self):
        
        parser = reqparse.RequestParser()
        parser.add_argument('num', type=int,default=30)
        parser.add_argument('type', type=str,default='', help='Index type!')
        parser.add_argument('windcode', type=str,default='', help='Must set code!')
        args = parser.parse_args()
       
        limit = args['num']
        index_type = args['type']

        
        if limit > 30 or limit<1:
            limit = 30
        task = {}
        if len(index_type) >0 :
            index_type= index_type.strip().upper()

        if index_type=='FUND' or index_type=='STRONG' or index_type=='READY':
            task['windcode'] = index_type.title() +'Index'
            task['type'] = 'Index'
            task['num' ] = limit
            task['data'] = [ x['data'] for x in stocksIndex[ task['windcode'] ] ]  [limit*-1:]
            task['update_date'] = [x['update_date'] for x in stocksIndex[ task['windcode'] ] ]  [limit*-1:]
        else :
            task['windcode'] = args['windcode'].strip().upper()
            
            if task['windcode'] in stocksClose:
                task['type'] = 'close'
                task['num'] = limit                
                task['data'] = [ x['data'] for x in stocksClose[ task['windcode'] ] ]  [limit*-1:]
                task['update_date'] = [ x['update_date'] for x in stocksClose[ task['windcode'] ] ]  [limit*-1:]
            else:
                task['type'] = 'not existed!'
                task['num'] = 0
                task['data'] = []
                task['update_date'] = []
        return jsonify(data=(marshal(task, stock_index_fields) ) )
        # return {'data': [marshal(task, stock_select_fields) for task in stocksReady]}

stock_market_fields = {
    'windcode': fields.String,    
    'num' : fields.Integer(default=0),
    'open': fields.List( fields.Float ),
    'high': fields.List( fields.Float ),
    'low' : fields.List( fields.Float ),
    'close': fields.List( fields.Float ),
    'update_date': fields.List( fields.String ),
}

class  StockOHLCAPI(Resource):
    # decorators = [auth.login_required]
    def __init__(self):
        super(StockOHLCAPI, self).__init__()

    def get(self):
        parser = reqparse.RequestParser()
        parser.add_argument('num', type=int,default=30)
        parser.add_argument('windcode', type=str, required=True, help='Must set code!')
        args = parser.parse_args()
        windcode = args['windcode'].strip().upper()
        limit = args['num']
        
        if limit > 30 or limit<1:
            limit = 30
        task = {}        
        if windcode in stocksOHLC:
            task['windcode'] = windcode
            task['num'] = limit
            task['open']= stocksOHLC[windcode]['open'][-limit:]
            task['high']= stocksOHLC[windcode]['high'][-limit:]
            task['low']= stocksOHLC[windcode]['low'][-limit:]
            task['close']= stocksOHLC[windcode]['close'][-limit:]
            task['update_date']= stocksOHLC[windcode]['update_date'][-limit:]
            
       
        return jsonify(data=(marshal(task, stock_market_fields) ) )        

stock_mainforce_fields = {
    'windcode' : fields.String,
    'name' : fields.String,
    'date' : fields.DateTime(dt_format='iso8601'),
    'type' : fields.String,
    'ratio': fields.Float,
    'cost' : fields.Float,
    'target': fields.Float,    
    'pct' : fields.Float,
    'cap' : fields.Float,
}

class  StockMainForceAPI(Resource):
    # decorators = [auth.login_required]
    def __init__(self):
        super(StockMainForceAPI, self).__init__()

    def get(self,offset):
        
        index_type =  request.args.get('type','').strip().upper()
        
        
        if index_type=='INC' or index_type=='NEW' or index_type=='EQUAL':
            tasks = [ task for task in stocksMainForce if task['type'].upper()==index_type ]             
        else :
            tasks = stocksMainForce
        tasks = PageContent(stocksMainForce ,offset)
        return jsonify( data=[marshal(task, stock_mainforce_fields) for task in tasks] )
        # return {'data': [marshal(task, stock_select_fields) for task in stocksReady]}

stock_days_fields = {
    'status': fields.String,
    'valid' : fields.Integer(default=0),
}

class  StockDaysAPI(Resource):
    # decorators = [auth.login_required]
    def __init__(self):
        super(StockDaysAPI, self).__init__()

    def get(self):
                
        current_date =  request.args.get('date','').strip().upper()
        if current_date=='':
            current_date = datetime.now()
        task = {'status':'OK',}
        wsdata=WindPy.tdayscount(current_date, current_date, "")
        if  wsdata.ErrorCode !=0:      
            task['valid'] = 0
        else:
            if wsdata.Data[0][0] == 0 : # trade day 1
                task['valid'] = 0
            else:
                task['valid'] = 1
        
        return jsonify(data=(marshal(task, stock_days_fields) ) )
        
class  StockCalcDateAPI(Resource):
    # decorators = [auth.login_required]
    def __init__(self):
        super(StockCalcDateAPI, self).__init__()

    def get(self):
                
        return jsonify(data=(marshal({'date':stocksCalcDate.strftime("%Y-%m-%d %H:%M:%S")}, {'date': fields.String}) ) )

stock_warningmessage_fields = {
    # 'id': fields.Integer(default=-1),
    'windcode': fields.String,
    'name' : fields.String,
    'warning_status': fields.Integer,
    'type' : fields.String,
    'update_date' : fields.DateTime(dt_format='iso8601'),
    # 'description' : fields.Integer,
    'price' : fields.Float,
}
stock_wm_str_fields = {    
    'windcode': fields.String,
    'name' : fields.String,
    'warning_status': fields.Integer,
    'type' : fields.Integer,
    'update_date' : fields.String,
    # 'description' : fields.String,
    'price' : fields.String,
    # "is_tjb": fields.Integer,
    # "is_qsb": fields.Integer,
    # "is_xsb": fields.Integer,
    'is_strong': fields.Integer(default=0),
    'is_ready': fields.Integer(default=0),
    'is_bc': fields.Integer(default=0),
    'is_bv': fields.Integer(default=0),
    'is_jishu': fields.Integer(default=0),
    'is_jiben': fields.Integer(default=0),
    'is_mf': fields.Integer(default=0),
    'is_bs': fields.Integer(default=0),
    'is_br': fields.Integer(default=0),
    'stars': fields.Integer(default=0),
}

api.add_resource(StockListAPI,   '/api/v1.0/stocks/', endpoint='stocks')
api.add_resource(StockAPI,        '/api/v1.0/stocks/<string:code>', endpoint='stock')
api.add_resource(StockBoardListAPI,  '/api/v1.0/stocksboard/', endpoint='stocksboard')
api.add_resource(StockBoardAPI,      '/api/v1.0/stocksboard/<string:code>', endpoint='stockboard')
api.add_resource(StockBoardStrongAPI, '/api/v1.0/stocksboardstrong/<int:offset>', endpoint='stockboardstrong')
api.add_resource(StockBoardReadyAPI,  '/api/v1.0/stocksboardready/<int:offset>', endpoint='stockboardready')
api.add_resource(StockStrongAPI, '/api/v1.0/stocksstrong/<int:offset>', endpoint='stockstrong')
api.add_resource(StockReadyAPI,   '/api/v1.0/stocksready/<int:offset>', endpoint='stockready')
api.add_resource(StockBreakCentralAPI,'/api/v1.0/stocksbreakcentral/<int:offset>', endpoint='stockbreakcentral')
api.add_resource(StockBreakVolumeAPI, '/api/v1.0/stocksbreakvolume/<int:offset>', endpoint='stockbreakvolume')
api.add_resource(StockBasicAPI,  '/api/v1.0/stocksbasic/<string:code>', endpoint='stockbasic')
api.add_resource(StockLabelAPI,  '/api/v1.0/stockslabel/', endpoint='stocklabel')
api.add_resource(StockBasicListAPI,  '/api/v1.0/stocksbasiclist/<int:offset>', endpoint='stockbasiclist')
api.add_resource(StockIndexAPI,  '/api/v1.0/stocksindex/', endpoint='stockindex')
api.add_resource(StockOHLCAPI,  '/api/v1.0/stocksmarket/', endpoint='stockohlc')
api.add_resource(StockMainForceAPI,  '/api/v1.0/stocksmainforce/<int:offset>', endpoint='stockmainforce')
api.add_resource(StockDaysAPI,  '/api/v1.0/dayvalid/', endpoint='stockdays')
api.add_resource(StockCalcDateAPI,  '/api/v1.0/calcdate/', endpoint='stockcalcdate')

stocks = []
stocksBoard = []
stocksBoardStrong = []
stocksBoardReady = []
stocksStrong = []
stocksReady = []
stocksBreakCentral = []
stocksBreakVolume  = []
stocksBasic = []
stocksLabel = {}
stocksBasicSelect = []
stocksIndex = {}
stocksClose = {}
stocksOHLC =  {}
stocksMainForce = []
stocksEventDriven = []
stocksSuspend0 = []
stocksCalcDate = datetime.now()-timedelta(days=30)
stocksNewInfoDate= datetime.now()
config = {
  'user': 'root',
  'password': 'ypt@66601622',
  'host': '10.15.6.108',
  # 'host': '119.164.253.141',
  'database': 'liefeng', 
  'port' : 3306,
  #'raise_on_warnings': True,
  'buffered':True,
}

def GetStocksBoard(Connector,isStrong=True,isReady=False):
    cursor = Connector.cursor()
    cursor_list = Connector.cursor()
    if isStrong and  not isReady:
        sql = "select id,sectorid,name,windcode,type,update_date,pct_chg,cap from stocks_board_sr where isStrong=True order by id"
        sql_list = "select id,windcode,name,pct_chg,cap from stocks_boardlist_sr where isStrong=True and sectorid=%(sectorid)s order by id"
    if isReady and not isStrong:
        sql = "select id,sectorid,name,windcode,type,update_date,pct_chg,cap from stocks_board_sr where isReady=True order by id"
        sql_list = "select id,windcode,name,pct_chg,cap from stocks_boardlist_sr where isReady=True and sectorid=%(sectorid)s order by id"
    if  (isStrong and isReady) or (not isStrong and not isReady) :
        return None
    cursor.execute(sql)
    stocksBoard = []
    for sid,sectorid,bname,windcode,btype,update_date,pct,cap in cursor:
        sbl = []
        cursor_list.execute(sql_list,{'sectorid':sectorid,})
        for lid,lwindcode,lname,lpct,lcap in cursor_list:
            sbl.append({
                'id':lid,
                'name':lname,
                'windcode':lwindcode,
                'pct':lpct,
                'cap':lcap
                })

        sbs = {
            'id': sid ,    
            'windcode': windcode,
            'name' : bname,
            'type' : btype,
            'update_date' : update_date,
            'pct'  : pct,
            'cap'  : cap,
            'stockslist':sbl,
            }
        stocksBoard.append(sbs)
    cursor_list.close()
    cursor.close()
    return stocksBoard

def GetStocksSType(Connector,stype,ktype,ExistedFlag=False):
    cursor = Connector.cursor()
    if ExistedFlag:
       sql = "select id,name,windcode,pct_chg,cap from stocks_sr where stype=%(stype)s and ktype=%(ktype)s  order by date"
    
       cursor.execute(sql,{'stype':stype,'ktype':ktype})
    else:

        sql = "select id,name,windcode,pct_chg,cap from stocks_sr where stype=%(stype)s and ktype=%(ktype)s and date=%(date)s order by id"
    
        cursor.execute(sql,{'stype':stype,'ktype':ktype,'date':stocksCalcDate,})
    stocksBoard = []
    for sid,sname,windcode,pct,cap in cursor:    

        sbs = {
            'id': sid ,    
            'windcode': windcode,
            'name' : sname,
            'pct'  : pct,
            'cap'  : cap,
            }
        stocksBoard.append(sbs)
    
    cursor.close()
    return stocksBoard

def GetStocksBasic(Connector):
    global stocks
    global stocksBasic
    cursor = Connector.cursor()
    
    for stock in stocks:
        qtl = [ ]
        sql = "SELECT distinct quarter from stocks_basic WHERE windcode = %(code)s and btype=0 order by quarter desc limit 6"
        cursor.execute(sql,{'code':stock['windcode'],})
        for quarter in cursor:
            qtl.insert(0,  quarter[0].strftime("%Y-%m-%d") ) 
        
        b = []
        sql = "select content,quarter,btype from stocks_basic where windcode=%(code)s and btype=%(type)s order by quarter desc limit 8"
        for index in range(12) :
            temp = []

            data = {
                'code':stock['windcode'],
                'type':index,
            }
            cursor.execute(sql,data)
            for content,quarter,btype in cursor:
                temp.insert(0,content)
            if len(temp)==0:
                temp = [0]
            b.append(temp)
        orl = []
        sql = "select content from stocks_basic where windcode=%(code)s and btype=12 order by quarter desc limit 2"
        cursor.execute(sql,{'code':stock['windcode'],})
        for content in cursor:
            orl.insert(0,content[0])
        # print stock['windcode'],orl
        if len(orl) == 0:
            or_lastyear = 0 
        else:
            if orl[-1] == b[7][-1] :
                if len(orl)>1 :
                    or_lastyear = orl[-2]
            else:
                or_lastyear = orl[-1]
        boards = ''
        # print stock['windcode'],b[6],b[7],b[8],b[9] 
        sql = "SELECT stocks_board.`name` AS `name` FROM stocks_board ,stocks_boardlist WHERE stocks_board.sectorid = stocks_boardlist.sectorid AND stocks_boardlist.windcode = %(code)s"
        cursor.execute(sql,{'code':stock['windcode'],})
        for sname in cursor:
            boards += ";"+(sname[0])
        sql = "SELECT content FROM stocks_basic_other WHERE typename='BRIEFING' AND windcode = %(code)s"
        cursor.execute(sql,{'code':stock['windcode'],})
        intro = ""
        if (cursor.rowcount>0) :
            intro = cursor.fetchone()[0]
        sql = "SELECT content FROM stocks_basic_other WHERE typename='SEGMENT_SALES' AND windcode = %(code)s"
        cursor.execute(sql,{'code':stock['windcode'],})
        seg_sales = ""
        if (cursor.rowcount>0) :
            seg_sales = cursor.fetchone()[0]
        # print 'or : ',b[7][-1],' orl: ' ,or_lastyear
        stocksBasic.append( {
                'windcode'  : stock['windcode'],
                'name'      : stock['name'],
                'boards'    : boards[1:],
                'intro'     : intro,
                'seg_sales' : seg_sales, 
                'quarter'   : qtl,
                'stars'     : int(b[9][-1]),
                'ev'    :b[6][-1],  
                'debt_assets' :b[3][-1],  
                'or'   : b[7][-1], 
                'pe'   : b[8][-1], 
                'or_lastyear' : or_lastyear,
                'wgsd'        : b[4][2:],  
                'gross_pro_mg': b[10][-3:], 
                'oper_cash_ps': b[2][2:], 
                'oper_tr'     : b[0][2:],
                'profit'      : b[11][2:] ,
                'roe_basic'   : b[5][-3:], 
            }
            )
    cursor.close()

if __package__ is None:
    import sys
    from os import path
    sys.path.append( path.dirname( path.dirname( path.abspath(__file__) ) ) )
    from LieFeng import Config,Stock,FourIndicator,Slice
else:
    from ..LieFeng import Config,Stock,FourIndicator,Slice
Windcode_Dict = {}
GoldenFlag = True
Connector = mysql.connector.connect(**Config.config)
from WindPy import w
    
WindPy = w 
TradeDayFlag = False
def Sche_Init():
    
    MarketType = 1
    current_date = datetime.now()
    if not TradeDayFlag :
        print "scheduler return because is not TradeDay!"
        return 
    print 'scheduler init-------------'
    current_date = current_date.replace(second=0,microsecond = 0)
    s12 = current_date.replace(hour= 9,minute=35)
    s15 = current_date.replace(hour= 10,minute=00)
    s16 = current_date.replace(hour= 10,minute=30)
    e1 = current_date.replace(hour=11,minute=30)
    s22 = current_date.replace(hour=13,minute=05)
    s25 = current_date.replace(hour=13,minute=30)
    s26 = current_date.replace(hour=14,minute=00)
    e2 = current_date.replace(hour=14,minute=59)
    scheduler.pause()
    job1_60 = scheduler.add_job(id='160',func=WarningMessage, args=(6,), max_instances=1,trigger='interval', hours=1,start_date=s16,end_date=e1,replace_existing=True)
    job2_60 = scheduler.add_job(id='260',func=WarningMessage, args=(6,), max_instances=1,trigger='interval', hours=1,start_date=s26,end_date=e2,replace_existing=True)
    job1_30 = scheduler.add_job(id='130',func=WarningMessage, args=(5,), max_instances=1,trigger='interval', minutes=30,start_date=s15,end_date=e1,replace_existing=True)
    job2_30 = scheduler.add_job(id='230',func=WarningMessage, args=(5,), max_instances=1,trigger='interval', minutes=30,start_date=s25,end_date=e2,replace_existing=True)
    job1_5  = scheduler.add_job(id='105',func=WarningMessage2, max_instances=2, trigger='interval', minutes=5,start_date=s12,end_date=e1,replace_existing=True)
    job2_5  = scheduler.add_job(id='205',func=WarningMessage2, max_instances=2, trigger='interval', minutes=5,start_date=s22,end_date=e2,replace_existing=True)
    job_last  = scheduler.add_job(func=WarningMessageAll, trigger='cron', day_of_week='0-6',hour='15',minute=5 ,replace_existing=True)
    scheduler.resume()
    # scheduler.start()
    # job_minute = scheduler.add_job(func=WarningMessage, args=(Windcode_Dict,), trigger='cron', minute='0' ,replace_existing=True)

def Days_Init():
    global stocks
    global stocksBoard
    global stocksBoardStrong
    global stocksBoardReady
    global stocksStrong
    global stocksReady
    global stocksBreakCentral
    global stocksBreakVolume
    global stocksBasic
    global stocksLabel
    global stocksBasicSelect
    global stocksIndex
    global stocksClose
    global stocksOHLC
    global stocksMainForce
    global stocksCalcDate
    global stocksNewInfoDate
    global stocksSuspend0
    global Connector
    global TradeDayFlag
    Connector = mysql.connector.connect(**config)
    cursor = Connector.cursor()
    sql = "select date from stocks_sr order by date desc limit 1"
    cursor.execute(sql)
    if cursor.rowcount>0:
        stocksCalcDate = cursor.fetchone()[0]
    
    sql = "select date from stocks_history_days where ktype=7 order by date desc limit 1"
    cursor.execute(sql)
    if cursor.rowcount>0:
        stocksNewInfoDate = cursor.fetchone()[0]
    cursor.close()
    
    stocksSuspend0 = Stock.GetSuspendStockSet(datetime.now(),0,WindPy)
    if Config.CheckDateValid(datetime.now(),WindPy) ==1 :
        TradeDayFlag = True
    else:
        TradeDayFlag = False
    if len(stocks)==0:
        sql = "select  windcode, name, date,status,starter,zhongzheng500,hushen300 from stocks_code"
        cursor = Connector.cursor()
        
        cursor.execute(sql)
        for windcode, name, update_date,status,starter,zz500,hs300 in cursor:
            stocks.append({    
                'windcode': windcode,
                'name' : name,
                'type' : 'A',
                'status': status ,
                'is_cyb': starter,
                'is_zz500': zz500,
                'is_hs300': hs300,
                })

        cursor.close()
    if len(stocksBasicSelect)==0 :
        sql = "select  windcode, name, date from stocks_code"
        cursor = Connector.cursor()
        cursor_stars = Connector.cursor()
        cursor.execute(sql)
        index = 0 
        sql_stars = "select content from stocks_basic where windcode=%(code)s and btype=9 order by quarter desc limit 1"
        sql_p="select pct_chg,cap from stocks_history_days where ktype=7 and windcode=%(code)s order by date desc limit 1"
        for windcode, name, update_date in cursor:
            
            cursor_stars.execute(sql_stars,{'code':windcode,})
            if cursor_stars.rowcount == 0 or cursor_stars.fetchone()[0] <=5 :
                continue
            
            cursor_stars.execute(sql_p,{'code':windcode,})
            pct = 0
            cap = 0
            if cursor_stars.rowcount>0:
                temp = cursor_stars.fetchone()
                pct = temp[0]
                cap = temp[1]
            # tasks =  [ task for task in stocks  if task['windcode'] == windcode ]
            stocksBasicSelect.append({ 
                'id'  : index  ,
                'windcode': windcode,
                'name' : name,
                'type' : 'stars 5',
                # 'status': tasks[0]['status'] ,
                'pct' : pct,
                'cap' : cap,
                })
            index += 1
        cursor_stars.close()
        cursor.close()
    if len(stocksBoard) == 0 :
        cursor = Connector.cursor()
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
    if len(stocksIndex) ==0 :
        cursor = Connector.cursor()
        sql = "select num,typename,date from stocks_index where typename=%(type)s order by date desc limit 30"
        tl = ['FundIndex','StrongIndex','ReadyIndex']
        for typename in tl:
            cursor.execute(sql,{'type':typename,})
            stocksIndex[ typename ] = []
            for bnum,btn,bdate in cursor:
                stocksIndex[ typename ].insert(0,{'data':bnum,'update_date':bdate})
        cursor.close()
    if len(stocksClose) == 0 :
        cursor = Connector.cursor()
        sql = "select close,date from stocks_history_days where ktype=7 and windcode=%(code)s order by date desc limit 30"
        zs = ["000001.SH",'399300.SZ',"399006.SZ","150051.SZ","399005.SZ","399905.SZ"]
        for windcode in zs:
            cursor.execute(sql,{'code':windcode,})
            stocksClose[ windcode ] = []
            for sclose,sdate in cursor:
                stocksClose[ windcode ].insert(0,{'data':sclose,'update_date':sdate})
        for board in stocksBoard:
            windcode = board['windcode']
            cursor.execute(sql,{'code':windcode,})
            stocksClose[ windcode ] = []
            for sclose,sdate in cursor:
                stocksClose[ windcode ].insert(0,{'data':sclose,'update_date':sdate}) 
        cursor.close()
    if len(stocksOHLC) == 0 :
        cursor = Connector.cursor()
        sql = "select open,high,low,close,date from stocks_history_days where ktype=7 and windcode=%(code)s order by date desc limit 30"
        
        for index  in range(len(stocks)):
            windcode = stocks[index]['windcode']
            cursor.execute(sql,{'code':windcode,})
            stocksOHLC[ windcode ] = {'open':[],'high':[],'low':[],'close':[],'update_date':[]}
            for sopen,shigh,slow,sclose,sdate in cursor:
                stocksOHLC[ windcode]['open'].insert(0,sopen)
                stocksOHLC[ windcode]['high'].insert(0,shigh)
                stocksOHLC[ windcode]['low'].insert(0,slow)
                stocksOHLC[ windcode]['close'].insert(0,sclose)
                stocksOHLC[ windcode]['update_date'].insert(0,sdate)
         
        cursor.close()  
    else:
        cursor = Connector.cursor()
        cursor_all= Connector.cursor()
        sql = "select open,high,low,close,date from stocks_history_days where ktype=7 and windcode=%(code)s and date>%(date)s order by date desc"
        sql_all = "select open,high,low,close,date from stocks_history_days where ktype=7 and windcode=%(code)s order by date desc limit 30"
        for index  in range(len(stocks)):            
            windcode = stocks[index]['windcode']
            if stocksOHLC.get(windcode) is None or stocksOHLC[ windcode].get('update_date') is None or len(stocksOHLC[ windcode]['update_date'])==0:
                
                cursor_all.execute(sql_all,{'code':windcode,})
                stocksOHLC[ windcode ] = {'open':[],'high':[],'low':[],'close':[],'update_date':[]}
                for sopen,shigh,slow,sclose,sdate in cursor_all:
                    stocksOHLC[ windcode]['open'].insert(0,sopen)
                    stocksOHLC[ windcode]['high'].insert(0,shigh)
                    stocksOHLC[ windcode]['low'].insert(0,slow)
                    stocksOHLC[ windcode]['close'].insert(0,sclose)
                    stocksOHLC[ windcode]['update_date'].insert(0,sdate)
                continue 
            
            else:        
                cursor.execute(sql,{'code':windcode,'date':stocksOHLC[ windcode]['update_date'][-1]})
                temp = {'open':[],'high':[],'low':[],'close':[],'update_date':[]}
                for sopen,shigh,slow,sclose,sdate in cursor:
                    temp['open'].insert(0,sopen)
                    temp['high'].insert(0,shigh)
                    temp['low'].insert(0,slow)
                    temp['close'].insert(0,sclose)
                    temp['update_date'].insert(0,sdate)
                if len( temp['open'] ) > 0 :
                    stocksOHLC[windcode]['open'].extend( temp['open'] )
                    stocksOHLC[windcode]['high'].extend( temp['high'] )
                    stocksOHLC[windcode]['low'].extend( temp['low'] )
                    stocksOHLC[windcode]['update_date'].extend( temp['update_date'] )
         
        cursor.close()
        cursor_all.close()
    if len(stocksMainForce) == 0 :
        cursor = Connector.cursor()
        cursor_p = Connector.cursor()
        sql = "select date from stocks_mainforce order by date desc limit 1 "
        cursor.execute(sql)
        if cursor.rowcount > 0 :
            date = cursor.fetchone()[0]
            sql = "select windcode,type,ratio,cost,target from stocks_mainforce where type=%(type)s and date = %(date)s "
            sql_p="select pct_chg,cap from stocks_history_days where ktype=7 and windcode=%(code)s order by date desc limit 1"
            tl = ['new','inc','equal']
            num = 0
            for typename in tl:
                cursor.execute(sql,{'date':date,'type':typename,})            
                for code,typename,ratio,cost,target in cursor:
                    cursor_p.execute(sql_p,{'code':code,})
                    pct = 0
                    cap = 0
                    if cursor_p.rowcount>0:
                        temp = cursor_p.fetchone()
                        pct = temp[0]
                        cap = temp[1]
                    name = [x['name'] for x in stocks if x['windcode'] == code ]
                    stocksMainForce.append({
                        'windcode' : code,
                        'id' : num,
                        'name'     : name[0],
                        'date' : datetime.combine(date,datetime.min.time()),
                        'type' : typename,
                        'ratio': ratio,
                        'cost' : cost,
                        'target': target,     
                        'pct' : pct ,
                        'cap' : cap ,
                    }) 
                    num += 1      
        cursor.close()
        cursor_p.close()
    if len(stocksBoardStrong) == 0 :
        stocksBoardStrong=GetStocksBoard(Connector,isStrong=True,isReady=False)
    if len(stocksBoardReady) == 0 :
        stocksBoardReady =GetStocksBoard(Connector,isStrong=False,isReady=True)
    if len(stocksStrong) == 0 :
        stocksStrong = GetStocksSType(Connector,0,8)
    if len(stocksReady) == 0 :
        stocksReady  = GetStocksSType(Connector,1,8)
    if len(stocksBreakCentral) == 0 :
        stocksBreakCentral  = GetStocksSType(Connector,2,8)
    if len(stocksBreakVolume) == 0 :
        stocksBreakVolume   = GetStocksSType(Connector,3,8)
    if len(stocksBasic) == 0 :
        GetStocksBasic(Connector)
    if len(stocksLabel) == 0 :
        cursor = Connector.cursor()
        BreakCentralAll = GetStocksSType(Connector,2,8,ExistedFlag=True)
        BreakVolumeAll  = GetStocksSType(Connector,3,8,ExistedFlag=True)
        sql = "select content from stocks_basic where windcode=%(code)s and btype=9 order by quarter desc limit 1"
        for stock in stocks:
            stars = 0            
            ready  = [ task for task in stocksReady  if task['windcode'] == stock['windcode']]
            strong = [ task for task in stocksStrong if task['windcode'] == stock['windcode']]
            bc = [ task for task in BreakCentralAll if task['windcode'] == stock['windcode']]
            bv = [ task for task in BreakVolumeAll if task['windcode'] == stock['windcode']]
            tech = len(ready) >0 or len(strong) >0 or len(bc) or len(bv)
            bs = [ task for x in stocksBoardStrong for task in x['stockslist'] if task['windcode'] == stock['windcode']]
            br = [ task for x in stocksBoardReady for task in x['stockslist'] if task['windcode'] == stock['windcode']]
            cursor.execute(sql,{'code':stock['windcode'],})
            if cursor.rowcount >0 :
                stars = cursor.fetchone()[0]
            basic = stars > 5
            tasks = [ x for x in stocksMainForce if x['windcode']==stock['windcode']]
            stocksLabel[stock['windcode']] = ({
                'windcode' : stock['windcode'] ,
                'is_strong': (len(strong)>0),
                'is_ready':  (len(ready)>0),
                'is_bc': (len(bc)>0),
                'is_bv': (len(bv)>0),    
                'is_bs': (len(bs)>0),
                'is_br': (len(br)>0),                
                'is_jishu' : tech,
                'is_jiben' : basic,
                'is_mf'    : (len(tasks)>0),
                'stars'    : int(stars),
                }
                )            
        cursor.close()
    
    # Connector.close()

def OHLC_Update():
    global stocksOHLC
    current_time = datetime.now()
    if not TradeDayFlag :
        print "OHLC_Update Error not TradeDay!"
        return 
    
    for windcode in  stocksOHLC :
        if windcode in stocksSuspend0:
            continue        
        wsi = WindPy.wsq(windcode, "rt_open,rt_high,rt_low,rt_last")
        # print windcode,wsi
        if not Config.CheckWindData(wsi,'WSQ Error') :
            continue        
        stocksOHLC[ windcode]['open'].append(wsi.Data[0][0])
        stocksOHLC[ windcode]['high'].append(wsi.Data[1][0])
        stocksOHLC[ windcode]['low'].append( wsi.Data[2][0])
        stocksOHLC[ windcode]['close'].append(wsi.Data[3][0])
        stocksOHLC[ windcode]['update_date'].append(current_time)

def OHLC_RemoveLast():
    global stocksOHLC  
    if not TradeDayFlag :
        print "OHLC_RemoveLast Error not TradeDay!"
        return 
    
    for windcode in  stocksOHLC :
        if windcode in stocksSuspend0:
            continue
        if len(stocksOHLC[ windcode ]['update_date']) == 0 :
            continue
        lastdate= stocksOHLC[ windcode ]['update_date'][-1]
        if lastdate.hour != 0 :       
            stocksOHLC[ windcode]['open'].pop()
            stocksOHLC[ windcode]['high'].pop()
            stocksOHLC[ windcode]['low'].pop()
            stocksOHLC[ windcode]['close'].pop()
            stocksOHLC[ windcode]['update_date'].pop()


def Init():
    global stocks
    global stocksBoard
    global stocksBoardStrong
    global stocksBoardReady
    global stocksStrong
    global stocksReady
    global stocksBreakCentral
    global stocksBreakVolume
    global stocksBasic
    global stocksLabel
    global stocksBasicSelect
    global stocksIndex
    global stocksClose
    global stocksOHLC
    global stocksMainForce
    global stocksCalcDate
    global GoldenFlag
    
    stocks = []
    stocksBoard = []
    stocksBoardStrong = []
    stocksBoardReady = []
    stocksStrong = []
    stocksReady = []
    stocksBreakCentral = []
    stocksBreakVolume  = []
    stocksBasic = []
    stocksLabel = {}
    stocksBasicSelect = []
    stocksIndex = {}
    stocksClose = {}
    stocksMainForce = []
    current_time = datetime.now()
    stocksCalcDateBefore= stocksCalcDate
    Days_Init()   
    EventDrivenStocks() 
    
    WarningMessage_Init(GdbbFlag=GoldenFlag)
    print stocksCalcDateBefore,stocksNewInfoDate,stocksCalcDate
    if GoldenFlag:
        WarningMessage_CurrentDay_Data([2,5,6],current_time)        
        if current_time.hour>9 and  current_time.hour <13:
            OHLC_Update()
        RefreshRequest()
    else:
        OHLC_RemoveLast() 
        if stocksCalcDateBefore.hour == 13 :
            if stocksCalcDateBefore > stocksNewInfoDate and stocksCalcDate > stocksNewInfoDate:
                RefreshRequest()
        else:
            if stocksCalcDateBefore < stocksNewInfoDate and stocksCalcDate > stocksNewInfoDate:
                RefreshRequest() 
    if TradeDayFlag and not GoldenFlag:
        # OHLC_RemoveLast()
        OHLC_Update()

        
    gc.collect()
    GoldenFlag = False

    
    print 'INIT finished!---------------'

def EventDrivenStocks():
    global stocksEventDriven
    url = "https://www.hanzhendata.com/ihanzhendata/logicstocks/all"
    response = requests.request("GET", url)
    if response.status_code == 200 :
        data = response.json()
        # print 'EventDrivenStocks ',response.status_code,data
        if data['status'] == 1 :
            stocksEventDriven = data['data']
    requests.request("GET", url, headers={'Connection':'close'})

def WarningMessage_Init(GdbbFlag=False):
    global Windcode_Dict
    # global Connector
    Windcode_Dict_New={}
    MarketType = 1
    sql = "select  name, windcode,stype from stocks_sr"   
    sql_w = "select wtype,ktype,date,stype  from stocks_warning where windcode=%(code)s  order by date desc limit 1"
    cursor = Connector.cursor()
    cursor_w = Connector.cursor()
    num = 0
    cursor.execute(sql)
    for bname,windcode,stype in cursor:
        if windcode in stocksSuspend0:
            continue  
        Windcode_Dict_New [windcode] = {'name':bname,'type':stype,'wsq2':0,'wsq5':0,'wsq6':0,}
        Windcode_Dict_New[windcode].update(stocksLabel[ windcode] )
    print len(Windcode_Dict_New),len(stocksBasicSelect),len(stocksMainForce),len(stocksEventDriven)
    wd=[stocksBasicSelect,stocksMainForce,stocksEventDriven]
    for wi in range(len(wd)):
        if wi==1 or wi==0: #current not in calcluting range
            continue
        for index in range(len( wd[wi] )):
            windcode = wd[wi][index]['windcode']
            if windcode in stocksSuspend0:
                continue
            if wi==1: #stocksMainForce satify either in boardstrong or in boardready
                if not stocksLabel[windcode]['is_bs'] and not stocksLabel[windcode]['is_br'] :                    
                    continue
            if Windcode_Dict_New.get(windcode) is None:                
                Windcode_Dict_New [windcode] = {'name':wd[wi][index]['name'],'type':wi+4,'wsq2':0,'wsq5':0,'wsq6':0,}
                Windcode_Dict_New[windcode].update(stocksLabel[ windcode] )
    cursor.close()
    for windcode in Windcode_Dict:
        if Windcode_Dict_New.get(windcode) is not None:
            Windcode_Dict_New[windcode] = Windcode_Dict[windcode]
    Windcode_Dict = Windcode_Dict_New
    del Windcode_Dict_New       
    for windcode in Windcode_Dict:
        cursor_w.execute(sql_w,{'code':windcode,})
        if cursor_w.rowcount>0:
            temp = cursor_w.fetchone()
            ts = WindPy.tdayscount(temp[2],datetime.now(),"")
            stype = temp[3]
            if ts.ErrorCode!=0 or ( Windcode_Dict[windcode]['is_bc'] and ts.Data[0][0] >Config.BreakCentralPermitDays) or ( Windcode_Dict[windcode]['is_bv']==3 and ts.Data[0][0] >Config.BreakVolumePermitDays):
                Windcode_Dict[windcode].update({'warning_status':0,})
                continue
            ws = temp[0]            
            Windcode_Dict[windcode].update({'warning_status':ws,})            
            if ws==1 :
                Windcode_Dict[windcode].update({'warning_ktype':temp[1],'warning_date':temp[2],})            
        else:
            Windcode_Dict[windcode].update({'warning_status':0,})
        num += 1
        # if num>6:
        #     break       

    cursor_w.close()
        
    # print Windcode_Dict['300039.SZ']
    
    print "stocks warning message init-------------------------------"
    print len(Windcode_Dict)
    ktype_list= [2,5,6,8]
    current_time = datetime.now()
    for ktype in ktype_list :
        if ktype > 6:
            tablename = 'stocks_history_days'
        else:       
            tablename = 'stocks_history_minutes'
        for windcode in  Windcode_Dict  :
            # print windcode
            
            ktypestr = str(ktype)
            if Windcode_Dict[windcode].get(ktypestr) is None:
                indicator = Stock.StocksOHLC_N(windcode,tablename,ktype,Connector,limit=2000)
                Windcode_Dict[windcode].update( { ktypestr : indicator,})
            else:
                indicator = Stock.StocksOHLC_N(windcode,tablename,ktype,Connector,limit=2000,StartDate=Windcode_Dict[windcode][ktypestr][-1]['date'])
                Windcode_Dict[windcode][ktypestr].extend( indicator  ) 
            Windcode_Dict[windcode]['tech'+ktypestr] = Stock.StocksTech(Windcode_Dict[windcode][ktypestr])
    for ktype in [2,5] : 
        if  not GdbbFlag :        
            continue 
        ktypestr = str(ktype)     
        for windcode in  Windcode_Dict  :            
            
            if Windcode_Dict[windcode]['warning_status'] != 1 :
                continue 
            if Windcode_Dict[windcode]['type'] == 2:
                if ktype != 2 :
                    continue
                
            else:
                if ktype != 5 :
                    continue            
            indicator =  Windcode_Dict[windcode][ktypestr]
            wnd =   Windcode_Dict[windcode]['warning_date']
            
            if wnd.date() == current_time :
                continue
            
            if ktype==5 :
                wnd = wnd.replace(minute= wnd.minute- wnd.minute % 30,second=0)
                if wnd.hour == 9 and wnd.minute==30 :
                    wnd = wnd.replace(hour = 10 ,second =0)
                if wnd.hour == 13 and wnd.minute==00 :
                    wnd = wnd.replace(hour = 14 ,second =0)                
            if ktype == 2:
                wnd = wnd.replace(minute= wnd.minute- wnd.minute % 5,second=0)
                if wnd.hour == 9 and wnd.minute==30 :
                    wnd = wnd.replace(minute = 35 ,second =0)
                if wnd.hour == 13 and wnd.minute==00 :
                    wnd = wnd.replace(minute = 5 ,second =0)
            
            print windcode,wnd
            for index in range( len(indicator) ):
                if  wnd == indicator[-index-1]['date'] :
                    break
            print ktype,windcode,Windcode_Dict[windcode]['warning_date'],Windcode_Dict[windcode]['warning_status'],-index-1
            for i in range(-index-1,0):
                # print -index-1,i,indicator[i]['date']
                if Windcode_Dict[windcode]['tech'+ktypestr]['diff'][i] > Windcode_Dict[windcode]['tech'+ktypestr]['dea'][i] :
                    if Windcode_Dict[windcode]['tech'+ktypestr]['diff'][i-1] < Windcode_Dict[windcode]['tech'+ktypestr]['dea'][i-1] :
                        if Windcode_Dict[windcode]['tech'+ktypestr].get('gdbb') is None:
                            Windcode_Dict[windcode]['tech'+ktypestr].update( {'gdbb':[4]} )
                        else:
                            Windcode_Dict[windcode]['tech'+ktypestr]['gdbb'].append(4)
    #add unfinished week days OHLC. 
    ktype = 8 
    last_date = Stock.StocksGetLastDate(ktype,current_time.date())

    ds =str(ktype)
    for windcode  in  Windcode_Dict :
        board = Windcode_Dict[windcode]     

        # print windcode,board.keys()
        board['ta'+str(ktype)] = board[ds]
         
        board['ta'+str(ktype-1)] = []
        for ri in range(7):
            board['ta'+str(ktype-1)].insert(0,{
                'date':stocksOHLC[windcode]['update_date'][-ri-1],
                'open':stocksOHLC[windcode]['open'][-ri-1],
                'high':stocksOHLC[windcode]['high'][-ri-1],
                'low' :stocksOHLC[windcode]['low'][-ri-1],
                'close':stocksOHLC[windcode]['close'][-ri-1],
                })        
        if len(last_date)>0 and board[ds][-1]['date'].date() < last_date[-1]:
            last_data=Stock.StocksGetLastData_New( board ,ktype,last_date)

            if last_data is not None and len(last_data) > 0: 
                last_data.update({'date':datetime.combine(last_date[-1], datetime.min.time())})           
                Windcode_Dict[windcode][ds].append(last_data)
            else:
                Windcode_Dict[windcode][ds].append({'date': current_time,
                    'open':Windcode_Dict[windcode][ds][-1]['open'],'high':0,'low':0,'close':0
                    })   

def WarningMessage_CurrentDay_Data(KtypeList,current_datetime):
    if current_datetime.hour <=9 or current_datetime.hour >=15 :
        return
    for windcode in Windcode_Dict:
        starttime = current_datetime.replace(hour=9,minute=31,second=0,microsecond=0)
        wsi = WindPy.wsi(windcode,"open,high,low,close,volume",starttime,current_datetime,"PriceAdj=F")  
        if not Stock.CheckWindData(wsi,Message='WSI Error') :
            continue
        for ktype in  KtypeList :
            rl=[]
            Config.CheckKtype(ktype,rl)
            BarSize = rl[0]
            ktypestr = str(ktype)
            for index in range(len(wsi.Data[0])):
                if index >0 and index % BarSize == (BarSize-1):
                    Windcode_Dict[windcode][ktypestr].append( {
                        'date' : wsi.Times[index],
                        'open' : wsi.Data[0][index-BarSize+1],
                        'high' : max(wsi.Data[1][index-BarSize+1:index+1]),
                        'low'  : min(wsi.Data[2][index-BarSize+1:index+1]),
                        'close': wsi.Data[3][index],
                        'volume':wsi.Data[4][index],
                    })

def WarningMessage_SignalRebuild(current_datetime):
    print "stocks warning Signal rebuild from today-------------------------------"    
    sql = "Insert into stocks_warning(id,wtype,stype,ktype,windcode,name,date) values(%(sid)s,%(w)s,%(s)s,%(k)s,%(code)s,%(name)s,%(wdate)s)"
    for ktype in [6] :
        for windcode in  Windcode_Dict  :
            # print windcode
            ktypestr = str(ktype)   
            if Windcode_Dict[windcode]['warning_status'] !=0 :
                continue
            
            for index in range( len( Windcode_Dict[windcode]['tech'+ ktypestr]) ) :
                wtc=WindPy.tdayscount(Windcode_Dict[windcode][ktypestr][-index-1]['date'],Windcode_Dict[windcode][ktypestr][-1]['date'], "")
                if wtc.ErrorCode ==0 :
                    if wtc.Data[0][0] > 10 :
                        break
                if ( Windcode_Dict[windcode]['is_bc'] and Windcode_Dict[windcode]['tech'+ktypestr]['fast_j'][-index-1] < 5 ) or ( ktype==6 and not Windcode_Dict[windcode]['is_bc'] and Windcode_Dict[windcode]['tech'+ktypestr]['slow_j'][-index-1] < 5 ):
                    print windcode,ktype,Windcode_Dict[windcode][ktypestr][-index-1]['date']
                    if Windcode_Dict[windcode]['is_bc']:
                        Windcode_Dict[windcode].update({'type':2})
                    Windcode_Dict[windcode].update({'warning_ktype':ktype,'warning_status':1,'warning_date':Windcode_Dict[windcode][ktypestr][-index-1]['date']}) 
    
    for ktype in [2,5]:
        ktypestr = str(ktype)
        for windcode in Windcode_Dict:
            if Windcode_Dict[windcode]['warning_status'] != 10 :
                continue 
            if Windcode_Dict[windcode]['type'] == 2:
                if ktype != 2 :
                    continue                
            else:
                if ktype != 5 :
                    continue            
            indicator =  Windcode_Dict[windcode][ktypestr]
            # if Windcode_Dict[windcode]['warning_ktype'] != ktype  :
            #     continue

            wnd =   Windcode_Dict[windcode]['warning_date']
            if ktype==5 :
                wnd = wnd.replace(minute= wnd.minute- wnd.minute % 30,second=0)
                if wnd.hour == 9 and wnd.minute==30 :
                    wnd = wnd.replace(hour = 10 ,second =0)
                if wnd.hour == 13 and wnd.minute==00 :
                    wnd = wnd.replace(hour = 14 ,second =0)                
            if ktype == 2:
                wnd = wnd.replace(minute= wnd.minute- wnd.minute % 5,second=0)
                if wnd.hour == 9 and wnd.minute==30 :
                    wnd = wnd.replace(minute = 35 ,second =0)
                if wnd.hour == 13 and wnd.minute==00 :
                    wnd = wnd.replace(minute = 5 ,second =0)
            for index in range( len(indicator) ):
                if  wnd == indicator[-index-1]['date'] :
                    break
            print ktype,windcode,Windcode_Dict[windcode]['warning_date'],Windcode_Dict[windcode]['warning_status'],-index-1
            if ktype == 2 :
                for i in range(-index-1,0):
                    temp = {'diff':Windcode_Dict[windcode]['tech'+ktypestr]['diff'][i-1:i+1], 
                            'dea' :Windcode_Dict[windcode]['tech'+ktypestr]['dea'][i-1:i+1]}
                    if  FourIndicator.GoldenDead(temp) == 4 :                        
                        Windcode_Dict[windcode].update({'warning_status':4,'warning_ktype':ktype,'warning_date':Windcode_Dict[windcode][ktypestr][i]['date']})
                        break
            if ktype==5:
                for i in range(-index-1,0):
                    # print -index-1,i,indicator[i]['date']
                    if Windcode_Dict[windcode]['tech'+ktypestr]['diff'][i] > Windcode_Dict[windcode]['tech'+ktypestr]['dea'][i] :
                        if Windcode_Dict[windcode]['tech'+ktypestr]['diff'][i-1] < Windcode_Dict[windcode]['tech'+ktypestr]['dea'][i-1] :
                            if Windcode_Dict[windcode]['tech'+ktypestr].get('gdbb') is None:
                                Windcode_Dict[windcode]['tech'+ktypestr].update( {'gdbb':[4]} )
                            else:
                                Windcode_Dict[windcode]['tech'+ktypestr]['gdbb'].append(4)
                    if Windcode_Dict[windcode]['tech'+ktypestr].get('gdbb') is None:
                        continue
                    gl = len(Windcode_Dict[windcode]['tech'+ktypestr]['gdbb'])
                    if gl==1 and Windcode_Dict[windcode]['is_strong'] :
                        ws = 4
                        Windcode_Dict[windcode].update({'warning_status':ws,'warning_ktype':ktype,'warning_date':Windcode_Dict[windcode][ktypestr][i]['date']})               
                        break
                    if gl==2 :
                        if Windcode_Dict[windcode]['is_jiben'] or Windcode_Dict[windcode]['is_ready'] or Windcode_Dict[windcode]['is_mf'] :
                            ws = 4
                            Windcode_Dict[windcode].update({'warning_status':ws,'warning_ktype':ktype,'warning_date':Windcode_Dict[windcode][ktypestr][i]['date']})
                            break
    f =open('Signal.txt','w')
    for windcode in  Windcode_Dict  :
        if Windcode_Dict[windcode]['warning_status']==10 or Windcode_Dict[windcode]['warning_status']==4 :
            cursor.execute(sql,{
                'sid':100,
                'w':Windcode_Dict[windcode]['warning_status']/10,
                's':Windcode_Dict[windcode]['type'],
                'k':Windcode_Dict[windcode]['warning_ktype'],
                'name':Windcode_Dict[windcode]['name'],
                'code':windcode,
                'wdate':Windcode_Dict[windcode]['warning_date']    
                })
            f.write(" code %s date %s type %d Signal %d ktype %d\n" %(windcode,Windcode_Dict[windcode]['warning_date'].strftime("%Y-%m-%d %H:%M:%S")
 ,Windcode_Dict[windcode]['type'] ,Windcode_Dict[windcode]['warning_status']/10,Windcode_Dict[windcode]['warning_ktype']))
            print windcode,Windcode_Dict[windcode]['warning_date'] 
    # Connector.commit() 

def WarningMessage_SignalRebuild():
    gd = WindPy.tdaysoffset(0,datetime.now(),'')
    gdate = gd.Data[0][0]
    MarketType = 1
    # sql = "select  name, windcode,stype,date from stocks_sr where windcode='600897.SH' order by windcode asc,date desc"   
    sql = "select  name, windcode,stype,date from stocks_sr  order by windcode asc,date desc"   
    sql_w = "select wtype,ktype,date,stype  from stocks_warning where windcode=%(code)s  order by date desc limit 1"
    cursor = Connector.cursor()
    cursor_w = Connector.cursor()
    num = 0
    cursor.execute(sql)
    for bname,windcode,stype,sdate in cursor:
        if windcode in stocksSuspend0:
            continue 
        if sdate.hour == 11 :
            continue 
        Windcode_Dict [windcode] = {'name':bname,'warning_status':0,'type':stype,'date':sdate,'wsq2':0,'wsq5':0,'wsq6':0,}
        Windcode_Dict[windcode].update(stocksLabel[ windcode] )
    print len(Windcode_Dict),len(stocksBasicSelect),len(stocksMainForce),len(stocksEventDriven)
    wd=[] #[stocksBasicSelect,stocksMainForce,stocksEventDriven]
    for wi in range(len(wd)):
        if wi==1:
            continue
        for index in range(len( wd[wi] )):
            windcode = wd[wi][index]['windcode']
            if windcode in stocksSuspend0:
                continue
            if Windcode_Dict.get(windcode) is None:
                if wi==1: #stocksMainForce satify either in boardstrong or in boardready
                    if not stocksLabel[windcode]['is_bs'] and not stocksLabel[windcode]['is_br'] :
                        continue
                Windcode_Dict [windcode] = {'name':wd[wi][index]['name'],'warning_status':0,'type':wi+4,'date':gdate,'wsq2':0,'wsq5':0,'wsq6':0,}
                Windcode_Dict[windcode].update(stocksLabel[ windcode] )
    cursor.close()
    
    # for windcode in Windcode_Dict:
    #     cursor_w.execute(sql_w,{'code':windcode,})
    #     if cursor_w.rowcount>0:
    #         temp = cursor_w.fetchone()
    #         ts = WindPy.tdayscount(temp[2],datetime.now(),"")
    #         stype = temp[3]
    #         if ts.ErrorCode!=0 or ( stype==2 and ts.Data[0][0] >Config.BreakCentralPermitDays) or ( stype==3 and ts.Data[0][0] >Config.BreakVolumePermitDays):
    #             Windcode_Dict[windcode].update({'warning_status':0,})
    #             continue
    #         ws = temp[0]            
    #         Windcode_Dict[windcode].update({'warning_status':ws,})            
    #         if ws==1 :
    #             Windcode_Dict[windcode].update({'warning_status':10,'warning_ktype':temp[1],'warning_date':temp[2],})            
    #     else:
    #         Windcode_Dict[windcode].update({'warning_status':0,})
    #     num += 1
    #     # if num>6:
    #     #     break       

    # cursor_w.close()
        

    
    print "stocks warning message rebuild-------------------------------"
    print len(Windcode_Dict)
    ktype_list= [2,5,6,8]
    current_date = datetime.now().date()
    for ktype in ktype_list :
        if ktype>6:
            tablename = 'stocks_history_days'
        else:       
            tablename = 'stocks_history_minutes'
        for windcode in  Windcode_Dict  :
            # print windcode
            
            ktypestr = str(ktype)
            indicator = Stock.StocksOHLC_N(windcode,tablename,ktype,Connector,limit=2000)
            Windcode_Dict[windcode].update( { ktypestr : indicator,})
            if ktype == 8 :
                last_date = Stock.StocksGetLastDate(ktype,current_date)
                if last_date is None:
                    continue
                ds =str(ktype)
                
                board = Windcode_Dict[windcode]     
                if board.get(ds) is None or len(board[ds]) ==0 :
                    continue
                board['ta'+str(ktype)] = board[ds]
                board['ta'+str(ktype-1)] = []
                for ri in range(7):
                    board['ta'+str(ktype-1)].insert(0,{
                        'date':stocksOHLC[windcode]['update_date'][-ri-1],
                        'open':stocksOHLC[windcode]['open'][-ri-1],
                        'high':stocksOHLC[windcode]['high'][-ri-1],
                        'low' :stocksOHLC[windcode]['low'][-ri-1],
                        'close':stocksOHLC[windcode]['close'][-ri-1],
                        })
                # print board[ds][-1]['date'],last_date[-1]
                if len(last_date)>0 and board[ds][-1]['date'].date() < last_date[-1]:
                    last_data=Stock.StocksGetLastData_New( board ,ktype,last_date)

                    if last_data is not None and len(last_data) > 0: 
                        last_data.update({'date':last_date[-1]})           
                        Windcode_Dict[windcode][ds].append(last_data) 
            
            Windcode_Dict[windcode]['tech'+ktypestr] = Stock.StocksTech(indicator)
            
    cursor = Connector.cursor()
    sql = "Insert into stocks_warning(id,wtype,stype,ktype,windcode,name,date) values(%(sid)s,%(w)s,%(s)s,%(k)s,%(code)s,%(name)s,%(wdate)s)"
    f =open('Signal.txt','w')
    for windcode in  Windcode_Dict  :
        tablename = 'stocks_history_minutes'
        for ktype in [6] :
            # print windcode
            ktypestr = str(ktype)   
            if Windcode_Dict[windcode]['warning_status'] !=0 :
                continue
            print windcode,Windcode_Dict[windcode]['date'],len(Windcode_Dict[windcode][ktypestr]),len(Windcode_Dict[windcode]['tech'+ ktypestr])
            for index in range( len(Windcode_Dict[windcode][ktypestr]) ) :
                wtc=WindPy.tdayscount(Windcode_Dict[windcode][ktypestr][-index-1]['date'],Windcode_Dict[windcode][ktypestr][-1]['date'], "")
                if wtc.ErrorCode ==0 :
                    if wtc.Data[0][0] > 10 :
                        break
                # print Windcode_Dict[windcode][ktypestr][-index-1]['date']
                if Windcode_Dict[windcode][ktypestr][-index-1]['date'] <= Windcode_Dict[windcode]['date'] :
                    break
                print -index-1,Windcode_Dict[windcode]['tech'+ktypestr]['slow_j'][-index-1]
                if ( Windcode_Dict[windcode]['is_bc'] and Windcode_Dict[windcode]['tech'+ktypestr]['fast_j'][-index-1] < 5 ) or ( not Windcode_Dict[windcode]['is_bc'] and Windcode_Dict[windcode]['tech'+ktypestr]['slow_j'][-index-1] < 5 ):
                        print windcode,ktype,Windcode_Dict[windcode][ktypestr][-index-1]['date'],Windcode_Dict[windcode]['is_bc']
                        if Windcode_Dict[windcode]['is_bc']:
                            Windcode_Dict[windcode].update({'type':2}) 
                        Windcode_Dict[windcode].update({'warning_ktype':ktype,'warning_status':10,'warning_date':Windcode_Dict[windcode][ktypestr][-index-1]['date']}) 
        
        if Windcode_Dict[windcode]['warning_status'] != 10 :
            continue
        f.write(" code %s date %s type %d Signal %d ktype %d\n" %(windcode,Windcode_Dict[windcode]['warning_date'].strftime("%Y-%m-%d %H:%M:%S")
            ,Windcode_Dict[windcode]['type'] ,Windcode_Dict[windcode]['warning_status']/10,Windcode_Dict[windcode]['warning_ktype']))
               
        for ktype in [2,5]:
            ktypestr = str(ktype) 
            if Windcode_Dict[windcode]['type'] == 2:
                if ktype != 2 :
                    continue                
            else:
                if ktype != 5 :
                    continue            
            indicator =  Windcode_Dict[windcode][ktypestr]
            # if Windcode_Dict[windcode]['warning_ktype'] != ktype  :
            #     continue

            wnd =   Windcode_Dict[windcode]['warning_date']
            if ktype == 5 :
                wnd = wnd.replace(minute= wnd.minute- wnd.minute % 30,second=0)
                if wnd.hour == 9 and wnd.minute==30 :
                    wnd = wnd.replace(hour = 10 ,second =0)
                if wnd.hour == 13 and wnd.minute==00 :
                    wnd = wnd.replace(hour = 14 ,second =0)                
            if ktype == 2 :
                wnd = wnd.replace(minute= wnd.minute- wnd.minute % 5,second=0)
                if wnd.hour == 9 and wnd.minute==30 :
                    wnd = wnd.replace(minute = 35 ,second =0)
                if wnd.hour == 13 and wnd.minute==00 :
                    wnd = wnd.replace(minute = 5 ,second =0)
            for index in range( len(indicator) ):
                if  wnd == indicator[-index-1]['date'] :
                    break
            print ktype,windcode,Windcode_Dict[windcode]['warning_date'],Windcode_Dict[windcode]['warning_status'],-index-1
            
            if ktype == 2 :
                for i in range(-index,0):
                    temp = {'diff':Windcode_Dict[windcode]['tech'+ktypestr]['diff'][i-1:i+1], 
                            'dea' :Windcode_Dict[windcode]['tech'+ktypestr]['dea'][i-1:i+1]}
                    if Windcode_Dict[windcode]['tech'+ktypestr]['diff'][i] > 0 or Windcode_Dict[windcode]['tech'+ktypestr]['dea'][i]>0 :
                        continue
                    if i==-1:
                        continue
                    if Windcode_Dict[windcode]['warning_status'] != 40 and FourIndicator.GoldenDead(temp) == 4 :                        
                        Windcode_Dict[windcode].update({'warning_status':40,'warning_ktype':ktype,'warning_date':Windcode_Dict[windcode][ktypestr][i]['date']})
                        break
            if ktype == 5 :
                for i in range(-index,0):
                    # print -index-1,i,indicator[i]['date']
                    if Windcode_Dict[windcode]['tech'+ktypestr]['diff'][i] > 0 or Windcode_Dict[windcode]['tech'+ktypestr]['dea'][i]>0 :
                        continue
                    if i==-1:
                        continue
                    if Windcode_Dict[windcode]['tech'+ktypestr]['diff'][i] > Windcode_Dict[windcode]['tech'+ktypestr]['dea'][i] :
                        if Windcode_Dict[windcode]['tech'+ktypestr]['diff'][i-1] < Windcode_Dict[windcode]['tech'+ktypestr]['dea'][i-1] :
                            if Windcode_Dict[windcode]['tech'+ktypestr].get('gdbb') is None:
                                Windcode_Dict[windcode]['tech'+ktypestr].update( {'gdbb':[Windcode_Dict[windcode][ktypestr][i]['date']]} )
                            else:
                                Windcode_Dict[windcode]['tech'+ktypestr]['gdbb'].append(Windcode_Dict[windcode][ktypestr][i]['date'])
                    if Windcode_Dict[windcode]['tech'+ktypestr].get('gdbb') is None:
                        continue
                    print windcode,Windcode_Dict[windcode]['tech'+ktypestr]['gdbb'],i,Windcode_Dict[windcode]['warning_status']
                    if Windcode_Dict[windcode]['warning_status'] == 40 :
                        continue
                    gl = len(Windcode_Dict[windcode]['tech'+ktypestr]['gdbb'])
                    
                    if gl==1 and Windcode_Dict[windcode]['is_strong'] :
                        ws = 40
                        Windcode_Dict[windcode].update({'warning_status':ws,'warning_ktype':ktype,'warning_date':Windcode_Dict[windcode][ktypestr][i]['date']})               
                        break
                    if gl==2 :
                        if Windcode_Dict[windcode]['type']==6 or Windcode_Dict[windcode]['is_jiben'] or Windcode_Dict[windcode]['is_ready'] or Windcode_Dict[windcode]['is_mf'] :
                            ws = 40
                            Windcode_Dict[windcode].update({'warning_status':ws,'warning_ktype':ktype,'warning_date':Windcode_Dict[windcode][ktypestr][i]['date']})
                            break
        print windcode,Windcode_Dict[windcode]['is_strong'],Windcode_Dict[windcode]['warning_status']
        if Windcode_Dict[windcode]['warning_status']==40 :
            f.write(" code %s date %s type %d Signal %d ktype %d\n" %(windcode,Windcode_Dict[windcode]['warning_date'].strftime("%Y-%m-%d %H:%M:%S")
                ,Windcode_Dict[windcode]['type'] ,Windcode_Dict[windcode]['warning_status']/10,Windcode_Dict[windcode]['warning_ktype']))
 
 #    for windcode in  Windcode_Dict  :
 #        if Windcode_Dict[windcode]['warning_status']==10 or Windcode_Dict[windcode]['warning_status']==40 :
 #            # cursor.execute(sql,{
 #            #     'sid':100,
 #            #     'w':Windcode_Dict[windcode]['warning_status']/10,
 #            #     's':Windcode_Dict[windcode]['type'],
 #            #     'k':Windcode_Dict[windcode]['warning_ktype'],
 #            #     'name':Windcode_Dict[windcode]['name'],
 #            #     'code':windcode,
 #            #     'wdate':Windcode_Dict[windcode]['warning_date']    
 #            #     })
 #            f.write(" code %s date %s type %d Signal %d ktype %d\n" %(windcode,Windcode_Dict[windcode]['warning_date'].strftime("%Y-%m-%d %H:%M:%S")
 # ,Windcode_Dict[windcode]['type'] ,Windcode_Dict[windcode]['warning_status']/10,Windcode_Dict[windcode]['warning_ktype']))
 #            print windcode,Windcode_Dict[windcode]['warning_date'] 
    # Connector.commit() 
    f.close()
 
def WarningMessageAll():
    WarningMessage2(True)
    WarningMessage(5,True)
    WarningMessage(6,True)

def WarningMessage_SendEnter():
   
    cursor = Connector.cursor()
    cursor_p=Connector.cursor()
    sql = "select id,wtype,stype,ktype,windcode,name,date from stocks_warning where wtype=1"
    sql_p = "select close from stocks_history_minutes where windcode=%(windcode)s and ktype=%(ktype)s and date=%(date)s"
    cursor.execute(sql)
    stocksWM = []
    for num,ws,stype,ktype,windcode,name,wdate in cursor:
        cursor_p.execute(sql_p,{'windcode':windcode,'ktype':ktype,'date':wdate})
        stocksWM.append( {
            'id': num,
            'windcode': windcode,
            'name' : name,
            'warning_status': ws,
            'type' : stype,
            'update_date' : wdate,

            'price'   : cursor_p.fetchone()[0] ,
            
            'is_strong': Windcode_Dict[windcode]['is_strong'],
            'is_ready': Windcode_Dict[windcode]['is_ready'],
            'is_bc': Windcode_Dict[windcode]['is_bc'],
            'is_bv': Windcode_Dict[windcode]['is_bv'],
            'is_jishu': Windcode_Dict[windcode]['is_jishu'],
            'is_jiben': Windcode_Dict[windcode]['is_jiben'],
            'is_mf': Windcode_Dict[windcode]['is_mf'],
            'is_bs': Windcode_Dict[windcode]['is_bs'],
            'is_br': Windcode_Dict[windcode]['is_br'],
            'stars': Windcode_Dict[windcode]['stars'],
        })
    cursor.close()
    cursor_p.close()
    with app.app_context():
        SendWarningMessage(  [marshal(task, stock_wm_str_fields) for task in stocksWM] )      

def WarningMessage2(LastFlag=False):
    ktype = 2 
    ktypestr = str(ktype)
    current_time = datetime.now()
    current_time = current_time.replace(microsecond = 0)
    print "WarningMessage2",ktype,current_time
    stocksWM= []
    num = 0
    Connector = mysql.connector.connect(**Config.config)
    sql = "Insert into stocks_warning(id,wtype,stype,ktype,windcode,name,price,date) values(%(sid)s,%(wtype)s,%(stype)s,%(ktype)s,%(code)s,%(name)s,%(price)s,%(wdate)s)"
    cursor = Connector.cursor()
    for windcode in Windcode_Dict:
        if Windcode_Dict[windcode]['is_bc'] == False:
            continue
        if LastFlag:            
            wsi=Slice.SliceMinute_WindPy(windcode,WindPy,current_time.replace(hour=14,minute=56,second=0),current_time.replace(hour=15,minute=1,second=0))
            temp={'date':current_time.replace(hour=15,minute=0,second=0),
                'open': 0 ,
                'high':  0,
                'low':   0,
                'close': 0 ,
                'volume':0 ,}
            if wsi is not None:
                temp.update(wsi)
        else:
            wsi = WindPy.wsq(windcode, "rt_time,rt_open,rt_high,rt_low,rt_last,rt_vol")
            if not Stock.CheckWindData(wsi,Message='WSQ Error') :
                temp = {
                        'date':  current_time,
                        'open': 0 ,
                        'high':  0,
                        'low':   0,
                        'close': 0 ,
                        'volume':0 ,
                    }        
            else:
                # if windcode=='002019.SZ' :
                #     print windcode,wsi.Data[0][0],int(wsi.Data[0][0] )
                dtime = int( wsi.Data[0][0 ] )
                if dtime > 100:                
                    dtime = datetime.strptime(str(dtime),'%H%M%S')
                    temp = {
                        'date':  current_time.replace(second=0 , microsecond = 0,
                            hour= dtime.hour, minute=dtime.minute - dtime.minute % 5),
                        'open':  wsi.Data[1][0] ,
                        'high':  wsi.Data[2][0],
                        'low':   wsi.Data[3][0],
                        'close': wsi.Data[4][0] ,
                        'volume':wsi.Data[5][0] ,
                    }
                else:
                    temp = {
                        'date':  current_time,
                        'open': 0 ,
                        'high':  0,
                        'low':   0,
                        'close': 0 ,
                        'volume':0 ,
                    }
        Windcode_Dict[windcode][ktypestr].append(temp)
        Windcode_Dict[windcode]['wsq'+ktypestr] += 1
    for windcode in Windcode_Dict:        
        dtime =  Windcode_Dict[windcode][ktypestr][-1]['date']
        if dtime.date() != current_time.date() :
            continue
        if dtime == current_time and Windcode_Dict[windcode][ktypestr][-1]['close']==0 :
            continue
        if Windcode_Dict[windcode]['wsq'+ktypestr] >1:
            temp=Windcode_Dict[windcode][ktypestr][-2]
            wsi=Slice.SliceMinute_WindPy(windcode,WindPy,temp['date']-timedelta(minutes=9),temp['date']-timedelta(minutes=4))
            if wsi is not None:
                temp.update(wsi)
                last=Windcode_Dict[windcode][ktypestr].pop()
                Windcode_Dict[windcode][ktypestr].pop()
                Windcode_Dict[windcode][ktypestr].append(temp)
                Windcode_Dict[windcode][ktypestr].append(last)
        # temp = Windcode_Dict[windcode]['8'].pop()
        # W= Windcode_Dict[windcode][ktypestr][ -Windcode_Dict[windcode]['wsq'+ktypestr]: ]
        # if temp['open'] !=0 and temp['close'] !=0 :
        #     W.insert(0,temp)
        # temp = {
        #         'date':W[-1]['date'],
        #         'open':W[0 ]['open'],
        #         'high':max([ x['high'] for x in W ]),
        #         'low' :min([ x['low']  for x in W ]),
        #         'close':W[-1]['close'],
        # }
        # Windcode_Dict[windcode]['8'].append(temp)
        # Windcode_Dict[windcode]['tech8'] = Stock.StocksTech(Windcode_Dict[windcode]['8'])
        # if Windcode_Dict[windcode]['tech8']['fast_k'][-1] <=Windcode_Dict[windcode]['tech8']['fast_d'][-1]:
        #     continue
        Windcode_Dict[windcode]['tech'+ktypestr] = Stock.StocksTech(Windcode_Dict[windcode][ktypestr]) 
        if Windcode_Dict[windcode]['tech'+ktypestr]['diff'][-1]>0 or Windcode_Dict[windcode]['tech'+ktypestr]['dea'][-1] >0:
            continue
        ws = 0
        # if Windcode_Dict[windcode]['warning_status']==0 and Windcode_Dict[windcode]['tech'+ktypestr]['fast_j'][-1] < 5:
        #     ws = 1
        #     Windcode_Dict[windcode].update({'warning_status':ws,'warning_ktype':ktype})
        if Windcode_Dict[windcode]['warning_status']==1 and Windcode_Dict[windcode]['warning_ktype']==6 and FourIndicator.GoldenDead(Windcode_Dict[windcode]['tech'+ktypestr]) == 4 :
            ws = 4
            Windcode_Dict[windcode].update({'warning_status':ws,'warning_ktype':ktype,'warning_date':dtime})
        if ws ==0 :
            continue
        
        
        close =  Windcode_Dict[windcode][ktypestr][-1]['close']
        if close ==0 :
            continue
        stocksWM.append( {
            'id': num,
            'windcode': windcode,
            'name' : Windcode_Dict[windcode]['name'],
            'warning_status': ws,
            'type' : Windcode_Dict[windcode]['type'],
            'update_date' : Windcode_Dict[windcode]['warning_date'].strftime("%Y-%m-%d %H:%M:%S"),

            'price'   : str(close) ,
            
            'is_strong': Windcode_Dict[windcode]['is_strong'],
            'is_ready': Windcode_Dict[windcode]['is_ready'],
            'is_bc': Windcode_Dict[windcode]['is_bc'],
            'is_bv': Windcode_Dict[windcode]['is_bv'],
            'is_jishu': Windcode_Dict[windcode]['is_jishu'],
            'is_jiben': Windcode_Dict[windcode]['is_jiben'],
            'is_mf': Windcode_Dict[windcode]['is_mf'],
            'is_bs': Windcode_Dict[windcode]['is_bs'],
            'is_br': Windcode_Dict[windcode]['is_br'],
            'stars': Windcode_Dict[windcode]['stars'],
        })
         
        cursor.execute(sql,{
            'sid':num,
            'wtype':ws,
            'stype':Windcode_Dict[windcode]['type'],
            'ktype':ktype,
            'name':Windcode_Dict[windcode]['name'],
            'code':windcode,
            'price': close,
            'wdate':dtime,    
            })
        num = num  + 1 
    Connector.commit()
    cursor.close()
    
    
    with app.app_context():
        SendWarningMessage(  [marshal(task, stock_wm_str_fields) for task in stocksWM] )   

def WarningMessage(ktype,LastFlag=False):
    if ktype==5:
        BarSize= 30
    if ktype==6:
        BarSize=60
    ktypestr = str(ktype)
    current_time = datetime.now()
    print "WarningMessage",ktype,current_time
    stocksWM= []
    num = 0
    Connector = mysql.connector.connect(**Config.config)
    sql = "Insert into stocks_warning(id,wtype,stype,ktype,windcode,name,price,date) values(%(sid)s,%(wtype)s,%(stype)s,%(ktype)s,%(code)s,%(name)s,%(price)s,%(wdate)s)"
    cursor = Connector.cursor()
    for windcode in Windcode_Dict:
        # if Windcode_Dict[windcode]['type'] == 2:
        #     continue
        if LastFlag:            
            wsi=Slice.SliceMinute_WindPy(windcode,WindPy,current_time.replace(hour=14,minute=56,second=0),current_time.replace(hour=15,minute=1,second=0))
            temp={'date':current_time.replace(hour=15,minute=0,second=0),
                'open': 0 ,
                'high':  0,
                'low':   0,
                'close': 0 ,
                'volume':0 ,}
            if wsi is not None:
                temp.update(wsi)
        else:
            wsi = WindPy.wsq(windcode, "rt_time,rt_open,rt_high,rt_low,rt_last,rt_vol")
            if not Stock.CheckWindData(wsi,Message='WSQ Error') :
                temp={'date':current_time,
                    'open': 0 ,
                    'high':  0,
                    'low':   0,
                    'close': 0 ,
                    'volume':0 ,} 
            else:   
                dtime = int( wsi.Data[0][0 ] )
                if dtime > 100:                
                    dtime = datetime.strptime(str(dtime),'%H%M%S')
                    temp = {
                        'date':  current_time.replace(second=0 , microsecond = 0,
                            hour= dtime.hour, minute=dtime.minute - dtime.minute % 5),
                        'open':  wsi.Data[1][0] ,
                        'high':  wsi.Data[2][0],
                        'low':   wsi.Data[3][0],
                        'close': wsi.Data[4][0] ,
                        'volume':wsi.Data[5][0] ,
                    }
                else:
                    temp = {
                        'date':  current_time,
                        'open': 0 ,
                        'high':  0,
                        'low':   0,
                        'close': 0 ,
                        'volume':0 ,
                    }  
                
        Windcode_Dict[windcode][ktypestr].append(temp)
        Windcode_Dict[windcode]['wsq'+ktypestr] += 1
    for windcode in Windcode_Dict:        
        dtime = Windcode_Dict[windcode][ktypestr][-1]['date'] 
        if dtime.date() != current_time.date() :
            continue
        if Windcode_Dict[windcode]['wsq'+ktypestr] >1:
            temp=Windcode_Dict[windcode][ktypestr][-2]
            wsi=Slice.SliceMinute_WindPy(windcode,WindPy,temp['date']-timedelta(minutes=2*BarSize-1),temp['date']-timedelta(minutes=BarSize-1))
            if wsi is not None:
                temp.update(wsi)
                last=Windcode_Dict[windcode][ktypestr].pop()
                Windcode_Dict[windcode][ktypestr].pop()
                Windcode_Dict[windcode][ktypestr].append(temp)
                Windcode_Dict[windcode][ktypestr].append(last)
        temp = Windcode_Dict[windcode]['8'].pop()
        W= Windcode_Dict[windcode][ktypestr][ -Windcode_Dict[windcode]['wsq'+ktypestr]: ]
        if temp['open'] !=0 and temp['close'] !=0 :
            W.insert(0,temp)
        temp = {
                'date':W[-1]['date'],
                'open':W[0 ]['open'],
                'high':max([ x['high'] for x in W ]),
                'low' :min([ x['low']  for x in W ]),
                'close':W[-1]['close'],
        }
        Windcode_Dict[windcode]['8'].append(temp) 
        Windcode_Dict[windcode]['tech8'] = Stock.StocksTech(Windcode_Dict[windcode]['8'])
        if Windcode_Dict[windcode]['is_jishu'] and Windcode_Dict[windcode]['tech8']['fast_k'][-1] <=Windcode_Dict[windcode]['tech8']['fast_d'][-1]:
            continue
        Windcode_Dict[windcode]['tech'+ktypestr] = Stock.StocksTech(Windcode_Dict[windcode][ktypestr]) 
        if Windcode_Dict[windcode]['tech'+ktypestr]['diff'][-1]>0 or Windcode_Dict[windcode]['tech'+ktypestr]['dea'][-1] >0:
            continue
        ws = 0
        if ktype == 6 :
            if Windcode_Dict[windcode]['is_bc'] :
                if Windcode_Dict[windcode]['warning_status']==0 and Windcode_Dict[windcode]['tech'+ktypestr]['fast_j'][-1] < 5:
                    ws = 1
                    Windcode_Dict[windcode].update({'warning_status':ws,'type':2,'warning_ktype':ktype,'warning_date':dtime,})
                else:
                    continue
            else:
                if Windcode_Dict[windcode]['warning_status']==0 and Windcode_Dict[windcode]['tech'+ktypestr]['slow_j'][-1] < 5:
                    ws = 1
                    Windcode_Dict[windcode].update({'warning_status':ws,'warning_ktype':ktype,'warning_date':dtime,})
                else:
                    continue
        if ktype ==  5 :
            if Windcode_Dict[windcode]['warning_status'] !=1 or Windcode_Dict[windcode]['warning_status']==4:
                continue
            if FourIndicator.GoldenDead(Windcode_Dict[windcode]['tech'+ktypestr]) == 4 :
                if Windcode_Dict[windcode]['tech'+ktypestr].get('gdbb') is None:
                    Windcode_Dict[windcode]['tech'+ktypestr].update( {'gdbb':[4]} )
                else:
                    Windcode_Dict[windcode]['tech'+ktypestr]['gdbb'].append(4)
            else:
                continue
            if Windcode_Dict[windcode]['tech'+ktypestr].get('gdbb') is None:
                continue
            gl = len(Windcode_Dict[windcode]['tech'+ktypestr]['gdbb'])
            if gl==1 and Windcode_Dict[windcode]['is_strong'] :
                ws = 4
                Windcode_Dict[windcode].update({'warning_status':ws,'type':0,'warning_ktype':ktype,'warning_date':dtime,})               
            if gl==2 :
                if Windcode_Dict[windcode]['type']==6 or Windcode_Dict[windcode]['is_jiben'] or Windcode_Dict[windcode]['is_ready'] or Windcode_Dict[windcode]['is_mf'] :
                    ws = 4
                    Windcode_Dict[windcode].update({'warning_status':ws,'warning_ktype':ktype,'warning_date':dtime,})
        if ws == 0 :            
            continue

               
        close =  Windcode_Dict[windcode][ktypestr][-1]['close']
        if close==0:
            continue
        stocksWM.append( {
            'id': num,
            'windcode': windcode,
            'name' : Windcode_Dict[windcode]['name'],
            'warning_status': ws,
            'type' : Windcode_Dict[windcode]['type'],
            'update_date' : Windcode_Dict[windcode]['warning_date'].strftime("%Y-%m-%d %H:%M:%S"),
            'price'   : str(close) ,            
            'is_strong': Windcode_Dict[windcode]['is_strong'],
            'is_ready': Windcode_Dict[windcode]['is_ready'],
            'is_bc': Windcode_Dict[windcode]['is_bc'],
            'is_bv': Windcode_Dict[windcode]['is_bv'],
            'is_jishu': Windcode_Dict[windcode]['is_jishu'],
            'is_jiben': Windcode_Dict[windcode]['is_jiben'],
            'is_mf': Windcode_Dict[windcode]['is_mf'],
            'is_bs': Windcode_Dict[windcode]['is_bs'],
            'is_br': Windcode_Dict[windcode]['is_br'],
            'stars': Windcode_Dict[windcode]['stars'],
        })
         
        cursor.execute(sql,{
            'sid':num,
            'wtype':ws,
            'stype':Windcode_Dict[windcode]['type'],
            'ktype':ktype,
            'name':Windcode_Dict[windcode]['name'],
            'code':windcode,
            'price': close,
            'wdate':dtime,    
            })
        num = num + 1
    Connector.commit()
    cursor.close()
    
    with app.app_context():
        SendWarningMessage(  [marshal(task, stock_wm_str_fields) for task in stocksWM] )   

def WarningMessage_Test(ktype):
    ktypestr = str(ktype)
    current_time = datetime.now()
    print "WarningMessage2",ktype,current_time
    stocksWM= []
    num = 0
    Connector = mysql.connector.connect(**Config.config)
    sql = "Insert into stocks_warning(id,wtype,stype,ktype,windcode,name,date) values(%(sid)s,%(wtype)s,%(stype)s,%(ktype)s,%(code)s,%(name)s,%(wdate)s)"
    cursor = Connector.cursor()
    for windcode in Windcode_Dict:
        if Windcode_Dict[windcode]['type'] != 2:
            continue
        wsi = WindPy.wsq(windcode, "rt_time,rt_open,rt_high,rt_low,rt_last,rt_vol")
        if not Stock.CheckWindData(wsi,Message='WSQ Error') :
            temp = {
                    'date':  current_time,
                    'open': 0 ,
                    'high':  0,
                    'low':   0,
                    'close': 0 ,
                    'volume':0 ,
                }        
        else:
            # print wsi.Data[0][0],int(wsi.Data[0][0] )
            dtime = int( wsi.Data[0][0 ] )
            if dtime > 100:                
                dtime = datetime.strptime(str(dtime),'%H%M%S')
                temp = {
                    'date':  current_time.replace(second=0 , microsecond = 0,
                        hour= dtime.hour, minute=dtime.minute - dtime.minute % 5),
                    'open':  wsi.Data[1][0] ,
                    'high':  wsi.Data[2][0],
                    'low':   wsi.Data[3][0],
                    'close': wsi.Data[4][0] ,
                    'volume':wsi.Data[5][0] ,
                }
            else:
                temp = {
                    'date':  current_time,
                    'open': 0 ,
                    'high':  0,
                    'low':   0,
                    'close': 0 ,
                    'volume':0 ,
                }
        Windcode_Dict[windcode][ktypestr].append(temp)
        Windcode_Dict[windcode]['wsq'+ktypestr] += 1
    for windcode in Windcode_Dict:        
        dtime =  Windcode_Dict[windcode][ktypestr][-1]['date']
        if dtime.date() != current_time.date() :
            continue
        if dtime == current_time and Windcode_Dict[windcode][ktypestr][-1]['close']==0 :
            continue
        if Windcode_Dict[windcode]['wsq'+ktypestr] >1:
            temp=Windcode_Dict[windcode][ktypestr][-2]

            wsi=Slice.SliceMinute_WindPy(windcode,WindPy,temp['date']-timedelta(minutes=9),temp['date']-timedelta(minutes=4))
            if wsi is not None:
                temp.update(wsi)
                last=Windcode_Dict[windcode][ktypestr].pop()
                Windcode_Dict[windcode][ktypestr].pop()
                Windcode_Dict[windcode][ktypestr].append(temp)
                Windcode_Dict[windcode][ktypestr].append(last)
        temp = Windcode_Dict[windcode]['8'].pop()
        W= Windcode_Dict[windcode][ktypestr][ -Windcode_Dict[windcode]['wsq'+ktypestr]: ]
        W.insert(0,temp)
        temp = {
                'date':W[-1]['date'],
                'open':W[0 ]['open'],
                'high':max([ x['high'] for x in W ]),
                'low' :min([ x['low']  for x in W ]),
                'close':W[-1]['close'],
        }
        Windcode_Dict[windcode]['8'].append(temp)
        Windcode_Dict[windcode]['tech8'] = Stock.StocksTech(Windcode_Dict[windcode]['8'])
        if Windcode_Dict[windcode]['tech8']['fast_k'][-1] <=Windcode_Dict[windcode]['tech8']['fast_d'][-1]:
            continue
        Windcode_Dict[windcode]['tech'+ktypestr] = Stock.StocksTech(Windcode_Dict[windcode][ktypestr]) 
        if Windcode_Dict[windcode]['tech'+ktypestr]['diff'][-1]>0 or Windcode_Dict[windcode]['tech'+ktypestr]['dea'][-1] >0:
            continue
        ws = 0
        
        
        close =  Windcode_Dict[windcode][ktypestr][-1]['close']
        stocksWM.append( {
            'id': num,
            'windcode': windcode,
            'name' : Windcode_Dict[windcode]['name'],
            'warning_status': ws,
            'type' : Windcode_Dict[windcode]['type'],
            'update_date' : dtime.strftime("%Y-%m-%d %H:%M:%S"),

            'price'   : str(close) ,
            
            'is_strong': Windcode_Dict[windcode]['is_strong'],
            'is_ready': Windcode_Dict[windcode]['is_ready'],
            'is_bc': Windcode_Dict[windcode]['is_bc'],
            'is_bv': Windcode_Dict[windcode]['is_bv'],
            'is_jishu': Windcode_Dict[windcode]['is_jishu'],
            'is_jiben': Windcode_Dict[windcode]['is_jiben'],
            'is_mf': Windcode_Dict[windcode]['is_mf'],
            'is_bs': Windcode_Dict[windcode]['is_bs'],
            'is_br': Windcode_Dict[windcode]['is_br'],
            'stars': Windcode_Dict[windcode]['stars'],
        })
         
        
         
        if num > 2:
            break
        

        num = num  + 1 
    # Connector.commit()
    cursor.close()
    print stocksWM
    # with app.app_context():
    #     SendWarningMessage(  [marshal(task, stock_wm_str_fields) for task in stocksWM] )  

import json
def SendWarningMessage(message):
    url = "https://www.hanzhendata.com/ihanzhendata/warning/buyorder"

    headers = {
        'content-type': "application/json",
        'cache-control': "no-cache",
    }
    print message
    if len(message)==0 or message is None :
        return
    response = requests.request("POST", url, data=json.dumps(message), headers=headers)
    if response.status_code == 200 :
        print response.status_code,response.json()
    requests.request("POST", url, headers={'Connection':'close'})
def RefreshRequest():
    url = "https://www.hanzhendata.com/ihanzhendata/stockpool/flushdata"
    print url
    response = requests.request("GET", url)
    if response.status_code == 200 :
        print 'RefreshRequest ',response.status_code,response.json()
    requests.request("GET", url, headers={'Connection':'close'})
  
import logging
logging.basicConfig(level=logging.INFO, 
        format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s', 
        datefmt='%Y-%m-%d %H:%M:%S',  filename='log1.txt',  filemode='a')
clients = set()
def update_message(message):
    for c in clients:
        c.write_message(message)

class WebSocket(WebSocketHandler):
    
    def open(self):
        print("Socket opened.")
        clients.add(self)

    def on_message(self, message):
        self.write_message("Received: " + message)
        
        print("Received message: " + message)
    
    

    def on_close(self):
        # for c in clients:

        clients.remove(self)
        print("Socket closed.")



# scheduler = BackgroundScheduler()
scheduler = TornadoScheduler()
Debug = True
# Debug = False
if __name__ == '__main__':
    # global WindPy

    
    WindPy.start()
    job_init  = scheduler.add_job(func=Init, trigger='cron', day_of_week='0-6',hour='5,13,21' ,replace_existing=True)
    job_sinit = scheduler.add_job(func=Sche_Init, trigger='cron', day_of_week='0-6',hour='6',minute='50',replace_existing=True)
    job_minit = scheduler.add_job(func=OHLC_Update, trigger='cron', day_of_week='0-6',hour='9',minute='32',replace_existing=True)
    # job_minit2 = scheduler.add_job(func=OHLC_Update, trigger='cron', day_of_week='0-6',hour='13',minute='5',replace_existing=True)
    scheduler._logger = logging
    scheduler.start()    
    
    if Debug:
                
        # EventDrivenStocks()
        Days_Init()
        WarningMessage_SignalRebuild()
        
        # WarningMessage_Init()
        # WarningMessage_Test(5)
        # RefreshRequest()
        
        WindPy.stop()
        import sys
        sys.exit(0)
    
    Init()    
    if datetime.now().hour>=6 :
        Sche_Init()
    # #WarningMessage_SendEnter()

    
    print 'Start server-------------\n'
    # app.run(host='0.0.0.0' ,port=3307,debug=True, use_evalex=False, threaded=True)
    
    container = WSGIContainer(app)
    server = Application([
        (r'/websocket/', WebSocket),
        (r'.*', FallbackHandler, dict(fallback=container))
    ])
    server.listen(3307)
    IOLoop.instance().start()

