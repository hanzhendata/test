#!flask/bin/python

"""Alternative version of the ToDo RESTful server implemented using the
Flask-RESTful extension."""

from flask import Flask, jsonify, abort, make_response
from flask.ext.restful import Api, Resource, reqparse, fields, marshal
from flask.ext.httpauth import HTTPBasicAuth

import mysql.connector

from flask import current_app, json, request
from datetime import date, datetime, timedelta

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
    # 'intro': fields.String,
    # 'province': fields.String,
    # 'city': fields.String,
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

class StockBasicListAPI(Resource):
    # decorators = [auth.login_required]

    def __init__(self):
        super(StockBasicListAPI, self).__init__()

    def get(self,offset):
        tasks = PageContent(stocksBasicSelect ,offset)
        return jsonify(data=[ marshal(task, stock_fields) for task in tasks ])
        

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

stock_label_fields = {    
    'windcode': fields.String,
    'is_jishu': fields.Integer(default=0),
    'is_jiben': fields.Integer(default=0),
    'is_mf': fields.Integer(default=0),
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
            task = [task for task in stocksLabel if task['windcode'] == code]
            if len(task)==0 :
                continue
            tasks.append(task[0])
        return jsonify(data=[marshal(task, stock_label_fields) for task in tasks])
        # return {'data': [marshal(task, stock_select_fields) for task in stocksReady]}

stock_index_fields = {
    'windcode': fields.String,
    'type': fields.String,
    'num' : fields.Integer(default=0),
    'data': fields.List( fields.Float ),
}

class  StockIndexAPI(Resource):
    # decorators = [auth.login_required]
    def __init__(self):
        super(StockIndexAPI, self).__init__()

    def get(self):
        try:
            limit =  int(request.args.get('num',30) )
        except Exception, e:
            limit = 30
        
        index_type =  request.args.get('type','').strip().upper()
        if limit > 30 or limit<1:
            limit = 30
        task = {}
        if index_type=='FUND' or index_type=='STRONG' or index_type=='READY':
            task['windcode'] = index_type.title() +'Index'
            task['type'] = 'Index'
            task['num' ] = limit
            task['data'] = stocksIndex[ task['windcode'] ] [limit*-1:]
        else :
            task['windcode'] = (request.args.get('windcode','')).strip().upper()
            task['type'] = 'close'
            task['num'] = limit
            if task['windcode'] in stocksClose:
                task['data'] = stocksClose[ task['windcode'] ] [limit*-1:]
            else:
                task['data'] = []
        return jsonify(data=(marshal(task, stock_index_fields) ) )
        # return {'data': [marshal(task, stock_select_fields) for task in stocksReady]}

stock_mainforce_fields = {
    'windcode' : fields.String,
    'name' : fields.String,
    'date' : fields.DateTime(dt_format='iso8601'),
    'type' : fields.String,
    'ratio': fields.Float,
    'cost' : fields.Float,
    'target': fields.Float,
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

api.add_resource(StockListAPI,        '/api/v1.0/stocks/', endpoint='stocks')
api.add_resource(StockAPI,            '/api/v1.0/stocks/<string:code>', endpoint='stock')
api.add_resource(StockBoardListAPI,   '/api/v1.0/stocksboard/', endpoint='stocksboard')
api.add_resource(StockBoardAPI,       '/api/v1.0/stocksboard/<string:code>', endpoint='stockboard')
api.add_resource(StockBoardStrongAPI, '/api/v1.0/stocksboardstrong/<int:offset>', endpoint='stockboardstrong')
api.add_resource(StockBoardReadyAPI,  '/api/v1.0/stocksboardready/<int:offset>', endpoint='stockboardready')
api.add_resource(StockStrongAPI,      '/api/v1.0/stocksstrong/<int:offset>', endpoint='stockstrong')
api.add_resource(StockReadyAPI,       '/api/v1.0/stocksready/<int:offset>', endpoint='stockready')
api.add_resource(StockBreakCentralAPI,'/api/v1.0/stocksbreakcentral/<int:offset>', endpoint='stockbreakcentral')
api.add_resource(StockBasicAPI,       '/api/v1.0/stocksbasic/<string:code>', endpoint='stockbasic')
api.add_resource(StockLabelAPI,       '/api/v1.0/stockslabel/', endpoint='stocklabel')
api.add_resource(StockBasicListAPI,   '/api/v1.0/stocksbasiclist/<int:offset>', endpoint='stockbasiclist')
api.add_resource(StockIndexAPI,       '/api/v1.0/stocksindex/', endpoint='stockindex')
api.add_resource(StockMainForceAPI,   '/api/v1.0/stocksmainforce/<int:offset>', endpoint='stockmainforce')


stocks = []
stocksBoard = []
stocksBoardStrong = []
stocksBoardReady = []
stocksStrong = []
stocksReady = []
stocksBreakCentral = []
stocksBasic = []
stocksLabel = []
stocksBasicSelect = []
stocksIndex = {}
stocksClose = {}
stocksMainForce = []

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

def GetStocksSType(Connector,stype,ktype):
    cursor = Connector.cursor()
    sql = "select id,name,windcode,pct_chg,cap from stocks_sr where stype=%(stype)s and ktype=%(ktype)s order by id"
    
    cursor.execute(sql,{'stype':stype,'ktype':ktype})
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
        or_lastyear = None
        sql = "select content from stocks_basic where windcode=%(code)s and btype=12 order by quarter desc limit 1"
        cursor.execute(sql,{'code':stock['windcode'],})
        for content in cursor:
            or_lastyear = content[0]
        if or_lastyear is None:
            or_lastyear = 0 
       
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


def Init():
    global stocks
    global stocksBoard
    global stocksBoardStrong
    global stocksBoardReady
    global stocksStrong
    global stocksReady
    global stocksBreakCentral
    global stocksBasic
    global stocksLabel
    global stocksBasicSelect
    global stocksIndex
    global stocksClose
    global stocksMainForce
    cnx = mysql.connector.connect(**config)
    if len(stocks)==0:
        sql = "select  windcode, name, date from stocks_code"
        cursor = cnx.cursor()
        
        cursor.execute(sql)
        for windcode, name, update_date in cursor:
            stocks.append({    
                'windcode': windcode,
                'name' : name,
                'type' : 'A',
                })

        cursor.close()
    if len(stocksBasicSelect)==0 :
        sql = "select  windcode, name, date from stocks_code"
        cursor = cnx.cursor()
        cursor_stars = cnx.cursor()
        cursor.execute(sql)
        index = 0 
        for windcode, name, update_date in cursor:
            sql_stars = "select content from stocks_basic where windcode=%(code)s and btype=9 order by quarter desc limit 1"
            cursor_stars.execute(sql_stars,{'code':windcode,})
            if cursor_stars.rowcount == 0 or cursor_stars.fetchone()[0] <=5 :
                continue
            
            stocksBasicSelect.append({ 
                'id'  : index  ,
                'windcode': windcode,
                'name' : name,
                'type' : 'stars 5',
                })
            index += 1
        cursor_stars.close()
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
    if len(stocksIndex) ==0 :
        cursor = cnx.cursor()
        sql = "select num,typename from stocks_index where typename=%(type)s order by date desc limit 30"
        tl = ['FundIndex','StrongIndex','ReadyIndex']
        for typename in tl:
            cursor.execute(sql,{'type':typename,})
            stocksIndex[ typename ] = []
            for bnum,btn in cursor:
                stocksIndex[ typename ].insert(0,bnum)
    if len(stocksClose) == 0 :
        cursor = cnx.cursor()
        sql = "select close,date from stocks_history_days where ktype=7 and windcode=%(code)s order by date desc limit 30"
        zs = ["000001.SH",'399300.SZ',"399006.SZ","150051.SZ","399005.SZ","399905.SZ"]
        for windcode in zs:
            cursor.execute(sql,{'code':windcode,})
            stocksClose[ windcode ] = []
            for sclose,sdate in cursor:
                stocksClose[ windcode ].insert(0,sclose)
        for board in stocksBoard:
            windcode = board['windcode']
            cursor.execute(sql,{'code':windcode,})
            stocksClose[ windcode ] = []
            for sclose,sdate in cursor:
                stocksClose[ windcode ].insert(0,sclose)   
    if len(stocksMainForce) == 0 :
        cursor = cnx.cursor()
        sql = "select date from stocks_mainforce order by date desc limit 1 "
        cursor.execute(sql)
        if cursor.rowcount > 0 :
            date = cursor.fetchone()[0]
            sql = "select windcode,type,ratio,cost,target from stocks_mainforce where type=%(type)s and date = %(date)s "
            tl = ['new','inc','equal'] 
            num = 0 
            for typename in tl:
                cursor.execute(sql,{'date':date,'type':typename,})            
                for code,typename,ratio,cost,target in cursor:
                    name = [x['name'] for x in stocks if x['windcode'] == code ]
                    stocksMainForce.append({
                        'windcode' : code,
                        'name'     : name[0],
                        'id' : num ,
                        'date' : datetime.combine(date,datetime.min.time()),
                        'type' : typename,
                        'ratio': ratio,
                        'cost' : cost,
                        'target': target, 
                    })  
                    num += 1   
    if len(stocksBoardStrong) == 0 :
        stocksBoardStrong=GetStocksBoard(cnx,isStrong=True,isReady=False)
    if len(stocksBoardReady) == 0 :
        stocksBoardReady =GetStocksBoard(cnx,isStrong=False,isReady=True)
    if len(stocksStrong) == 0 :
        stocksStrong = GetStocksSType(cnx,0,8)
    if len(stocksReady) == 0 :
        stocksReady  = GetStocksSType(cnx,1,8)
    if len(stocksBreakCentral) == 0 :
        stocksBreakCentral  = GetStocksSType(cnx,2,8)        
    if len(stocksBasic) == 0 :
        GetStocksBasic(cnx)
    if len(stocksLabel) == 0 :
        cursor = cnx.cursor()
        sql = "select content from stocks_basic where windcode=%(code)s and btype=9 order by quarter desc limit 1"
        for stock in stocks:
            stars = 0

            ready  = [ task for task in stocksReady  if task['windcode'] == stock['windcode']]
            strong = [ task for task in stocksStrong if task['windcode'] == stock['windcode']]
            tech = len(ready) >0 or len(strong) >0
            cursor.execute(sql,{'code':stock['windcode'],})
            if cursor.rowcount >0 :
                stars = cursor.fetchone()[0]
            basic = ( stars == 6 )
            tasks = [ x for x in stocksMainForce if x['windcode']==stock['windcode']]
            stocksLabel.append ({
                'windcode' : stock['windcode'] ,
                'is_jishu' : tech,
                'is_jiben' : basic,
                'is_mf'    : (len(tasks)>0),
                'stars'    : int(stars),
                }
                )            
        cursor.close()

    cnx.close()

if __name__ == '__main__':
    # if __package__ is None:
    #     import sys
    #     from os import path
    #     sys.path.append( path.dirname( path.dirname( path.abspath(__file__) ) ) )
    #     from LieFeng import Config,Stock
    # else:
    #     from ..LieFeng import Config,Stock
    Init() 
    # print stocksBoardStrong,stocksBoardReady

    app.run(host='0.0.0.0', port=3307,debug=True, use_evalex=False)
