#!/usr/bin/env python
# -*- coding: utf8 -*-


import msvcrt
import sys
import TradeX
import mysql.connector
import tornado.ioloop
import tornado.web
from tornado.wsgi import WSGIContainer
from tornado.web import Application , FallbackHandler
from tornado.ioloop import IOLoop
import marshal
from datetime import datetime,timedelta
from flask import Flask
from flask_restful import Api, Resource, reqparse, fields, marshal
import time
from WindPy import w
# from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.schedulers.tornado import TornadoScheduler
import requests
import gc
import json
import logging
logging.basicConfig(level=logging.INFO, 
        format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s', 
        datefmt='%Y-%m-%d %H:%M:%S',  filename='tdx_log.txt',  filemode='a')
from LieFeng import Config,Stock,FourIndicator,Slice
app = Flask(__name__, static_url_path="")
Windcode_Dict = {}
Windcode_Stype = range(8)
GoldenFlag = True



sHost = "60.28.23.80"
nPort = 7709
WindPy = w 
TradeDayFlag = False
stocksCalcDate = datetime.now()-timedelta(days=30)
stocksNewInfoDate= datetime.now()
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
# stocksIndex = {}
# stocksClose = {}
stocksMainForce = []
stocksEventDriven = []
stocksSuspend0 = []
stocksUser = {}
stocks5MinuteNum =0
#  1 :           招商证券深圳行情  119.147.212.81:7709
#  2 :           华泰南京电信      221.231.141.60:7709
#  3 :           华泰  上海电信    101.227.73.20:7709
#  4 :           华泰 上海电信     101.227.77.254:7709
#  5 :           华泰 深圳电信     14.215.128.18:7709
#  6 :           华泰武汉电信      59.173.18.140:7709
#  7 :           华泰天津联通      60.28.23.80:7709
#  8 :           华泰 沈阳联通     218.60.29.136:7709
#  9 :           华泰              122.192.35.44:7709
# 10 :           华泰南京联通      122.192.35.44:7709

stock_wm_str_fields = {    
    'windcode': fields.String,
    'name' : fields.String,
    'warning_status': fields.Integer,
    'type' : fields.Integer,
    'update_date' : fields.String,
    # 'description' : fields.String,
    'price' : fields.String,
    "warning_priority": fields.Integer,
    "warning_sellinglevel": fields.Integer,
    "warning_buyinglevel": fields.Integer,
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

stock_wmrb_str_fields = {
    'source': fields.Integer,
    'calc_date'  : fields.String,
    'wm_type'  : fields.Integer, 
    'data': fields.List(fields.Nested({
        'windcode': fields.String,    
        'name' : fields.String,
        'warning_status': fields.Integer,
        'type' : fields.Integer,
        'update_date' : fields.String,
        # 'description' : fields.String,
        'price' : fields.String,
        "warning_priority": fields.Integer,
        "warning_sellinglevel": fields.Integer,
        "warning_buyinglevel": fields.Integer,
    })),
    

}
def SendWarningMessage(message,url = "http://www.hanzhendata.com/ihanzhendata/warning/buyorder" ):  

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

    global stocksMainForce
    global stocksCalcDate
    global stocksNewInfoDate
    global stocksSuspend0
    
    global TradeDayFlag
    Connector = mysql.connector.connect(**Config.db_config)
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
        stocksBoardStrong=Config.GetStocksBoard(Connector,isStrong=True,isReady=False)
    if len(stocksBoardReady) == 0 :
        stocksBoardReady =Config.GetStocksBoard(Connector,isStrong=False,isReady=True)
    if len(stocksStrong) == 0 :
        stocksStrong = Config.GetStocksSType(Connector,0,8,stocksCalcDate)
    if len(stocksReady) == 0 :
        stocksReady  = Config.GetStocksSType(Connector,1,8,stocksCalcDate)
    if len(stocksBreakCentral) == 0 :
        stocksBreakCentral  = Config.GetStocksSType(Connector,2,8,stocksCalcDate)
    if len(stocksBreakVolume) == 0 :
        stocksBreakVolume   = Config.GetStocksSType(Connector,3,8,stocksCalcDate)
    if len(stocksBasic) == 0 :
        stocksBasic=Config.GetStocksBasic(Connector,stocks)
    if len(stocksLabel) == 0 :
        cursor = Connector.cursor()
        BreakCentralAll = Config.GetStocksSType(Connector,2,8,stocksCalcDate,ExistedFlag=True)
        BreakVolumeAll  = Config.GetStocksSType(Connector,3,8,stocksCalcDate,ExistedFlag=True)
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
    
    Connector.close()

def WarningMessage_Init(GdbbFlag=False):
    global Windcode_Dict
    global Windcode_Stype    
    Connector = mysql.connector.connect(**Config.db_config)
    Windcode_Dict = {}
    for index in range(len(Windcode_Stype)):
        Windcode_Stype[index] = {}
    MarketType = 1
    current_time = datetime.now()
    sql = "select  name, windcode,stype from stocks_sr where date=%(cal_date)s" 
    sql_o = "select  name, windcode,stype from stocks_sr where stype=2 or stype=3"
    sql_w = "select wtype,ktype,date,stype,price  from stocks_warning where windcode=%(code)s and stype=%(stype)s order by date desc limit 1"
    sql_p = "Insert into stocks_warning_tdx(id,wtype,stype,ktype,windcode,name,price,date) values(%(sid)s,%(wtype)s,%(stype)s,%(ktype)s,%(code)s,%(name)s,%(price)s,%(wdate)s)"
    cursor = Connector.cursor()
    cursor_w = Connector.cursor()
    
    cursor.execute(sql,{'cal_date':stocksCalcDate})
    for bname,windcode,stype in cursor:
        if windcode in stocksSuspend0:
            continue  
        if stype==3 :
            continue
        Windcode_Stype[stype][windcode] = {'name':bname,}
        # Windcode_Stype[stype][windcode].update(stocksLabel[ windcode] )        
        Windcode_Dict[windcode] = {'wsq2':0,'wsq5':0,'wsq6':0,}
    cursor.execute(sql_o)
    for bname,windcode,stype in cursor:
        if windcode in stocksSuspend0:
            continue  
        Windcode_Stype[stype][windcode] = {'name':bname,}
        # Windcode_Stype[stype][windcode].update(stocksLabel[ windcode] )        
        Windcode_Dict[windcode] = {'wsq2':0,'wsq5':0,'wsq6':0,}
    print len(Windcode_Dict),len(stocksBasicSelect),len(stocksMainForce),len(stocksEventDriven)
    wd=[] #[stocksBasicSelect,stocksMainForce,stocksEventDriven]
    for wi in range(len(wd)):
        # if wi==2 or wi==1 or wi==0: #current not in calcluting range
        #     continue
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
    # cursor.close()

    for uid in stocksUser:
        stype = 7
        for windcode in stocksUser[uid]:
            if windcode not in stocksLabel :### 510500.SH
                continue
            if Windcode_Stype[stype].get(windcode) is None:
                Windcode_Stype[stype][windcode] = {'warning_status':4,'type':[],'date':[],'wsq2':0,'wsq5':0,'wsq6':0,}
                Windcode_Stype[stype][windcode].update(stocksLabel[ windcode] )
                Windcode_Stype[stype][windcode]['warning_price']=stocksUser[uid][windcode]['warning_price']
                Windcode_Stype[stype][windcode]['warning_date'] =stocksUser[uid][windcode]['warning_date']
                Windcode_Stype[stype][windcode]['name'] = [x['name'] for x in stocks if x['windcode'] == windcode ][0]
            Windcode_Dict[windcode] = {}
    num = 0 
    stocksWM = []
    for index in range(len(Windcode_Stype)) :
        if index == 7 :
            continue     
        for windcode in Windcode_Stype[index]:
            cursor_w.execute(sql_w,{'code':windcode,'stype':index})
            if cursor_w.rowcount>0:
                temp = cursor_w.fetchone()
                ts = WindPy.tdayscount(temp[2],datetime.now(),"")
                stype = temp[3]
                ws = temp[0]
                if ts.ErrorCode!=0 or ( ts.Data[0][0] >Config.BreakCentralPermitDays) or (  ts.Data[0][0] >Config.BreakVolumePermitDays):
                    Windcode_Stype[index][windcode].update({'warning_status':0,})
                    if ts.ErrorCode!=0 :
                        continue
                    if stype==2 or stype==3:
                        continue
                    ws = 3
                    try:
                        cursor.execute(sql_p,{
                            'sid':num,
                            'wtype':ws,
                            'stype':stype,
                            'ktype':3,
                            'name':Windcode_Stype[index][windcode]['name'],
                            'code':windcode,
                            'price': 0,
                            'wdate':current_time,    
                        })
                    except Exception, e:
                        print 'WarningMessage to Database Error',e.message
                        continue
                    stocksWM.append( {
                        'id': num,
                        'windcode': windcode,
                        'name' : Windcode_Stype[index][windcode]['name'],
                        'warning_status': 3,
                        'type' : index,
                        'update_date' : current_time.strftime("%Y-%m-%d %H:%M:%S"),
                        'price'   : 0 ,                        
                        'is_strong': stocksLabel[windcode]['is_strong'],
                        'is_ready': stocksLabel[windcode]['is_ready'],
                        'is_bc': stocksLabel[windcode]['is_bc'],
                        'is_bv': stocksLabel[windcode]['is_bv'],
                        'is_jishu': stocksLabel[windcode]['is_jishu'],
                        'is_jiben': stocksLabel[windcode]['is_jiben'],
                        'is_mf': stocksLabel[windcode]['is_mf'],
                        'is_bs': stocksLabel[windcode]['is_bs'],
                        'is_br': stocksLabel[windcode]['is_br'],
                        'stars': stocksLabel[windcode]['stars'],
                    })
                    Windcode_Stype[index][windcode].update({'warning_status':ws,})
                    num = num  + 1 

                    continue
                 
                if ws == 3 : #  or index==2 or index==3lose watch
                    ws = 0           
                Windcode_Stype[index][windcode].update({
                    #wtype,ktype,date,stype,price
                    'warning_status':ws,
                    'warning_ktype':temp[1],
                    'warning_date':temp[2],
                    'warning_price':temp[4],
                })            
                          
            else:
                Windcode_Stype[index][windcode].update({'warning_status':0,})
   
    Connector.commit()
    cursor.close()
    cursor_w.close()
        
    
    
    print "stocks warning message init-------------------------------"
    print len(Windcode_Dict)
    ktype_list= [0,2,5,6,7,8]
    current_time = datetime.now()
    for ktype in ktype_list :
        if ktype > 6:
            tablename = 'stocks_history_days'
        else:       
            tablename = 'stocks_history_minutes'
        for windcode in  Windcode_Dict  :
            # print windcode
            
            ktypestr = str(ktype)
            if ktype!=7:
                indicator = Stock.StocksOHLC_N(windcode,tablename,ktype,Connector,limit=2000)
                Windcode_Dict[windcode].update( { ktypestr : indicator,})             
                Windcode_Dict[windcode]['tech'+ktypestr] = Stock.StocksTech(Windcode_Dict[windcode][ktypestr])
            else:
                indicator = Stock.StocksOHLC_N(windcode,tablename,ktype,Connector,limit=7)
                Windcode_Dict[windcode].update( { ktypestr : indicator,})
    for stype in range(len(Windcode_Stype)) : 
        if  not GdbbFlag :        
            continue 
             
        for windcode in  Windcode_Stype[stype]  :            
            
            if Windcode_Stype[stype][windcode]['warning_status'] != 1 :
                continue 
            if stype == 2:
                ktype = 2 
            elif stype == 5 :
                ktype = 5
            else:
                continue            
            indicator =  Windcode_Dict[windcode][ktypestr]
            wnd =  Windcode_Stype[stype][windcode]['warning_date']
            
            if wnd.date() == current_time :
                continue
            ktypestr = str(ktype)

            
            print windcode,wnd
            for index in range( len(indicator) ):
                if  wnd == indicator[-index-1]['date'] :
                    break
            print ktype,windcode,Windcode_Stype[stype][windcode]['warning_date'],Windcode_Stype[stype][windcode]['warning_status'],-index-1
            for i in range(-index-1,0):
                # print -index-1,i,indicator[i]['date']
                if Windcode_Dict[windcode]['tech'+ktypestr]['diff'][i] > Windcode_Dict[windcode]['tech'+ktypestr]['dea'][i] :
                    if Windcode_Dict[windcode]['tech'+ktypestr]['diff'][i-1] < Windcode_Dict[windcode]['tech'+ktypestr]['dea'][i-1] :
                        if Windcode_Stype[stype][windcode].get('gdbb') is None:
                           Windcode_Stype[stype][windcode].update( {'gdbb':[4]} )
                        else:
                           Windcode_Stype[stype][windcode]['gdbb'].append(4)
    #add unfinished week days OHLC. 
    ktype = 8 
    last_date = Stock.StocksGetLastDate(ktype,current_time.date())

    ds =str(ktype)
    for windcode  in  Windcode_Dict :
        board = Windcode_Dict[windcode]     

        # print windcode,board.keys()
        board['ta'+str(ktype)] = board[ds]
         
        board['ta'+str(ktype-1)] = board[str(ktype-1)]
        # for ri in range(7):
        #     board['ta'+str(ktype-1)].insert(0,{
        #         'date':Windcode_Dict[windcode][-ri-1]['date'],
        #         'open':Windcode_Dict[windcode][-ri-1]['open'],
        #         'high':Windcode_Dict[windcode][-ri-1]['high'],
        #         'low' :Windcode_Dict[windcode][-ri-1]['low'],
        #         'close':Windcode_Dict[windcode][-ri-1]['close'],
        #         })        
        if len(last_date)>0 and board[ds][-1]['date'].date() < last_date[-1]:
            last_data=Stock.StocksGetLastData_New( board ,ktype,last_date)

            if last_data is not None and len(last_data) > 0: 
                last_data.update({'date':datetime.combine(last_date[-1], datetime.min.time())})           
                Windcode_Dict[windcode][ds].append(last_data)
            else:
                Windcode_Dict[windcode][ds].append({'date': current_time,
                    'open':Windcode_Dict[windcode][ds][-1]['open'],'high':0,'low':0,'close':0
                    }) 
        board.pop('ta'+str(ktype),None)  
        board.pop('ta'+str(ktype-1),None)

    Connector.close()

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
    # global stocksIndex
    # global stocksClose
    # global stocksOHLC
    global stocksMainForce
    global stocksCalcDate
    global GoldenFlag
    global stocksUser
    
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
    # stocksIndex = {}
    # stocksClose = {}
    stocksMainForce = []
    current_time = datetime.now()
    stocksCalcDateBefore= stocksCalcDate

    Days_Init()
    Config.EventDrivenStocks() 
    stocksUser=Config.UserStocks()

    WarningMessage_Init(GdbbFlag=GoldenFlag)
    WarningMessage_CurrentDay_Data([2,5,6],current_time)
    w.start()   
    gc.collect()
    GoldenFlag = False
    

def WarningMessage_CurrentDay_Data(KtypeList,current_datetime):
    if not TradeDayFlag :
        print WarningMessage_CurrentDay_Data.__name__," Error not TradeDay!"
        return
    if current_datetime.hour <=9 or current_datetime.hour >=15 :
        return
    
    nStart = 0 
    for windcode in Windcode_Dict:
        
        code,market = windcode.split(".")
        if market=="SZ":
            nMarket = 0
        elif market=="SH":
            nMarket = 1        
        for ktype in  KtypeList :
            nCount = 4
            if ktype==5:
                nCount *=2
            if ktype==2:
                nCount *=12 
            ktypestr = str(ktype)
            nCategory = Config.WindKtypeToTdxCategory(ktype)
            if nCategory <0 :
                continue
            trynum =0 
            
            while( trynum<nCount ) :                
                try:
                    clientHq = TradeX.TdxHq_Connect(sHost, nPort)
                except TradeX.TdxHq_error, e:
                    print "error: " + e.message,"Trynum",trynum
                    trynum = trynum + 1
                    continue
                errinfo, count, result = clientHq.GetSecurityBars(nCategory, nMarket, code, nStart, nCount)                     
                # print code,errinfo
                if errinfo == "" :
                    break
            if errinfo != "":
                print windcode,trynum,nCount
                continue
            result_list = []            
            for line in result.split("\n")[1:]:
                line =line.split('\t')
                dt = line[0]
                ro = line[1]
                rc = line[2]
                rh = line[3]
                rl = line[4]
                # ra = line[5]
                rv = line[6]
                dtime = datetime.strptime(dt,"%Y-%m-%d %H:%M")
                result_list.append({
                    'date':dtime,
                    'open' :float(ro),
                    'close':float(rc),
                    'high' :float(rh),
                    'low'  :float(rl),
                    'volume':float(rv),
                    })
            # print result_list
            rli = count -1
            wdi = -1
            while (rli !=0 and wdi !=-count) :
                if Windcode_Dict[windcode][ktypestr][wdi]['date'] == result_list[rli]['date']:
                    break
                elif Windcode_Dict[windcode][ktypestr][wdi]['date'] < result_list[rli]['date']:
                    rli -=1
                else:
                    wdi -=1
            if Windcode_Dict[windcode][ktypestr][wdi]['date'] != result_list[rli]['date']: #must satisfy count>1
                continue
            # wdi = len(Windcode_Dict[windcode][ktypestr])+wdi
            for index in range(count)[rli:]:
                if wdi < 0 :
                    Windcode_Dict[windcode][ktypestr][wdi] = result_list[index]
                else:
                    Windcode_Dict[windcode][ktypestr].append( result_list[index] )
                wdi +=1
            # print Windcode_Dict[windcode][ktypestr][-count:]
        # break 

def WarningMessage_SR_Main(Windcode_Dict,Windcode_Stype,tablename,filename,builddate,SendWM=False):
    Connector = mysql.connector.connect(**Config.db_config)
    dnow = datetime.now()
    cursor = Connector.cursor()
    sql = "Insert into "+tablename+" (id,wtype,wpriority,wsellinglevel,wbuyinglevel,bdate,stype,ktype,windcode,name,price,date) values(%(sid)s,%(w)s,%(wp)s,%(wsl)s,%(wbl)s,%(bdate)s,%(s)s,%(k)s,%(code)s,%(name)s,%(price)s,%(wdate)s)"
    stocksWM =[]
    num = 0
    f =open(filename,'w')
    for stype in range(len(Windcode_Stype)):
        for windcode in  Windcode_Dict  :
            if  Windcode_Stype[stype].get(windcode) is None:
                continue
            # tablename = 'stocks_history_minutes'
            if stype!=2 and stype!=3:
                days = 1
            elif stype==2:
                days = Config.BreakCentralPermitDays
            else:
                days = Config.BreakVolumePermitDays            
            tdo = WindPy.tdaysoffset(days,Windcode_Stype[stype][windcode]['warning_date'],"")
            if Config.CheckWindData(tdo):
                tdo= tdo.Data[0][0]
            else:
                tdo = Windcode_Stype[stype][windcode]['warning_date']+timedelta(days=days)
            print  Windcode_Stype[stype][windcode]['warning_date'],tdo
            tdo = tdo.replace(hour = 16)
               
            # if stype!=2 and stype!=3 and Wd_Rb[windcode]['warning_status'] !=0 :
            #     continue
            # if stype==2 or stype==3:
            #     Windcode_Stype[stype][windcode]['warning_status'] = 0
            for ktype in [2,5,6]:
                ktypestr = str(ktype)
                wdlen =len(Windcode_Dict[windcode][ktypestr])    
                for index in range( wdlen ) :
                    wddate = Windcode_Dict[windcode][ktypestr][-index-1]['date']
                    if wddate<=Windcode_Stype[stype][windcode]['warning_date'] or wddate>tdo:
                        continue
                 
                    
                    # if wdlen-index-1 < -1 or wdlen<=index:
                    #     continue
                    # wtc=WindPy.tdayscount(wddate,Windcode_Stype[stype][windcode]['warning_date'], "")
                    # if wtc.ErrorCode ==0 :
                    #     if wtc.Data[0][0] > 10 :
                    #         break
                    # print windcode,ktype,index,-index-1,len(Windcode_Dict[windcode][ktypestr])
                    
                    
                    wdclose= Windcode_Dict[windcode][ktypestr][-index-1]['close']                    
                    kline_ktype = Windcode_Dict[windcode][ktypestr][:-index]
                    tech_ktype =  {
                            'diff' : Windcode_Dict[windcode]['tech'+ktypestr]['diff'] [:-index ] ,
                            'dea'  : Windcode_Dict[windcode]['tech'+ktypestr]['dea'] [:-index ] ,
                            'macd' : Windcode_Dict[windcode]['tech'+ktypestr]['macd'] [:-index ] ,
                            # 'vma'  : vma [:-index ] ,
                            'fast_k':Windcode_Dict[windcode]['tech'+ktypestr]['fast_k'][:-index ],
                            'fast_d':Windcode_Dict[windcode]['tech'+ktypestr]['fast_d'][:-index ],
                            'fast_j':Windcode_Dict[windcode]['tech'+ktypestr]['fast_j'][:-index ],
                            'slow_k':Windcode_Dict[windcode]['tech'+ktypestr]['slow_k'][:-index ],
                            'slow_d':Windcode_Dict[windcode]['tech'+ktypestr]['slow_d'][:-index ],
                            'slow_j':Windcode_Dict[windcode]['tech'+ktypestr]['slow_j'][:-index ],
                            'pb1':Windcode_Dict[windcode]['tech'+ktypestr]['pb1'][:-index ],
                            'pb2':Windcode_Dict[windcode]['tech'+ktypestr]['pb2'][:-index ],
                            'pb6':Windcode_Dict[windcode]['tech'+ktypestr]['pb6'][:-index ],                            
                        }
                    if len(kline_ktype) ==0 :
                        continue
                    ws = 0
                    wp = 0
                    wbl = 0
                    wsl = 0                    
                    if stype==2 and ktype==2:
                        if Windcode_Dict[windcode]['tech8']['fast_k'][-1] <=Windcode_Dict[windcode]['tech8']['fast_d'][-1]:
                            continue
                        if Windcode_Stype[stype][windcode]['warning_status'] ==1 and FourIndicator.GoldenDead(tech_ktype) ==4:                           
                             
                                ws = 4 
                                Windcode_Stype[stype][windcode].update({
                                    'warning_status':ws,
                                    'type':stype,
                                    'warning_date':wddate,
                                    'warning_ktype':ktype,
                                    'warning_priority':wp,
                                    'warning_price':wdclose,
                                    'warning_sellinglevel' :wsl,
                                    'warning_buyinglevel' :wbl,
                                    })

                    if Windcode_Stype[stype][windcode]['warning_status'] ==0 :
                        if stype<4 and stype!=1 and Windcode_Dict[windcode]['tech8']['fast_k'][-1] <=Windcode_Dict[windcode]['tech8']['fast_d'][-1]:
                            continue
                        if ktype == 6:
                            if stype==2 or stype==3:
                                if Windcode_Dict[windcode]['tech'+ktypestr]['fast_j'][-1] < 5:
                                    if stype==2:
                                        if Windcode_Dict[windcode]['tech8']['pb1'][-1]>Windcode_Dict[windcode]['tech8']['pb6'][-1] and \
                                           Windcode_Dict[windcode]['tech8']['pb2'][-1]>Windcode_Dict[windcode]['tech8']['pb6'][-1] :
                                            ws=1
                                        else:
                                            continue
                                    else:
                                        ws = 1
                                    Windcode_Stype[stype][windcode].update({'warning_status':ws,'type':stype,'warning_ktype':ktype,'warning_date':dtime,})
                            
                            else:
                                if Windcode_Dict[windcode]['tech'+ktypestr]['slow_j'][-1] < 5:
                                    if stype==1:
                                        if wdclose>Windcode_Dict[windcode]['tech8']['pb6'][-1] :
                                            ws = 1
                                        else:
                                            continue
                                    else:
                                        ws = 1  
                            if ws ==0 :
                                continue                   

                            print windcode,ktype,stype,wddate
                            Windcode_Stype[stype][windcode].update({
                                'warning_status':ws,
                                'type':stype,
                                'warning_date':wddate,
                                'warning_ktype':ktype,
                                'warning_priority':wp,
                                'warning_price':wdclose,
                                'warning_sellinglevel' :wsl,
                                'warning_buyinglevel' :wbl,
                            }) 
                    else:
                        if ktype==6:
                            if Config.SellingEnable  and Windcode_Stype[stype][windcode]['warning_status'] ==4: 
                                # if len(kline_ktype)<12:
                                #     continue
                                print windcode,stype,len(kline_ktype)
                                rd = FourIndicator.RallyDivergence(tech_ktype,kline_ktype)
                                if rd==2 :
                                    ws = 5
                                    wp = 2
                                if rd==1 :
                                    ws = 5
                                    wp = 1
                                if rd==2 or rd==1 :
                                    Windcode_Stype[stype][windcode].update({
                                        'warning_status':ws,
                                        'warning_priority':wp,
                                        'warning_sellinglevel':wsl,
                                        'warning_buyinglevel':wbl,
                                        'warning_ktype':ktype,
                                        'warning_date':wddate,
                                        'warning_price':wdclose,
                                        })
                                        
                        if ktype ==  5 :                                
                            if Config.SellingEnable and Windcode_Stype[stype][windcode]['warning_status']==4:
                                if float(wdclose)/float(Windcode_Stype[stype][windcode]['warning_price']) >=1.1 :
                                    ws = 5
                                    wp = 0
                                    wsl= 3
                                    Windcode_Stype[stype][windcode].update({
                                        'warning_status':ws,
                                        'type':stype,
                                        'warning_date':wddate,
                                        'warning_ktype':ktype,
                                        'warning_priority':wp,
                                        'warning_price':wdclose,
                                        'warning_sellinglevel' :wsl,
                                        'warning_buyinglevel' :wbl,
                                    })
                            if Windcode_Stype[stype][windcode]['warning_status'] ==1 :
                                if stype<4 and stype!=1 and Windcode_Dict[windcode]['tech8']['fast_k'][-1] <=Windcode_Dict[windcode]['tech8']['fast_d'][-1]:
                                    continue
                                if FourIndicator.GoldenDead(tech_ktype) == 4 :
                                    if Windcode_Stype[stype][windcode].get('gdbb') is None:
                                        Windcode_Stype[stype][windcode].update( {'gdbb':[4]} )
                                    else:
                                        Windcode_Stype[stype][windcode]['gdbb'].append(4)
                                
                                if Windcode_Stype[stype][windcode].get('gdbb') is None:
                                    continue
                                gl = len(Windcode_Stype[stype][windcode]['gdbb'])

                                if  stype==0 :
                                    if  gl==1 :
                                        ws = 4
                                        Windcode_Stype[stype][windcode].update({
                                            'warning_status':ws,
                                            'type':stype,
                                            'warning_date':wddate,
                                            'warning_ktype':ktype,
                                            'warning_priority':wp,
                                            'warning_price':wdclose,
                                            'warning_sellinglevel' :wsl,
                                            'warning_buyinglevel' :wbl,
                                        })               
                                else:
                                    if stype==3 and gl==1:
                                        ws = 4
                                        Windcode_Stype[stype][windcode].update({
                                            'warning_status':ws,
                                            'type':stype,
                                            'warning_date':wddate,
                                            'warning_ktype':ktype,
                                            'warning_priority':wp,
                                            'warning_price':wdclose,
                                            'warning_sellinglevel' :wsl,
                                            'warning_buyinglevel' :wbl,
                                        })
                                    if gl==2 :  
                                        if stype==1:
                                                if wdclose>Windcode_Dict[windcode]['tech8']['pb6'][-1] :
                                                    ws = 4
                                                else:
                                                    continue
                                        else:                     
                                            ws = 4
                                        Windcode_Stype[stype][windcode].update({
                                            'warning_status':ws,
                                            'type':stype,
                                            'warning_date':wddate,
                                            'warning_ktype':ktype,
                                            'warning_priority':wp,
                                            'warning_price':wdclose,
                                            'warning_sellinglevel' :wsl,
                                            'warning_buyinglevel' :wbl,
                                            })
                    if ws ==0 :
                            continue
                    
                    
                    # print windcode,Windcode_Stype[stype][windcode]['warning_date'],Windcode_Stype[stype][windcode]['warning_status']
                    f.write(" code %s date %s type %d Signal %d ktype %d\n" %(windcode,Windcode_Stype[stype][windcode]['warning_date'].strftime("%Y-%m-%d %H:%M:%S")
                        ,stype ,Windcode_Stype[stype][windcode]['warning_status'],Windcode_Stype[stype][windcode]['warning_ktype']))
                   
                    if SendWM and Windcode_Stype[stype][windcode]['warning_date'].date() == builddate.date():  
                        stocksWM.append( {
                                'id': num,
                                'windcode': windcode,
                                'name' : Windcode_Stype[stype][windcode]['name'],
                                'warning_status': Windcode_Stype[stype][windcode]['warning_status'],
                                'warning_priority':Windcode_Stype[stype][windcode]['warning_priority'],
                                'warning_sellinglevel':Windcode_Stype[stype][windcode]['warning_sellinglevel'],
                                'warning_buyinglevel':Windcode_Stype[stype][windcode]['warning_buyinglevel'],
                                'type' : stype,
                                'update_date' : Windcode_Stype[stype][windcode]['warning_date'].strftime("%Y-%m-%d %H:%M:%S"),

                                'price'   : str(Windcode_Stype[stype][windcode]['warning_price']) ,
                                
                                'is_strong': stocksLabel[windcode]['is_strong'],
                                'is_ready': stocksLabel[windcode]['is_ready'],
                                'is_bc': stocksLabel[windcode]['is_bc'],
                                'is_bv': stocksLabel[windcode]['is_bv'],
                                'is_jishu': stocksLabel[windcode]['is_jishu'],
                                'is_jiben': stocksLabel[windcode]['is_jiben'],
                                'is_mf': stocksLabel[windcode]['is_mf'],
                                'is_bs': stocksLabel[windcode]['is_bs'],
                                'is_br': stocksLabel[windcode]['is_br'],
                                'stars': stocksLabel[windcode]['stars'],
                        })                                      
                        cursor.execute(sql,{
                            'sid':num,
                            'w':Windcode_Stype[stype][windcode]['warning_status'],
                            'wp':Windcode_Stype[stype][windcode]['warning_priority'],
                            'wsl':Windcode_Stype[stype][windcode]['warning_sellinglevel'],
                            'wbl':Windcode_Stype[stype][windcode]['warning_buyinglevel'],
                            'bdate':dnow,
                            's':stype,
                            'k':Windcode_Stype[stype][windcode]['warning_ktype'],
                            'name':Windcode_Stype[stype][windcode]['name'],
                            'code':windcode,
                            'price': Windcode_Stype[stype][windcode]['warning_price'],
                            'wdate': Windcode_Stype[stype][windcode]['warning_date']    
                        })                        
                    num += 1                    
    f.close()
    Connector.commit()
    cursor.close()
    Connector.close()
    return stocksWM

def WarningMessage_SignalRebuild(current_datetime,SendWM=False):
    Connector = mysql.connector.connect(**Config.db_config)
    MarketType = 1
    
    Wd_Rb = {}
    Wd_Rb_Stype= range(8)
    for index in range(len(Wd_Rb_Stype)):
        Wd_Rb_Stype[index] = {}
    sql_date = "select  distinct  date from stocks_sr  order by date desc limit 2"   
    sql = "select  name, windcode,stype,date from stocks_sr  where date=%(cal_date)s order by windcode asc,date desc"   
    sql_w = "select wtype,ktype,date,stype,price  from stocks_warning where windcode=%(code)s and stype=%(stype)s order by date desc limit 1"
    cursor = Connector.cursor()
    cursor_w = Connector.cursor()
    num = 0
    cursor.execute(sql_date)
    datelist=[]
    for sdate in cursor:        
        datelist.append( sdate[0])
    if stocksNewInfoDate.date() >= stocksCalcDate.date():
        cal_date = datelist[0]
    # elif stocksNewInfoDate.date() == stocksCalcDate.date() :
    #     cal_date = datelist[0]
    else:
        cal_date = datelist[1]
    # cal_date = datelist[1]
    print stocksNewInfoDate,stocksCalcDate,cal_date
    
    cursor.execute(sql,{'cal_date':cal_date})
    for bname,windcode,stype,sdate in cursor:
        if windcode in stocksSuspend0:
            continue 
        Wd_Rb_Stype[stype][windcode] = {
            'name':bname,
            'warning_status':0,
            'warning_date':sdate,
            'wsq2':0,'wsq5':0,'wsq6':0,}
        Wd_Rb[windcode] = {}
       
    for uid in stocksUser:
        stype = 7
        for windcode in stocksUser[uid]:
            if windcode not in stocksLabel :### 510500.SH
                continue
            if Wd_Rb_Stype[stype].get(windcode) is None:
                Wd_Rb_Stype[stype][windcode] = {'warning_status':4,'type':[],'date':[],'wsq2':0,'wsq5':0,'wsq6':0,}
                Wd_Rb_Stype[stype][windcode]['warning_price']=stocksUser[uid][windcode]['warning_price']
                Wd_Rb_Stype[stype][windcode]['warning_date'] =cal_date
                Wd_Rb_Stype[stype][windcode]['name'] = [x['name'] for x in stocks if x['windcode'] == windcode ][0]
            Wd_Rb[windcode] = {}
            
    print len(Wd_Rb),len(stocksBasicSelect),len(stocksMainForce),len(stocksEventDriven)
    cursor.close()
    
    for index in range(len(Wd_Rb_Stype)) :
        if index == 7 :
            continue
        for windcode in Wd_Rb_Stype[index]:
            cursor_w.execute(sql_w,{'code':windcode,'stype':index})
            if cursor_w.rowcount>0:
                temp = cursor_w.fetchone()
                ts = WindPy.tdayscount(temp[2],datetime.now(),"")
                stype = temp[3]
                if ts.ErrorCode!=0: 
                    Wd_Rb_Stype[index][windcode].update({'warning_status':0,'warning_date':temp[2]}) 
                    continue
                ws = temp[0] 
                if ( ts.Data[0][0] >Config.BreakCentralPermitDays) or ( ts.Data[0][0] >Config.BreakVolumePermitDays):
                    ws = 3
                    continue
                if ws == 3  or index==2 or index==3: # lose watch
                    ws = 0           
                Wd_Rb_Stype[index][windcode].update({
                    #wtype,ktype,date,stype,price
                    'warning_status':ws,
                    'warning_ktype':temp[1],
                    'warning_date':temp[2],
                    'warning_price':temp[4],
                })             
            else:
                Wd_Rb_Stype[index][windcode].update({'warning_status':0,'warning_date':cal_date})
    print "tdx stocks warning message rebuild-------------------------------"
    print len(Wd_Rb)
    ktype_list= [2,5,6,8]
    current_date = datetime.now().date()
    for ktype in ktype_list :
        if ktype>6:
            tablename = 'stocks_history_days'
        else:       
            tablename = 'stocks_history_minutes'
        for windcode in  Wd_Rb  :
            # print windcode            
            ktypestr = str(ktype)
            indicator = Stock.StocksOHLC_N(windcode,tablename,ktype,Connector,limit=2000)
            Wd_Rb[windcode].update( { ktypestr : indicator,})
     
    for windcode in Wd_Rb: 
        nStart = 0       
        code,market = windcode.split(".")
        if market=="SZ":
            nMarket = 0
        elif market=="SH":
            nMarket = 1        
        for ktype in  ktype_list :
            nCount = 5
            if ktype==5:
                nCount *=2
            if ktype==2:
                nCount *=12 
            ktypestr = str(ktype)
            nCategory = Config.WindKtypeToTdxCategory(ktype)
            if nCategory <0 :
                continue
            trynum =0 
            
            while( trynum<nCount ) :                
                try:
                    clientHq = TradeX.TdxHq_Connect(sHost, nPort)
                except TradeX.TdxHq_error, e:
                    print windcode,"error: " + e.message,"Trynum",trynum
                    trynum = trynum + 1
                    continue
                errinfo, count, result = clientHq.GetSecurityBars(nCategory, nMarket, code, nStart, nCount)                     
                # print code,errinfo
                if errinfo == "" :
                    break
            if errinfo != "":
                print windcode,trynum,nCount
                continue
            result_list = []            
            for line in result.split("\n")[1:]:
                line =line.split('\t')
                dt = line[0]
                ro = line[1]
                rc = line[2]
                rh = line[3]
                rl = line[4]
                # ra = line[5]
                rv = line[6]
                # print ktype, line
                if ktype< 8:
                    dtime = datetime.strptime(dt,"%Y-%m-%d %H:%M")
                else:
                    dtime = datetime.strptime(dt,"%Y%m%d")
                    # dtime = datetime.combine(dtime, datetime.min.time())#.replace(microsecond = 500)
                result_list.append({
                    'date':dtime,
                    'open' :float(ro),
                    'close':float(rc),
                    'high' :float(rh),
                    'low'  :float(rl),
                    'volume':float(rv),
                    })
            # print result_list
            if count == 0 :
                continue
            rli = count -1
            wdi = -1
            while (rli !=0 and wdi !=-count) :
                # print ktype,count,wdi, rli,result
                if Wd_Rb[windcode][ktypestr][wdi]['date'] == result_list[rli]['date']:
                    break
                elif Wd_Rb[windcode][ktypestr][wdi]['date'] < result_list[rli]['date']:
                    rli -=1
                else:
                    wdi -=1
            if Wd_Rb[windcode][ktypestr][wdi]['date'] != result_list[rli]['date']: #must satisfy count>1
                continue
            # wdi = len(Wd_Rb[windcode][ktypestr])+wdi
            for index in range(count)[rli:]:
                if wdi < 0 :
                    Wd_Rb[windcode][ktypestr][wdi] = result_list[index]
                else:
                    Wd_Rb[windcode][ktypestr].append( result_list[index] )
                wdi +=1
            # print Wd_Rb[windcode][ktypestr][-count:]
    pwd = [] # [ '600129.SH','601238.SH','601636.SH','601633.SH' ]
    for windcode in Wd_Rb :
        for ktype in ktype_list:
            ktypestr = str(ktype)
            Wd_Rb[windcode]['tech'+ktypestr] = Stock.StocksTech(Wd_Rb[windcode][ktypestr] )
            if ktype == 5 and windcode in pwd:
                for index in range(8) :                    
                    print windcode,len(Wd_Rb[windcode][ktypestr]),Wd_Rb[windcode][ktypestr][-index-1]['date']
                    print Wd_Rb[windcode]['tech'+ktypestr]['fast_k'][-index-1],Wd_Rb[windcode]['tech'+ktypestr]['fast_d'][-index-1],Wd_Rb[windcode]['tech'+ktypestr]['fast_j'][-index-1]
                    print Wd_Rb[windcode]['tech'+ktypestr]['slow_k'][-index-1],Wd_Rb[windcode]['tech'+ktypestr]['slow_d'][-index-1],Wd_Rb[windcode]['tech'+ktypestr]['slow_j'][-index-1]
    tablename = 'stocks_warning_rebuild'
    filename  = 'Signal_tdx.txt'       
    stocksWM =  WarningMessage_SR_Main(Wd_Rb,Wd_Rb_Stype,tablename,filename,current_datetime,SendWM = SendWM)
    Connector.close()
    del Wd_Rb,Wd_Rb_Stype
    print stocksWM
    stocksWMRB ={}
    stocksWMRB['source'] = 1
    stocksWMRB['calc_date'] = current_datetime.strftime("%Y-%m-%d %H:%M:%S")
    stocksWMRB['wm_type']=1
    stocksWMRB['data'] = stocksWM
    with app.app_context():
        SendWarningMessage(  marshal(stocksWMRB, stock_wmrb_str_fields) ,url="http://www.hanzhendata.com/ihanzhendata/warning/rebuildworder" ) 
    
    gc.collect()
 
def WarningMessage_Minute():
    global stocks5MinuteNum
    current_datetime = datetime.now()
    print current_datetime,Config.Get5MintueNumber( current_datetime )
    stocks5MinuteNum = Config.Get5MintueNumber( current_datetime )
    print 'stocks5MinuteNum',stocks5MinuteNum
    if stocks5MinuteNum ==0 or stocks5MinuteNum==25:
        print datetime.now()
        stocks5MinuteNum +=1
        return    
    ktype_list = [2,8]
    if stocks5MinuteNum % 6  == 0 :
        ktype_list =[2,5,8]
    if stocks5MinuteNum % 12 == 0 :
        ktype_list =[2,5,6,8]
    stocks5MinuteNum +=1
    
    stype_list = [0,1,2,3,7]
    stocksWM= []
    num = 0
    Connector = mysql.connector.connect(**Config.config)
    sql = "Insert into stocks_warning_tdx(id,wtype,wpriority,wsellinglevel,wbuyinglevel,stype,ktype,windcode,name,price,date) values(%(sid)s,%(wtype)s,%(wp)s,%(wsl)s,%(wbl)s,%(stype)s,%(ktype)s,%(code)s,%(name)s,%(price)s,%(wdate)s)"
    cursor = Connector.cursor()

    starttime1 = time.time()
    
    for windcode in Windcode_Dict:
        # print windcode
        nStart = 0
        starttime = time.time()
        code,market = windcode.split(".")
        if market=="SZ":
            nMarket = 0
        elif market=="SH":
            nMarket = 1
        # nMarket = 0 #0 SZ 1 SH
        for ktype in ktype_list:
            ktypestr = str(ktype)
            nCategory = Config.WindKtypeToTdxCategory(ktype)
            nCount = 60
            if ktype==2:
                nCount *=12
            if ktype==5 :
                nCount *=2
            
            trynum =0 
            
            while( trynum<nCount ) :                
                try:
                    clientHq = TradeX.TdxHq_Connect(sHost, nPort)
                except TradeX.TdxHq_error, e:
                    trynum = trynum + 1
                    print code,"error: " + e.message,"Trynum",trynum                    
                    continue
                errinfo, count, result = clientHq.GetSecurityBars(nCategory, nMarket, code, nStart, nCount)                     
                # print code,errinfo
                if errinfo == "" :
                    break
            if errinfo != "" or trynum >= nCount:
                print windcode,"Failed TradeX",trynum,nCount
                continue
            result_list = [] 
            if ktype==8 :
                result = result.split("\n")[1:] 
            else:
                result = result.split("\n")[1:-1]              
            for line in result:
                line =line.split('\t')
                dt = line[0]
                ro = line[1]
                rc = line[2]
                rh = line[3]
                rl = line[4]
                # ra = line[5]
                rv = line[6]
                # print line
                if ktype<8 :
                    dtime = datetime.strptime(dt,"%Y-%m-%d %H:%M")
                else:
                    dtime = datetime.strptime(dt,"%Y%m%d")
                result_list.append({
                    'date':dtime,
                    'open' :float(ro),
                    'close':float(rc),
                    'high' :float(rh),
                    'low'  :float(rl),
                    'volume':float(rv),
                    })
            # print count,len(result_list)
            count = len(result_list)
            rli = count -1
            wdi = -1
            while (rli !=0 and wdi !=-count) :
                # print rli,wdi,count
                # print  result_list[rli]['date']
                if Windcode_Dict[windcode][ktypestr][wdi]['date'] == result_list[rli]['date']:
                    break
                elif Windcode_Dict[windcode][ktypestr][wdi]['date'] < result_list[rli]['date']:
                    rli -=1
                else:
                    wdi -=1
            if Windcode_Dict[windcode][ktypestr][wdi]['date'] != result_list[rli]['date']: #must satisfy count>1
                continue
            wdi -= rli
            for index in range(count):
                if wdi < 0 :
                    Windcode_Dict[windcode][ktypestr][wdi] = result_list[index]
                else:
                    Windcode_Dict[windcode][ktypestr].append( result_list[index] )
                wdi +=1
    
    for windcode in Windcode_Dict:
        for ktype in ktype_list[:-1]:
            ktypestr = str(ktype)
            Windcode_Dict[windcode]['tech'+ktypestr] =  Stock.StocksTech(Windcode_Dict[windcode][ktypestr])
            ws = 0
            wp = 0
            wsl = 0 
            wbl = 0
            dtime = Windcode_Dict[windcode][ktypestr][-1]['date']
            close = Windcode_Dict[windcode][ktypestr][-1]['close']
            
            if ktype==2:
                stype = 2
                if Windcode_Stype[stype].get(windcode) is None :
                    continue
                print windcode,ktype,dtime,close,Windcode_Stype[stype][windcode]['warning_status']
                # Windcode_Dict[windcode]['tech'+ktypestr] = Stock.StocksTech(Windcode_Dict[windcode][ktypestr]) 
                # if Windcode_Dict[windcode]['tech'+ktypestr]['diff'][-1]>0 or Windcode_Dict[windcode]['tech'+ktypestr]['dea'][-1] >0:
                #     continue
                
                # if close ==0 :
                #     continue
                if Windcode_Dict[windcode]['tech8']['fast_k'][-1] <=Windcode_Dict[windcode]['tech8']['fast_d'][-1]:
                    continue   
                # print windcode,stype,"week pass"             
                # if Windcode_Dict[windcode]['warning_status']==0 and Windcode_Dict[windcode]['tech'+ktypestr]['fast_j'][-1] < 5:
                #     ws = 1
                #     Windcode_Dict[windcode].update({'warning_status':ws,'warning_ktype':ktype})

                if Windcode_Stype[stype][windcode]['warning_status']==1 and \
                    Windcode_Stype[stype][windcode]['warning_ktype'] ==6 and \
                    FourIndicator.GoldenDead(Windcode_Dict[windcode]['tech'+ktypestr]) == 4 :
                    ws = 4
                    Windcode_Stype[stype][windcode].update({'warning_status':ws,'warning_ktype':ktype,'warning_date':dtime})
                if ws==0:
                    continue
                try:
                    cursor.execute(sql,{
                        'sid':num,
                        'wtype':ws,
                        'wp':wp,
                        'wbl':wbl,
                        'wsl':wsl,
                        'stype':stype,
                        'ktype':ktype,
                        'name':Windcode_Stype[stype][windcode]['name'],
                        'code':windcode,
                        'price': close,
                        'wdate':dtime,    
                    })
                except Exception, e:
                    print 'WarningMessage to Database Error',e.message
                    print  windcode,ktype,dtime, ws ,stype
                    raise e
                    cursor.close()
                    cursor=Connector.cursor()
                    continue
                stocksWM.append( {
                    'id': num,
                    'windcode': windcode,
                    'name' : Windcode_Stype[stype][windcode]['name'],
                    'warning_status': ws,
                    'warnig_priority':wp,
                    'warning_buyinglevel':wbl,
                    'warning_sellinglevel':wsl,
                    'type' : stype,
                    'update_date' : dtime.strftime("%Y-%m-%d %H:%M:%S"),
                    'price'   : close ,                        
                    'is_strong': stocksLabel[windcode]['is_strong'],
                    'is_ready': stocksLabel[windcode]['is_ready'],
                    'is_bc': stocksLabel[windcode]['is_bc'],
                    'is_bv': stocksLabel[windcode]['is_bv'],
                    'is_jishu': stocksLabel[windcode]['is_jishu'],
                    'is_jiben': stocksLabel[windcode]['is_jiben'],
                    'is_mf': stocksLabel[windcode]['is_mf'],
                    'is_bs': stocksLabel[windcode]['is_bs'],
                    'is_br': stocksLabel[windcode]['is_br'],
                    'stars': stocksLabel[windcode]['stars'],
                })
                num = num  + 1   
            else :
                for stype in stype_list :
                    
                    if Windcode_Stype[stype].get(windcode) is None:
                        continue
                    # print windcode,ktype,dtime,close,Windcode_Stype[stype][windcode]['warning_status']
                    if Windcode_Stype[stype][windcode]['warning_status']==0 :
                        if stype<4 and stype!=1  and Windcode_Dict[windcode]['tech8']['fast_k'][-1] <= Windcode_Dict[windcode]['tech8']['fast_d'][-1] :
                            continue
                        print windcode,ktype,' stype=',stype,"week pass",Windcode_Dict[windcode]['tech'+ktypestr]['fast_j'][-1],Windcode_Dict[windcode]['tech'+ktypestr]['slow_j'][-1]
                        if ktype == 6 :
                            if stype==2 or stype==3:
                                if Windcode_Dict[windcode]['tech'+ktypestr]['fast_j'][-1] < 5:
                                    if stype==2:
                                        if Windcode_Dict[windcode]['tech8']['pb1'][-1]>Windcode_Dict[windcode]['tech8']['pb6'][-1] and \
                                           Windcode_Dict[windcode]['tech8']['pb2'][-1]>Windcode_Dict[windcode]['tech8']['pb6'][-1] :
                                            ws=1
                                        else:
                                            continue
                                    else:
                                        ws = 1
                                    Windcode_Stype[stype][windcode].update({'warning_status':ws,'type':stype,'warning_ktype':ktype,'warning_date':dtime,})
                            
                            else:
                                if Windcode_Dict[windcode]['tech'+ktypestr]['slow_j'][-1] < 5:
                                    if stype==1:
                                        if close>Windcode_Dict[windcode]['tech8']['pb6'][-1] :
                                            ws = 1
                                        else:
                                            continue
                                    else:
                                        ws = 1
                                    Windcode_Stype[stype][windcode].update({'warning_status':ws,'type':stype,'warning_ktype':ktype,'warning_date':dtime,})
                    else:
                        if ktype == 6 :
                            if Windcode_Stype[stype][windcode]['warning_status']==4 :
                                if Config.SellingEnable:
                                # short = FourIndicator.LongShort(Windcode_Dict[windcode]['tech'+ktypestr])
                                    rd = FourIndicator.RallyDivergence(Windcode_Dict[windcode]['tech'+ktypestr],Windcode_Dict[windcode][ktypestr])
                                    if rd==2 or rd==1:
                                        ws = 5
                                        if rd==2:
                                            wp = 2
                                        if rd==1:
                                            wp = 3
                                        Windcode_Stype[stype][windcode].update({'warning_status':ws,'type':stype,'warning_order':wo,'warning_ktype':ktype,'warning_date':dtime,})

                        if ktype ==  5 :
                            if Windcode_Stype[stype][windcode]['warning_status']==4:
                                if Config.SellingEnable and float(close)/Windcode_Stype[stype][windcode]['warning_price'] >=1.1:
                                    ws = 5
                                    wp = 0
                                    wsl = 3
                                    Windcode_Stype[stype][windcode].update({'warning_status':ws,'warning_priority':wp,'warning_sellinglevel':wsl,'type':0,'warning_ktype':ktype,'warning_price':close,'warning_date':dtime,})
                            if Windcode_Stype[stype][windcode]['warning_status'] ==1 :
                                if stype<4 and stype!=1 and Windcode_Dict[windcode]['tech8']['fast_k'][-1] <=Windcode_Dict[windcode]['tech8']['fast_d'][-1]:
                                    continue
                                if FourIndicator.GoldenDead(Windcode_Dict[windcode]['tech'+ktypestr]) == 4 :
                                    if Windcode_Stype[stype][windcode].get('gdbb') is None:
                                        Windcode_Stype[stype][windcode].update( {'gdbb':[4]} )
                                    else:
                                        Windcode_Stype[stype][windcode]['gdbb'].append(4)
                                
                                if Windcode_Stype[stype][windcode].get('gdbb') is None:
                                    continue
                                gl = len(Windcode_Stype[stype][windcode]['gdbb'])

                                if  stype==0 :
                                    if  gl==1 :
                                        ws = 4
                                        Windcode_Stype[stype][windcode].update({'warning_status':ws,'type':0,'warning_ktype':ktype,'warning_date':dtime,})               
                                else:
                                    if stype==3 :
                                        if gl==1:
                                            ws = 4
                                            Windcode_Stype[stype][windcode].update({'warning_status':ws,'type':3,'warning_ktype':ktype,'warning_date':dtime,})
                                    else:
                                        if gl==2 :
                                            if stype==1:
                                                if close>Windcode_Dict[windcode]['tech8']['pb6'][-1] :
                                                    ws = 4
                                                else:
                                                    continue
                                            else:                
                                                ws = 4
                                            Windcode_Stype[stype][windcode].update({'warning_status':ws,'type':stype,'warning_ktype':ktype,'warning_date':dtime,})
                
                    if ws ==0 :
                            continue                   
                    try:
                        cursor.execute(sql,{
                            'sid':num,
                            'wtype':ws,
                            'wp':wp,
                            'wbl':wbl,
                            'wsl':wsl,
                            'stype':stype,
                            'ktype':ktype,
                            'name':Windcode_Stype[stype][windcode]['name'],
                            'code':windcode,
                            'price': close,
                            'wdate':dtime,    
                        })
                    except Exception, e:
                        print 'WarningMessage to Database Error',e.message
                        print  windcode,ktype,dtime, ws ,stype
                        raise e
                        cursor.close()
                        cursor=Connector.cursor()
                        continue
                    stocksWM.append( {
                        'id': num,
                        'windcode': windcode,
                        'name' : Windcode_Stype[stype][windcode]['name'],
                        'warning_status': ws,
                        'warning_priority':wp,
                        'warning_buyinglevel':wbl,
                        'warning_sellinglevel':wsl,
                        'type' : stype,
                        'update_date' : dtime.strftime("%Y-%m-%d %H:%M:%S"),
                        'price'   : close ,                        
                        'is_strong': stocksLabel[windcode]['is_strong'],
                        'is_ready': stocksLabel[windcode]['is_ready'],
                        'is_bc': stocksLabel[windcode]['is_bc'],
                        'is_bv': stocksLabel[windcode]['is_bv'],
                        'is_jishu': stocksLabel[windcode]['is_jishu'],
                        'is_jiben': stocksLabel[windcode]['is_jiben'],
                        'is_mf': stocksLabel[windcode]['is_mf'],
                        'is_bs': stocksLabel[windcode]['is_bs'],
                        'is_br': stocksLabel[windcode]['is_br'],
                        'stars': stocksLabel[windcode]['stars'],
                    })
                    num = num  + 1  
    endtime = time.time()
    print  " Total runtime %s seconds" %( str(endtime-starttime1) )
    Connector.commit()
    cursor.close()
    Connector.close()
    del clientHq
    stocksWMRB ={}
    stocksWMRB['source'] = 1
    stocksWMRB['calc_date'] = current_datetime.strftime("%Y-%m-%d")
    stocksWMRB['wm_type']=0
    stocksWMRB['data'] = stocksWM
    with app.app_context():
        SendWarningMessage(  marshal(stocksWMRB, stock_wmrb_str_fields) ,url="http://www.hanzhendata.com/ihanzhendata/warning/rebuildworder" ) 
    
    

def SignalRebuild(builddate=None):
    if builddate is None:
        builddate = datetime.now()    
    SendWMflag = True
    # if SendWMflag:
    #     WarningMessage_Delete(dt)
    WarningMessage_SignalRebuild(builddate,SendWM=SendWMflag)

def SignalRebulidToFront(current_datetime):
    Connector = mysql.connector.connect(**Config.db_config)
    sql = "Insert into stocks_warning(id,wtype,wpriority,wsellinglevel,wbuyinglevel,stype,ktype,windcode,name,price,date) values(%(sid)s,%(wtype)s,%(wp)s,%(wsl)s,%(wbl)s,%(stype)s,%(ktype)s,%(code)s,%(name)s,%(price)s,%(wdate)s)"
    cursor = Connector.cursor()
    sql_rb = "select id,wtype,wpriority,wsellinglevel,wbuyinglevel,stype,ktype,windcode,name,price,date from  stocks_warning_rebuild where bdate=%(bdate)s "
    cursor_rb = Connector.cursor()
    cursor_rb.execute(sql_rb,{'bdate':current_datetime,})
    stocksWM = []
    num = 0
    for rid,rw,wp,wsl,wbl,rs,rk,windcode,name,price,date in cursor_rb:
        cursor.execute(sql,{
            'sid':rid,
            'wtype':rw,
            'wp':wp,
            'wsl':wsl,
            'wbl':wbl,
            'stype':rs,
            'ktype':rk,
            'name':name,
            'code':windcode,
            'price': price,
            'wdate':date,    
        })
        stocksWM.append( {
            'id': rid,
            'windcode': windcode,
            'name' : name,
            'warning_status': rw,
            'warning_priority':wp,
            'warning_sellinglevel':wsl,
            'warning_buyinglevel' :wbl,
            'type' : rs,
            'update_date' : date.strftime("%Y-%m-%d %H:%M:%S"),

            'price'   : price ,

            'is_strong': stocksLabel[windcode]['is_strong'],
            'is_ready': stocksLabel[windcode]['is_ready'],
            'is_bc': stocksLabel[windcode]['is_bc'],
            'is_bv': stocksLabel[windcode]['is_bv'],
            'is_jishu': stocksLabel[windcode]['is_jishu'],
            'is_jiben': stocksLabel[windcode]['is_jiben'],
            'is_mf': stocksLabel[windcode]['is_mf'],
            'is_bs': stocksLabel[windcode]['is_bs'],
            'is_br': stocksLabel[windcode]['is_br'],
            'stars': stocksLabel[windcode]['stars'],
        })  
    cursor_rb.close()
    Connector.commit()
    cursor.close()
    Connector.close()
    with app.app_context():
        SendWarningMessage(  [marshal(task, stock_wm_str_fields) for task in stocksWM] ) 

def PushSignal(current_datetime):
    Connector = mysql.connector.connect(**Config.db_config)
    cursor = Connector.cursor()
    sql="select distinct bdate from stocks_warning_rebuild where bdate=%(bdate)s order by bdate desc limit 1"
    cursor.execute(sql,{'bdate':current_datetime})
    if cursor.rowcount <1:
        cursor.close()
        return
    bdate = cursor.fetchone()[0]
    cursor.close()
    Connector.close()
    print bdate,current_datetime
    WarningMessage_Delete(bdate)
    SignalRebulidToFront(bdate)


def Sche_Init(scheduler):
    global stocks5MinuteNum
    stocks5MinuteNum = 0
    s1  = current_date.replace(hour=  9, minute=30, second= 30)
    e1  = current_date.replace(hour= 11, minute=30, second= 30)
    s2  = current_date.replace(hour= 13, minute=00, second= 30)
    e2  = current_date.replace(hour= 15, minute=00, second= 30)
    job_init1  = scheduler.add_job(func=WarningMessage_Minute, trigger='interval', minutes=5,start_date=s1,end_date=e1,replace_existing=True)
    job_init2  = scheduler.add_job(func=WarningMessage_Minute, trigger='interval', minutes=5,start_date=s2,end_date=e2,replace_existing=True)
    
if __name__ == '__main__':
    WindPy.start()

    
    # scheduler = BackgroundScheduler()
    scheduler = TornadoScheduler()
    current_date = datetime.now()
    print current_date
    scheduler._logger = logging
    job_init  = scheduler.add_job(id='Sche_Init',func=Sche_Init,args=(scheduler,), trigger='cron', hour=6,replace_existing=True)
    job_rbinit  = scheduler.add_job(id='rebuild',func=SignalRebuild, trigger='cron', hour=16,replace_existing=True)
    if current_date.hour>6:
        Sche_Init(scheduler)
    Init()
    scheduler.start() 
    
    # WarningMessage_Minute()
    # dt = datetime.now()-timedelta(days=3)
    # SignalRebuild(builddate= dt)
    # PushSignal(dt)

    print datetime.now()
    print "Tdx Server start-----------------"
    

    container = WSGIContainer(app)
    server = Application([      
        (r'.*', FallbackHandler, dict(fallback=container))
    ])
    server.listen(3308)
    IOLoop.instance().start()






    


