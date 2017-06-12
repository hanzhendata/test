#!/usr/bin/env python
# -*- coding: utf-8 -*-
'''
from WindPy import *
w.start()
wsdata =w.wsi('000001.SH','open,high,low,close,volume,amt','2016-09-28 09:00:00','2016-10-02 11:37:53',  "BarSize=1;Fill=Previous;PriceAdj=F")
if wsdata.ErrorCode !=0:
	print 	"ErrorCode=",wsdata.ErrorCode
else :
	f =  open("000001SH.txt",'w')
	for wtime in wsdata.Times: 
		index  = wsdata.Times.index(wtime)
		buf = wtime.strftime('%Y-%m-%d %H:%M:%S')
		for j in range(0,6):
			buf= buf + ""+  str(wsdata.Data[j][index])
		buf = buf + "\n"
		f.write(buf)
	f.close()
w.stop()

#EMA12,EMA26,DIF,DEA,MACD
import  mysql.connector
import decimal
config = {
  'user': 'root',
  'password': 'toortoor',
  'host': '127.0.0.1',
  'database': 'LieFeng',f
  #'raise_on_warnings': True,
  'buffered':True,
}
cnx = mysql.connector.connect(**config)
cursor = cnx.cursor()    
sql = " select close_price from stocks_history where prod_code='000001.SH' and  ktype=0 and date='"
f = open('000001.txt','r')
fw = open('compare.txt','w')

for line in f.readlines():
         sl = line.split(',')
         cursor.execute(sql +sl[0] + "'")
	 if (cursor.rowcount!=0):
		  wind = cursor.fetchone()[0] 
		  fw.write(sl[0] + ' ' + str(decimal.Decimal(sl[4])-wind) + '\n' )
cursor.close()
cnx.close()
f.close()
fw.close()
		


import  mysql.connector
import  Stock
import datetime
config = {
  'user': 'root',
  'password': 'toortoor',
  'host': '127.0.0.1',
  'database': 'LieFeng',
  #'raise_on_warnings': True,
  'buffered':True,
}
cnx = mysql.connector.connect(**config)
cursor = cnx.cursor()    
sql = " insert into stocks_board(sectorid,name,type,windcode,update_date) values(%(id)s,%(name)s,%(type)s,%(windcode)s,%(update_date)s)"
f = open('Z:/Outsourcing/LieFeng/requirement/board.txt','r')
update_date = datetime.datetime.now().strftime("%Y-%m-%d")
index = 1
for line in f.readlines() :
	sl = line.split(',')
	sl = map(str.strip, sl )
	if sl==[''] :
		continue
	if  len(sl)>1:		
		name = sl[1]
		sid  = sl[0]
		windcode = sl[2]
		st = sl[3]
	data ={
    	'id':sid ,
    	'name':name,
    	'windcode':windcode,
    	'type': st,
    	'update_date' : update_date, 
    }
	cursor.execute(sql , data)
	Stock.StocksBoardList(sid,update_date,cnx)
cnx.commit()
cursor.close()
cnx.close()
f.close()

import Config
import mysql.connector
import datetime,time
from WindPy import w 
import numpy as np

def CheckWindData(wsdata):
	if wsdata.ErrorCode !=0:
		print 	"ErrorCode=",wsdata.ErrorCode
		print   "ErrorData ",wsdata.Data
		return False
	return True
cnx = mysql.connector.connect(**Config.config)

#set windcode
windcode_set = set()
stocks_board_set = set()
MarketType = 1
sql = "select  name, windcode,sectorid from stocks_board"	
sql_list = "select name,windcode from stocks_boardlist where sectorid=%(sectorid)s"

cursor = cnx.cursor()
cursor_l = cnx.cursor()
cursor.execute(sql)
for bname,windcode,sectorid in cursor:		
	stocks_board_set.add(windcode)
	cursor_l.execute(sql_list,{'sectorid':sectorid})
	for lname,lwindcode in cursor_l:
		windcode_set.add(lwindcode)		
cursor_l.close()

	
# cursor.close()
w.start()
now = datetime.datetime.now()
maxcode_set= set()
wsdata=w.wset("sectorconstituent","date="+now.strftime("%Y-%m-%d")+";sectorid=a001010100000000")
if not Config.CheckWindData(wsdata):
	exit()
for index in range(0,len(wsdata.Data[0])):
	# print wsdata.Data[1][index]
	maxcode_set.add(wsdata.Data[1][index])
ktype = 8
num = 1
maxcode_set.update(stocks_board_set)
for windcode in maxcode_set:
	
	print num,windcode
	num+=1
	# continue
	ss = time.time()
	ws = w.wss(windcode,"basedate,ipo_date")
	if ws.Data[0][0]>ws.Data[1][0]:
		starttime = ws.Data[0][0]
	else:
		starttime = ws.Data[1][0]
	if starttime.year <1900 :
		continue
	wsdata = w.wsd( windcode , "open,high,low,close,volume,pct_chg,chg",starttime,now, "showblank=0;Period=W;PriceAdj=F")
	se = time.time()
	print 'WindPy %s runs %0.2f seconds.' % (windcode, (se - ss))
	if not CheckWindData(wsdata):		
		continue
	# print wsdata
	wslength = len(wsdata.Times)
	filename ="temp.txt"
	f = open(filename,'w')
	ss = time.time()
	insert_num = 0
	# fbuffer = ""
	for index in range(0,wslength): 
		wtime  = wsdata.Times[index]
		# print insert_num					
		temp = "%s,%d,%s,%f,%f,%f,%f,%f,%f,%f\n" % (windcode,ktype,
			wtime.strftime("%Y-%m-%d %H:%M:%S"),wsdata.Data[0][index],
			wsdata.Data[1][index],wsdata.Data[2][index],wsdata.Data[3][index],
			wsdata.Data[4][index],wsdata.Data[5][index],wsdata.Data[6][index])
		insert_num += 1
		
		f.write(temp)
	# f.write(fbuffer)
	f.close()
	print insert_num
	se = time.time()
	print 'FileSave  %s runs %0.2f seconds.' % (windcode, (se - ss))
	ss = time.time()
	cursor.execute("START TRANSACTION;")
	cursor.execute("SET autocommit=0;")
	#data to liefeng database	
	temp ="load data local infile '"+filename+"' ignore into table stocks_history_days character set utf8 fields terminated by ','  lines terminated by '\n' (`windcode`,`ktype`,`date`,`open`,`high`,`low`,`close`,`volume`,`pct_chg`,`chg`);"
	cursor.execute(temp)
	cursor.execute("COMMIT;")
	se = time.time()
	print 'Mysql  %s runs %0.2f seconds.' % (windcode, (se - ss))
cnx.close()

windcode_all = ""
	
f= open('zl.txt','w')
	
for windcode,wname in cursor :
	windcode_all  += ";" +windcode
windcode_all = windcode_all[1:]	
newin_code=[]
inc_code=[]
wsb = w.wss(windcode_all,"holder_pctbyssfund","rptDate=20160630")
wsc = w.wss(windcode_all,"holder_pctbyssfund","rptDate=20160930")
td = w.tdays("2016-06-30", "2016-09-30", "")
sql = "select high,low from stocks_history_days where ktype=7 and date=%(date)s and windcode=%(code)s"
for index in range( len(wsc.Codes)):
	windcode = wsc.Codes[index]
	if not math.isnan( wsc.Data[0][index] ) and  wsc.Data[0][index] > 1:
		high = 0 
		low = 100000
		for ti in range(len(td.Times)):
			da = {
				'date':td.Times[ti],
				'code':windcode,
			}
			cursor.execute(sql,da)
			temp = cursor.fetchone()
			if high< temp[0]:
				high= temp[0]
			if low > temp[1]:
				low= temp[1]
		price = (high+low) /2
		if (high-price)/price*100>15:
			continue
		if math.isnan(wsb.Data[0][index]) :
			newin_code.append( windcode )
			continue
		if wsc.Data[0][index] > wsb.Data[0][index] :
			inc_code.append( windcode )
f.write("New %d \n" %(len(newin_code)))
for code in newin_code:
	f.write(code+"\n")
f.write("Inc %d \n" %(len(inc_code)))
for code in inc_code:
	f.write(code+"\n")
'''

import Config
import mysql.connector
from datetime import datetime,timedelta,time
from WindPy import w 
import numpy 
import math
# import time
import Stock,FourIndicator,Slice
cnx = mysql.connector.connect(**Config.config)


w.start()

def Warning_Init():
	
	Windcode_Dict = {}
	# global Connector
	MarketType = 1
	sql = "select  name, windcode,stype from stocks_sr"   

	cursor = cnx.cursor()
	num = 0
	cursor.execute(sql)
	for bname,windcode,stype in cursor:  
	    Windcode_Dict [windcode] = {'name':bname,'type':stype}
	    
	    Windcode_Dict[windcode].update({'warning_status':0,})
	    num += 1
	    # if num>6:
	    #     break       
	# sql  = "select type,windcode from stocks_mainforce"
	# cursor.execute(sql)
	# for bname,windcode in cursor:      
	#     Windcode_Dict [windcode] = {'name':bname,}
	cursor.close()
	print "stocks warning status init-------------------------------"
	print len(Windcode_Dict)
	ktype_list= [ 2,6 ]
	cursor = cnx.cursor()
	sql = "Insert into stocks_warning(id,wtype,stype,ktype,windcode,name,date) values(%(sid)s,%(w)s,%(s)s,%(k)s,%(code)s,%(name)s,%(wdate)s)"
	for ktype in ktype_list :       
	    tablename = 'stocks_history_minutes'
	    for windcode in  Windcode_Dict  :
	        # print windcode
	        ktypestr = str(ktype)
	        indicator = Stock.StocksOHLC_N(windcode,tablename,ktype,cnx,limit=5000)
	        Windcode_Dict[windcode].update( { ktypestr : indicator,})
	        #    'tech'+str(ktype): StocksTech(indicator),} )
	        Windcode_Dict[windcode]['tech'+ ktypestr] = Stock.StocksTech(Windcode_Dict[windcode][ ktypestr ]) 
    
	        
	        
	        for index in range( len(Windcode_Dict[windcode]['tech'+ ktypestr]) ) :
				wtc=w.tdayscount(Windcode_Dict[windcode][ktypestr][-index-1]['date'],Windcode_Dict[windcode][ktypestr][-1]['date'], "")
				if wtc.ErrorCode ==0 :
					if wtc.Data[0][0] > 10 :
						break
				if ( ktype==2 and Windcode_Dict[windcode]['type']==2 and Windcode_Dict[windcode]['tech'+ktypestr]['fast_j'][-index-1] < 5 )	or ( ktype==6 and Windcode_Dict[windcode]['type']!=2 and Windcode_Dict[windcode]['tech'+ktypestr]['slow_j'][-index-1] < 5 ):
						print windcode,ktype,Windcode_Dict[windcode][ktypestr][-index-1]['date']
						Windcode_Dict[windcode].update({'ktype':ktype,'warning_status':1,'wdate':Windcode_Dict[windcode][ktypestr][-index-1]['date']}) 
	for windcode in  Windcode_Dict  :
		if Windcode_Dict[windcode]['warning_status'] !=0 :
			cursor.execute(sql,{
				'sid':0,
				'w':1,
				's':Windcode_Dict[windcode]['type'],
				'k':Windcode_Dict[windcode]['ktype'],
				'name':Windcode_Dict[windcode]['name'],
				'code':windcode,
				'wdate':Windcode_Dict[windcode]['wdate']	
				})
			print windcode,Windcode_Dict[windcode]['wdate']	
	cnx.commit()	           
	w.stop()

def Slice0():
	cursor = cnx.cursor()
	select_cursor = cnx.cursor()
	tablename = "stocks_history_minutes"
	prod_code = '600895.SH'
	sql = "select date,open,high,low,close,volume,amount,pct_chg,chg from "+tablename+" where windcode=%(prod_code)s and ktype=0 order by date asc" 
	sql_data= {
		#'tablename' :tablename,
		'prod_code' : prod_code,
	} 
	UpdateFlag = True
	if UpdateFlag :
		filename ="temp.txt"
		f = open(filename,'w')	
	
	
	 
	for ktype in [2,5,6] :
		rl=[]
		if (Config.CheckKtype(ktype,rl)!=0):
			continue		
		Barsize = rl[0]
		SingleTime = rl[1]	
					 
		zero_sql = "select date,open,high,low,close,volume,amount,pct_chg,chg from "+tablename+" where windcode=%(prod_code)s and ktype=0 and date>%(date)s and date<%(edate)s order by date asc"
		sql_data['date']  = '2017-03-05 15:00:00'#begindate.strftime('%Y-%m-%d %H:%M:%S')
		sql_data['edate'] = '2017-03-07 09:00:00'
		cursor.execute(zero_sql, sql_data)
		if cursor.rowcount==0 :
			continue
		indicator = []
		high1 =  []
		low1 = []
		tempdict = {}

		if(ktype != 6 ):
			for date,open_price,high_price,low_price,close_price,volume,amount,pct_chg,chg in cursor:
				minute = date.time().minute
				hour  = date.time().hour
				high1.append(high_price)
				low1.append(low_price)			
				if (minute % Barsize) == 1 :
					tempdict['open'] = open_price
				elif (minute % Barsize) == 0 :		
					if len(high1)!=Barsize :
						high1 = []
						low1 = []
						continue
					tempdict[ 'close' ] = close_price
					tempdict[ 'volume'] = volume
					tempdict[ 'amount'] = amount
					tempdict[ 'chg'   ] = chg
					tempdict[ 'pct_chg'] = pct_chg
					if hour==13 and minute == 0 :
						date = date.replace(hour=11,minute=30)
					tempdict['date'] = date
					tempdict['high'] = max(high1)
					tempdict['low'] = min(low1)
					indicator.append(tempdict)
					high1 = []
					low1  = []
					tempdict = {}
		else:
			for date,open_price,high_price,low_price,close_price,volume,amount,pct_chg,chg in cursor:
				minute = date.time().minute
				hour  = date.time().hour
				high1.append(high_price)
				low1.append(low_price)
				if ( hour<12 and minute % (Barsize) == 31 ) or (hour>11 and minute % Barsize == 1)  :
					tempdict['open'] = open_price
				elif ( hour<12 and minute % (Barsize) == 30 ) or (hour>11 and minute % Barsize == 0) :		
					if len(high1)!=Barsize :
						if hour<12 and minute == 0 :
							continue
						high1 = []
						low1 = []
						continue
					tempdict['close'] = close_price
					tempdict[ 'volume'] = volume
					tempdict[ 'amount'] = amount
					tempdict[ 'chg'   ] = chg
					tempdict[ 'pct_chg'] = pct_chg
					if hour==13 and minute == 0 :
						date = date.replace(hour=11,minute=30)
					tempdict['date'] = date
					tempdict['high'] = max(high1)
					tempdict['low'] = min(low1)
					if hour==15:
						print high1
						print low1
						print max(high1),min(low1)

					indicator.append(tempdict)
					high1 = []
					low1  = []
					tempdict = {}
		if UpdateFlag :
			insert_num = 0	
			for current in  indicator  : 			
				# print insert_num					
				temp = "%s,%d,%s,%f,%f,%f,%f,%f,%f,%f,%f\n" % (prod_code,ktype,
					current['date'].strftime("%Y-%m-%d %H:%M:%S"),current['open'],
					current['high'],current['low'],current['close'],
					current['volume'],current['amount'],current['pct_chg'], current['chg'] )
				insert_num += 1				
				f.write(temp)
			# print ktype,insert_num
		del indicator,high1,low1,tempdict
	
	if UpdateFlag :
		f.close()
		cursor.execute("START TRANSACTION;")
		cursor.execute("SET autocommit=0;")
		#data to liefeng database	
		temp ="load data local infile '"+filename+"' replace into table stocks_history_minutes character set utf8 fields terminated by ','  lines terminated by '\n'  (`windcode`,`ktype`,`date`,`open`,`high`,`low`,`close`,`volume`,`amount`,`pct_chg`,`chg`);"
		cursor.execute(temp)
		cursor.execute("COMMIT;")
	cursor.close()

def Slice0(windcode,begindate,enddate,ktype_list):
	cursor = cnx.cursor()
	select_cursor = cnx.cursor()
	tablename = "stocks_history_minutes"
	prod_code = windcode
	
	sql_data= {
		#'tablename' :tablename,
		'prod_code' : prod_code,
	} 
	UpdateFlag = True
	if UpdateFlag :
		filename ="temp.txt"
		f = open(filename,'w')	
	
	
	 
	for ktype in ktype_list :
		rl=[]
		if (Config.CheckKtype(ktype,rl)!=0):
			continue		
		Barsize = rl[0]
		SingleTime = rl[1]	
					 
		zero_sql = "select date,open,high,low,close,volume,amount,pct_chg,chg from "+tablename+" where windcode=%(prod_code)s and ktype=0 and date>%(date)s and date<%(edate)s order by date asc"
		sql_data['date']  = begindate.strftime('%Y-%m-%d %H:%M:%S')
		sql_data['edate'] = enddate.strftime('%Y-%m-%d %H:%M:%S')#'2017-03-07 09:00:00'
		cursor.execute(zero_sql, sql_data)
		if cursor.rowcount==0 :
			continue
		indicator = []
		high1 =  []
		low1 = []
		tempdict = {}

		if(ktype != 6 ):
			for date,open_price,high_price,low_price,close_price,volume,amount,pct_chg,chg in cursor:
				minute = date.time().minute
				hour  = date.time().hour
				high1.append(high_price)
				low1.append(low_price)			
				if (minute % Barsize) == 1 :
					tempdict['open'] = open_price
				elif (minute % Barsize) == 0 :		
					if len(high1)!=Barsize :
						high1 = []
						low1 = []
						continue
					tempdict[ 'close' ] = close_price
					tempdict[ 'volume'] = volume
					tempdict[ 'amount'] = amount
					tempdict[ 'chg'   ] = chg
					tempdict[ 'pct_chg'] = pct_chg
					if hour==13 and minute == 0 :
						date = date.replace(hour=11,minute=30)
					tempdict['date'] = date
					tempdict['high'] = max(high1)
					tempdict['low'] = min(low1)
					indicator.append(tempdict)
					high1 = []
					low1  = []
					tempdict = {}
		else:
			for date,open_price,high_price,low_price,close_price,volume,amount,pct_chg,chg in cursor:
				minute = date.time().minute
				hour  = date.time().hour
				high1.append(high_price)
				low1.append(low_price)
				if ( hour<12 and minute % (Barsize) == 31 ) or (hour>11 and minute % Barsize == 1)  :
					tempdict['open'] = open_price
				elif ( hour<12 and minute % (Barsize) == 30 ) or (hour>11 and minute % Barsize == 0) :		
					if len(high1)!=Barsize :
						if hour<12 and minute == 0 :
							continue
						high1 = []
						low1 = []
						continue
					tempdict['close'] = close_price
					tempdict[ 'volume'] = volume
					tempdict[ 'amount'] = amount
					tempdict[ 'chg'   ] = chg
					tempdict[ 'pct_chg'] = pct_chg
					if hour==13 and minute == 0 :
						date = date.replace(hour=11,minute=30)
					tempdict['date'] = date
					tempdict['high'] = max(high1)
					tempdict['low'] = min(low1)
					# if hour==15:
					# 	print high1
					# 	print low1
					# 	print max(high1),min(low1)

					indicator.append(tempdict)
					high1 = []
					low1  = []
					tempdict = {}
		if UpdateFlag :
			insert_num = 0	
			for current in  indicator  : 			
				# print insert_num					
				temp = "%s,%d,%s,%f,%f,%f,%f,%f,%f,%f,%f\n" % (prod_code,ktype,
					current['date'].strftime("%Y-%m-%d %H:%M:%S"),current['open'],
					current['high'],current['low'],current['close'],
					current['volume'],current['amount'],current['pct_chg'], current['chg'] )
				insert_num += 1				
				f.write(temp)
			# print ktype,insert_num
		del indicator,high1,low1,tempdict
	
	if UpdateFlag :
		f.close()
		cursor.execute("START TRANSACTION;")
		cursor.execute("SET autocommit=0;")
		#data to liefeng database	
		temp ="load data local infile '"+filename+"' replace into table stocks_history_minutes character set utf8 fields terminated by ','  lines terminated by '\n'  (`windcode`,`ktype`,`date`,`open`,`high`,`low`,`close`,`volume`,`amount`,`pct_chg`,`chg`);"
		cursor.execute(temp)
		cursor.execute("COMMIT;")
	cursor.close()

def UpdateSlice(windcode):
	cursor = cnx.cursor()
	select_cursor = cnx.cursor()
	sql = "select date,windcode from stocks_history_minutes where ktype=0 and close=0 and windcode=%(code)s order by date asc"
	dd= {}
	ktype = 0 
	Barsize =  1
	ktype_list = [2,4,5,6]
	SingleTime = timedelta(minutes=Barsize)
	cursor.execute(sql,{'code':windcode})
	for mdate,mcode in cursor:
		if mdate is None:
			print windcode ,"has none date!!"
			continue
		dd[mdate.date()] = mcode
	cursor.close()
	for mdate in dd:
		starttime = datetime.combine(mdate,time.min)
		starttime = starttime.replace(hour=9,minute=0)
		endtime  = datetime.combine(mdate,time.min)
		endtime = endtime.replace(hour=15,minute=30)
		print starttime, endtime
		# Stock.SingleUpdateMinutes_LoadDataInfile(windcode,0,select_cursor,w,starttime,endtime,SingleTime,Barsize)	
		para = "BarSize="+str(Barsize)+";Fill=Previous;PriceAdj=F"
	
		ss =  datetime.now()
		
		wsdata = w.wsi( windcode , "open,high,low,close,volume,amt,chg,pct_chg",starttime,endtime,para )
		se =  datetime.now()
		print 'WindPy %s runs %0.2f seconds.' % (windcode, (se - ss).total_seconds())
		if not Config.CheckWindData(wsdata):		
			continue
		# print wsdata
		wslength = len(wsdata.Times)
		filename ="temp.txt"
		f = open(filename,'w')
		ss =  datetime.now()
		insert_num = 0
		# fbuffer = ""
		for index in range(0,wslength): 
			wtime  = wsdata.Times[index]
			# print insert_num					
			temp = "%s,%d,%s,%f,%f,%f,%f,%f,%f,%f,%f\n" % (windcode,ktype,
				wtime.strftime("%Y-%m-%d %H:%M:%S"),wsdata.Data[0][index],
				wsdata.Data[1][index],wsdata.Data[2][index],wsdata.Data[3][index],
				wsdata.Data[4][index],wsdata.Data[5][index],wsdata.Data[6][index],wsdata.Data[7][index])
			insert_num += 1
			
			f.write(temp)
		# f.write(fbuffer)
		f.close()
		print insert_num
		se = datetime.now()
		print 'FileSave  %s runs %0.2f seconds.' % (windcode, (se - ss).total_seconds())
		ss = datetime.now()
		select_cursor.execute("START TRANSACTION;")
		select_cursor.execute("SET autocommit=0;")
		#data to liefeng database	
		temp ="load data local infile '"+filename+"' replace into table stocks_history_minutes character set utf8 fields terminated by ','  lines terminated by '\n' (`windcode`,`ktype`,`date`,`open`,`high`,`low`,`close`,`volume`,`amount`,`pct_chg`,`chg`);"
		select_cursor.execute(temp)
		select_cursor.execute("COMMIT;")
		se = datetime.now()
		print 'Mysql  %s runs %0.2f seconds.' % (windcode, (se - ss).total_seconds())
		Slice(windcode,starttime,endtime,ktype_list)

	select_cursor.close()
def Test():
	cursor = cnx.cursor()
	select_cursor = cnx.cursor()
	wd={}
	select_sql = "select windcode,name from stocks_code"
	select_cursor.execute(select_sql)
	for windcode,name in select_cursor:
		wd[windcode] = name
	tablename = "stocks_history_days"
	w.start()
	sd = w.tdays('1990-12-01','2017-05-20','Period=W')
	sql = "select windcode,date from "+tablename+" where ktype=8 and windcode=%(windcode)s order by date" 
	el = {}
	for windcode in wd :
		cursor.execute(sql,{'windcode':windcode})
		for windcode,wdate in cursor:
			if wdate in sd.Data[0]:
				continue
			if el.get(windcode) is None:
				el[windcode] = [wdate]
			else:
				el[windcode].append(wdate)
	for windcode in el:
		print windcode,min(el[windcode])

	ek = el.keys()
	print min(ek),max(ek),len(ek)
	cnx.commit()
	select_cursor.close()	
	cursor.close()

def Update_All():
	wd = {}
	cursor = cnx.cursor()
	sql = "select windcode,date from stocks_code where windcode>='300056.SZ' order by date"
	cursor.execute(sql)
	for windcode,ktype in cursor:
		if windcode in wd:
			continue
		UpdateSlice(windcode)
		wd[ windcode ] = ktype
	cursor.close()

def Tech8Ananysis(windcode_list,datetime):
    cursor = cnx.cursor()
    ktype_list= [8]
    for windcode in windcode_list:
        for ktype in ktype_list :
            if ktype > 6:
                tablename = 'stocks_history_days'
            else:       
                tablename = 'stocks_history_minutes'
                    
            ktypestr = str(ktype)
            
            indicator = Stock.StocksOHLC_N(windcode,tablename,ktype,cnx,limit=2000)       
            if ktype==8:
                wsd=w.wsd(windcode,"open,high,low,close",datetime,datetime,"Period=W;PriceAdj=F")
                indicator.append({
                    'date':wsd.Times[0],
                    'open':wsd.Data[0][0],
                    'high':wsd.Data[1][0],
                    'low' :wsd.Data[2][0],
                    'close':wsd.Data[3][0],
                    })
            tech = Stock.StocksTech(indicator)
            for i in range(len(indicator)) :
            	index = -i-1
            	if indicator[index]['date'].date() == datetime.date() or (index>-10 and ktype==8) :
            		print windcode,indicator[index]
            		print 'dea diff macd',tech['dea'][index],tech['diff'][index], tech['macd'][index]
            		print 'fast k d j',tech['fast_k'][index],tech['fast_d'][index], tech['fast_j'][index]
            		print 'slow k d j',tech['slow_k'][index],tech['slow_d'][index], tech['slow_j'][index]
            		print 'pb 1 2 6 ',tech['pb1'][index],tech['pb2'][index], tech['pb6'][index]

def TechAnanysis(windcode,ktype,datetime):
    cursor = cnx.cursor()
    
    if ktype > 6:
        tablename = 'stocks_history_days'
    else:       
        tablename = 'stocks_history_minutes'
            
    ktypestr = str(ktype)
    
    indicator = Stock.StocksOHLC_N(windcode,tablename,ktype,cnx,limit=2000)       
    
    indicator.append({
            'date': "2017-05-24 10:30:00",
            'open': 25.33,
            'high': 25.83,
            'low' : 24.92,
            'close': 25.78,
            })
    indicator.append({
            'date': "2017-05-24 11:30:00",
            'open': 25.4,
            'high': 26,
            'low' : 24.92,
            'close': 25.39,
            })
    tech = Stock.StocksTech(indicator)
    for i in range(len(indicator)) :
        index = -i-1
        if index>-10 :
            print windcode,indicator[index]
            print 'dea diff macd',tech['dea'][index],tech['diff'][index], tech['macd'][index]
            print 'fast k d j',tech['fast_k'][index],tech['fast_d'][index], tech['fast_j'][index]
            print 'slow k d j',tech['slow_k'][index],tech['slow_d'][index], tech['slow_j'][index]
            print 'pb 1 2 6 ',tech['pb1'][index],tech['pb2'][index], tech['pb6'][index]


def BreakVolume():
    cursor = cnx.cursor()
    cursor.execute("select windcode,name,stype,date from stocks_sr where stype=3 order by date desc")
    windcode_list = {}
    for windcode,sname,stype,sdate in cursor:
        if windcode_list.get(sdate) is None:
            windcode_list[sdate] = [ windcode ]
        else :
            windcode_list[sdate].append( windcode )
    current_date = datetime.now()
    rtn = {}
    for sdate in windcode_list:
        ktype = 8 
        if ktype > 6:
            tablename = 'stocks_history_days'
        else:       
            tablename = 'stocks_history_minutes'
                 
        ktypestr = str(ktype)
        for windcode in windcode_list[sdate] :
            indicator = Stock.StocksOHLC_N(windcode,tablename,ktype,cnx,limit=2000)       
            if ktype==8:
                wsd=w.wsd(windcode,"open,high,low,close",current_date,current_date,"Period=W;PriceAdj=F")
                print windcode,wsd
                if wsd.ErrorCode==0 and indicator[-1]['date'] < wsd.Times[0] :
                    indicator.append({
                        'date':wsd.Times[0],
                        'open':wsd.Data[0][0],
                        'high':wsd.Data[1][0],
                        'low' :wsd.Data[2][0],
                        'close':wsd.Data[3][0],
                        })
            tech = Stock.StocksTech(indicator)
            if tech['diff'][-1] >0 and tech['pb1'][-1] >tech['pb6'][-1] and tech['pb2'][-1]> tech['pb6'][-1] :
                if rtn.get(sdate) is None :
                    rtn[sdate] = [ windcode  ] 
                else:
                    rtn[sdate].append( windcode  )
    print rtn
    remains=[]
    sql = "select id,stype,ktype,date,windcode,name,pct_chg,cap from stocks_sr where stype=3 and date=%(sdate)s and windcode= %(code)s"
    for sdate in rtn:
        for windcode in rtn[sdate]:
            cursor.execute(sql,{'sdate':sdate,'code':windcode})
            for sid,stype,ktype,sdate,windcode,name,pct_chg,cap in cursor:
                remains.append({
                    'id':sid,
                    'stype':stype,
                    'ktype':ktype,
                    'date':sdate,
                    'windcode': windcode,
                    'name':name,
                    'pct_chg':pct_chg,
                    'cap':float(cap),
                    }
                )
    cursor.execute("delete from stocks_sr where stype=3")
    # sql = "insert into stocks_sr(id,stype,ktype,date,windcode,name,pct_chg,cap) valuse(%(id)s,%(stype)s,%(ktype)s,%(sdate)s,%(windcode)s,%(name)s,%(pct_chg)s,%(scap)s)  "
    sql = "Replace into stocks_sr(id,stype,ktype,date,windcode,name,pct_chg,cap) values(%(id)s,%(stype)s,%(ktype)s,%(date)s,%(windcode)s,%(name)s,%(pct_chg)s,%(cap)s)"
    for rem in remains:
        cursor.execute(sql,rem)
    cnx.commit()
    cursor.close()
                
def Slice1():
    cursor = cnx.cursor()
    cursor.execute("select windcode,name,date from stocks_code order by date desc")
    windcode_list = {}
    for windcode,sname,sdate in cursor:
        if windcode_list.get(sdate) is None:
            windcode_list[sdate] = [ windcode ]
        else :
            windcode_list[sdate].append( windcode )
    ktype_list = [2,4,5,6]

    for sdate in windcode_list:
        tablename = 'stocks_history_minutes'
        
        for windcode in windcode_list[sdate] :
            Slice.SliceMinuteUpdate_New(windcode,ktype_list,cnx)
            

    cnx.commit()
    cursor.close()
          
# windcode_dict  = {}
# Slice()
# UpdateZero('600895.SH')
# Update_All()
# Warning_Init()
# TechAnanysis(['603989.SH','600463.SH','000333.SZ','000504.SZ','300288.SZ'],datetime.now())
Tech8Ananysis([ '603199.SH'],datetime.now()-timedelta(days=0))
# BreakVolume()
# Slice1()
# Tech8Ananysis([ '002573.SZ','000888.SZ','300023.SZ','300288.SZ','300418.SZ','300307.SZ','600803.SH','603977.SH','603199.SH'],datetime.now()-timedelta(days=0))
# TechAnanysis('002365.SZ',6,datetime.now())
# current_date = datetime.now()	
# datestr = current_date.strftime("%Y-%m-%d")		
# wsdata=w.wset("sectorconstituent","date="+datestr+";sectorid=a001010100000000")
# if  Config.CheckWindData(wsdata):


# 	for index in range(0,len(wsdata.Data[0])):		
# 		sdate = wsdata.Data[0][index]
# 		da = {
# 				'code' : wsdata.Data[1][index],
# 				'name' : wsdata.Data[2][index],
# 				'date' : sdate,
# 			}
		
# 		windcode_dict  [da['code'] ] = { 'name':da['name'],} 
# Stock.StocksBasicUpdate(windcode_dict,cnx,w)

# windcode = '884210.WI'
# tablename  = 'stocks_history_days'
# indicator7 =Stock.StocksOHLC_N(windcode,tablename,8,cnx)
# high = [ indicator7[x]['high'  ] for x in range(0, len(indicator7) ) ] 
# low  = [ indicator7[x]['low'   ] for x in range(0, len(indicator7) ) ] 
# close =[ indicator7[x]['close' ] for x in range(0, len(indicator7) ) ] 
# high = numpy.array(high, dtype = numpy.float)
# low  = numpy.array(low , dtype = numpy.float)
# close= numpy.array(close,dtype = numpy.float)
# 			# volume= numpy.array(volume,dtype=numpy.float)
# macd=FourIndicator.MACD_Default(close)
# kdj_fast = FourIndicator.KDJ_Default(high,low,close)
# kdj_slow = FourIndicator.KDJ_Default(high,low,close,fastk_period=21,  slowk_period=9,  fastd_period =9)	
# print kdj_fast['K']
# diff = macd['DIFF']
# dea  = macd['DEA' ] 
# kdj_fast_k = kdj_fast['K'] 
# kdj_fast_d = kdj_fast['D']
# kdj_slow_k = kdj_slow['K'] 
# kdj_slow_d = kdj_slow['D']  
# pb = FourIndicator.PB(close)
# pb1 = pb['PB1'] 
# pb2 = pb['PB2'] 
# pb6 = pb['PB6'] 
# print " diff %f dea %f fast_k %f fast_d %f  pb1 %f pb2 %f  pb6 %f \n" %(diff[-1],dea[-1],kdj_fast_k[-1],kdj_fast_d[-1],pb1[-1],pb2[-1],pb6[-1])
# print kdj_fast_d