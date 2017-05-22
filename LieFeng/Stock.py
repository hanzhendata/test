#!/usr/bin/env python
# -*- coding: utf-8 -*-
#2017.1.1 StocksMainForceCalc finished and debug valid 
from WindPy import *
from datetime import date, datetime, timedelta
import gc
import mysql.connector
import FourIndicator
import numpy
import math
import os
from Config import *
# need remove update section to speed up.

def StocksHistoryAllUpdate(ktype,base_days= 365*2):
		#check parameter
		rl=[]
		if (CheckKtype(ktype,rl)!=0):
			return
		if(base_days<0):
			return
		
		Barsize = rl[0]
		SingleTime = rl[1]
		td = timedelta(days=base_days)
		#prepare database environment variable
		cnx = mysql.connector.connect(**config)
		cursor = cnx.cursor()
		cursor_history = cnx.cursor()
		cursor_maxdate = cnx.cursor()

		endtime = datetime.now().date()
		w.start()
                
	
		
		starttime = endtime - td

		cnx = mysql.connector.connect(**config)

		
		cursor = cnx.cursor()
		cursor_history = cnx.cursor()
		cursor_maxdate = cnx.cursor()
		query =[]
		query.append ("select finance_mic,prod_code "
			"from prodcode where hq_type_code='ESA.M' or hq_type_code='ESB' ")
		query.append("select finance_mic,prod_code from"
			     " prodcode where hq_type_code='ESA.SMSE' and finance_mic='SZ' ")
		query.append("select finance_mic,prod_code from"
			     " prodcode where hq_type_code='ESA.GEM' and finance_mic='SZ' ")


		for sql in query:   
		   cursor.execute(sql)
		   for (finance_mic,prod_code) in cursor:
		       code="{}.{}".format(prod_code, finance_mic )
		       SingleUpdate(code,ktype,cursor_history,w,starttime,endtime,1)	       
		       cnx.commit()
		w.stop()
		cursor_history.close()    
		cursor.close()
		cnx.close()

def StocksBoardHistoryAllUpdate(ktype,base_days= 365*2):
		#check parameter
		rl=[]
		if (CheckKtype(ktype,rl)!=0):
			return
		if(base_days<0):
			return
		
		Barsize = rl[0]
		SingleTime = rl[1]
		td = timedelta(days=base_days)
		#prepare database environment variable
		cnx = mysql.connector.connect(**config)
		cursor = cnx.cursor()
		cursor_history = cnx.cursor()
		cursor_maxdate = cnx.cursor()

		endtime = datetime.now().date()
		w.start()
                
	
		
		starttime = endtime - td

		cnx = mysql.connector.connect(**config)

		
		cursor = cnx.cursor()
		cursor_history = cnx.cursor()
		
		sql = "select windcode from stocks_board"

		  
		cursor.execute(sql)
		for code in cursor:	       
			SingleUpdate(code,ktype,cursor_history,w,starttime,endtime,1)	       
			cnx.commit()
		
		cursor_history.close()    
		cursor.close()
		cnx.close()
		w.stop()

def StocksHistoryUpdate(windcode_set,ktype,Connect,WindPy,base_days= 365*3,minflag=True,LoadFileFlag=False):
		#check parameter
		rl=[]
		if (CheckKtype(ktype,rl)!=0):
			return
		if(base_days<0):
			return
		
		Barsize = rl[0]
		SingleTime = rl[1]
		
		#prepare database environment variable
		# cnx = mysql.connector.connect(**config)
		cursor = Connect.cursor()
		cursor_history = Connect.cursor()
		markettype = 1 

		endtime = datetime.now()#.date()
		# w.start()	
		td = timedelta(days=base_days)
		wsdata = WindPy.tdaysoffset(-1*base_days, endtime.strftime("%Y-%m-%d"), "")
		if not CheckWindData(wsdata) :
			starttime = endtime - td
		else:
			starttime = wsdata.Data[0][0]		
		

		cursor = Connect.cursor()
		cursor_history = Connect.cursor()
		if minflag :
			if LoadFileFlag :
				for windcode in windcode_set:
					print " code= ",windcode," ktype=",ktype				      
					SingleUpdateMin(windcode,ktype,cursor_history,WindPy,starttime,endtime,td,SingleTime,Barsize,markettype)	       
					Connect.commit()
			else:
				for windcode in windcode_set:
					print " code= ",windcode," ktype=",ktype				      
					SingleUpdateMin_LoadDataInfile(windcode,ktype,cursor_history,WindPy,starttime,endtime,td,SingleTime,Barsize,markettype)	       
					Connect.commit()
		else :
			for windcode in windcode_set:
				print " code= ",windcode," ktype=",ktype				      
				SingleUpdate(windcode,ktype,cursor_history,WindPy,starttime,endtime,td,SingleTime,Barsize,markettype)	       
				Connect.commit()			
		# if ktype > 0 and ktype<7:	
		# 	for windcode in windcode_set:
		# 		print " code= ",windcode," ktype=",ktype				      
		# 		SingleUpdateMin(windcode,ktype,cursor_history,WindPy,starttime,endtime,td,SingleTime,Barsize,markettype)	       
		# 		Connect.commit()
		# else :
		#  	for windcode in windcode_set:
		# 		print " code= ",windcode," ktype=",ktype
		# 		SingleUpdate(windcode,ktype,cursor_history,WindPy,starttime,endtime,td,SingleTime,Barsize,markettype)
		# 		Connect.commit()
		# cursor_history.close()    
		cursor.close()
		# Connect.close()
		# w.stop()

def StocksDaysHistoryUpdate(Windcode_Dict,ktype,Connect,WindPy,base_days= 365*30):
	#check parameter
	rl=[]
	if (CheckKtype(ktype,rl)!=0):
		return
	if(base_days<0):
		return
	# print rl
	Barsize = rl[0]
	SingleTime = rl[1]
	

	cursor = Connect.cursor()
	
	markettype = 1 

	endtime = datetime.now()#.date()

	td = timedelta(days=base_days)
	wsdata = WindPy.tdaysoffset(-1*base_days, endtime, "")
	if not CheckWindData(wsdata) :
		starttime = endtime - td
	else:
		starttime = wsdata.Data[0][0]		
	
	wd = {}
	
	for windcode in Windcode_Dict:
		
		select_sql = "select date from stocks_history_days where ktype=%(ktype)s and windcode=%(code)s order by date desc limit 1"
		select_data = {
			'code' : windcode ,
			'ktype': ktype,
		}		        
		cursor.execute(select_sql,select_data)
		if (cursor.rowcount>0) :
			temp = cursor.fetchone()[0]
			if temp in wd:
				wd[temp] += ";" + windcode
			else:
				wd[temp] =  windcode

		 	
	
	
	if ktype==7:
		para = "showblank=0;PriceAdj=F"
	else:
		para = "showblank=0;Period="+Barsize+";PriceAdj=F"
	
	print para
	filename = 'temp_days.txt'
	for wdate in wd:
		ws = []
		starttime = wdate
		print starttime,endtime
		if (endtime-starttime).days<SingleTime.days:
			continue
		if (endtime-starttime).days==SingleTime.days and (endtime-starttime).seconds <= 19*3600:
			continue
		starttime += SingleTime
		# print wd[wdate]
		for mt in [ "open","high","low","close","volume","pct_chg","chg","mkt_cap_ard"]:
			wsdata = WindPy.wsd( wd[wdate] , mt,starttime,endtime,para )
			if not CheckWindData(wsdata):		
				continue		
			ws.append( wsdata.Data )

			times = wsdata.Times
			codes = wsdata.Codes
			del wsdata	
		# print wsdata
		if len(ws)==0 :
			continue
		wslength = len(times)
		
		f = open(filename,'w')
		# print codes
		insert_num = 0
		# fbuffer = ""
		tdays=WindPy.tdays(starttime,endtime,'Period=W')
		if not CheckWindData(tdays):
			continue
		if wslength == 1 :
			for cindex  in range(len(codes)):
				windcode = codes[cindex]			
				wtime  = times[0]
				if ws[0][0][cindex] == 0 and ws[1][0][cindex] == 0 and ws[2][0][-1] == 0 :	
					continue	
				if ws[4][0][cindex] == 0 and ws[5][0][cindex] == 0 and ws[6][0][cindex] == 0 :
					continue
				if ktype!=7 and (endtime.date()==wtime.date() ):
					if  (wtime-starttime).days<SingleTime.days :
							continue
					
				temp = "%s,%d,%s,%f,%f,%f,%f,%f,%f,%f,%f\n" % (windcode,ktype,
						wtime.strftime("%Y-%m-%d %H:%M:%S"),ws[0][0][cindex],
						ws[1][0][cindex],ws[2][0][cindex],ws[3][0][cindex],
						ws[4][0][cindex],ws[5][0][cindex],ws[6][0][cindex],ws[7][0][cindex])
				insert_num += 1
			
				f.write(temp)
		else:
			if ws[0][0][-1] == 0 and ws[1][0][-1] == 0 and ws[2][0][-1] == 0 and ws[0][-1][-1] == 0 and ws[1][-1][-1] == 0 and ws[2][-1][-1] == 0:
				wsl = wslength -1
			else:
				wsl = wslength

			for cindex  in range(len(codes)):
				windcode = codes[cindex]			
				for index in range(0,wsl): 
					wtime  = times[index]

					if ws[4][cindex][index] == 0 and ws[5][cindex][index] == 0 and ws[6][cindex][index] == 0 :
						continue
					if ws[4][cindex][index] == 0 :
						continue
					if index == wsl-1 :
							if ktype!=7 and (endtime.date()==wtime.date() ):
								if  (wtime-tdays.Data[0][-2]).days<SingleTime.days :
									continue			
					temp = "%s,%d,%s,%f,%f,%f,%f,%f,%f,%f,%f\n" % (windcode,ktype,
						wtime.strftime("%Y-%m-%d %H:%M:%S"),ws[0][cindex][index],
						ws[1][cindex][index],ws[2][cindex][index],ws[3][cindex][index],
						ws[4][cindex][index],ws[5][cindex][index],ws[6][cindex][index],ws[7][cindex][index])
					insert_num += 1
				
					f.write(temp)
			wtime = times[-1]
		f.close()
		print insert_num
		
		if insert_num == 0 :
			continue
		cursor.execute("START TRANSACTION;")
		cursor.execute("SET autocommit=0;")
		#data to liefeng database	
		temp ="load data local infile '"+filename+"' ignore into table stocks_history_days character set utf8 fields terminated by ','  lines terminated by '\n' (`windcode`,`ktype`,`date`,`open`,`high`,`low`,`close`,`volume`,`pct_chg`,`chg`,`cap`);"
		cursor.execute(temp)
		cursor.execute("COMMIT;")
	
	# os.remove(filename)
				                                 
	Connect.commit()	
   	
	cursor.close()

def StocksMinutesHistoryUpdate(Windcode_Dict,ktype,Connect,WindPy,base_days= 365*3):
	#check parameter
	rl=[]
	if (CheckKtype(ktype,rl)!=0):
		return
	if(base_days<0):
		return
	
	Barsize = rl[0]
	SingleTime = rl[1]
	

	cursor = Connect.cursor()
	cursor_history = Connect.cursor()	

	endtime = datetime.now()#.date()

	td = timedelta(days=base_days)
	wsdata = WindPy.tdaysoffset(-1*base_days, endtime.strftime("%Y-%m-%d"), "")
	if not CheckWindData(wsdata) :
		starttime = endtime - td
	else:
		starttime = wsdata.Data[0][0]		
	print starttime
	for windcode in Windcode_Dict:
		print " code= ",windcode," ktype=",ktype				      
		SingleUpdateMinutes_LoadDataInfile(windcode,ktype,cursor_history,WindPy,starttime,endtime,SingleTime,Barsize)			                                 
		# Connect.commit()	
	cursor_history.close()
	cursor.close()

def StocksBasicYearData(YearNum,Year,norm,para,Type,Windcode_All,Connect,WindPy):
	cursor  = Connect.cursor()
	 
	for yd in range(YearNum):
		mat = para+"="+str(Year-1-yd)+"1231" 
		# print mat
		md = datetime.now().replace(Year-1-yd,12,31)
		sql = "select quarter from stocks_basic where btype=%(btype)s order by quarter desc limit 1"
		cursor.execute(sql,{'btype':Type,})
		if cursor.rowcount != 0 :
			quarter = cursor.fetchone()[0]
			if yd != 0 and quarter > md :
				continue
		wss = WindPy.wss(Windcode_All,norm, mat )	
		if  not CheckWindData(wss):
			return				
			
		for index  in range(len(wss.Codes)):
			windcode = wss.Codes[index]			
			if wss.Data[0][index] is None or math.isnan( wss.Data[0][index] ):
				continue
			sql = "Replace into stocks_basic(windcode,quarter,btype,content) values (%(windcode)s,%(quarter)s,%(type)s,%(value)s)"
			data = {
				'windcode':windcode,
				'quarter': "%d-12-31" %(Year-1-yd),
				'type': Type,
				'value': wss.Data[0][index]
			}
			cursor.execute(sql,data)
	Connect.commit()
	cursor.close()

def StocksBasicUpdate(Windcode_Dict,Connect,WindPy):
	current_date = datetime.now()
	year =  current_date.year
	month = current_date.month
	day = current_date.day 
	wssd=WindPy.tdays("%d-%d-%d" %(year-2,month,day), "%d-%d-%d" %(year,month,day), "Days=Alldays;Period=Q")
	format=[]
	if not CheckWindData(wssd):
		return 
	
	cursor = Connect.cursor()
	windcode_all = ""
	for windcode in Windcode_Dict:
		windcode_all  += ";" +windcode
	windcode_all = windcode_all[1:]
	
	for mi in range( len(wssd.Data[0])-1 ):
		md = wssd.Data[0][mi]
		qdstr = md.strftime("%Y-%m-%d")
		print md	
		sql = "select quarter from stocks_basic where btype=4 order by quarter desc limit 1"
		cursor.execute(sql)
		if cursor.rowcount != 0 :
			quarter = cursor.fetchone()[0]
			if mi != len(wssd.Data[0])-2 and quarter> md :
				continue
		mat = "rptDate="+md.strftime("%Y%m%d")+";rptType=1;currencyType=" 
		
		wss = WindPy.wss(windcode_all, "yoy_tr,grossprofitmargin,yoyocf,debttoassets,wgsd_net_inc,wgsd_oper_cf,yoyprofit",mat)
		
		if  not CheckWindData(wss):
			continue				
		
		for index  in range(len(wss.Codes)):
			windcode = wss.Codes[index]			
			for j in range(4):				
				if wss.Data[j][index] is None or math.isnan(wss.Data[j][index]):
					continue
				sql = "Replace into stocks_basic(windcode,quarter,btype,content) values (%(windcode)s,%(quarter)s,%(type)s,%(value)s)"
				data = {
					'windcode':windcode,
					'quarter': qdstr,
					'type': j,
					'value': wss.Data[j][index]
				}
				# print data
				cursor.execute(sql,data)
			if wss.Data[4][index] is None or wss.Data[5][index] is None or math.isnan(wss.Data[4][index]) or math.isnan(wss.Data[5][index] ) :
				continue
			sql = "Replace into stocks_basic(windcode,quarter,btype,content) values (%(windcode)s,%(quarter)s,%(type)s,%(value)s)"
			data = {
					'windcode':windcode,
					'quarter': qdstr,
					'type': 4,
					'value': wss.Data[5][index]/wss.Data[4][index]*100,
				}
			cursor.execute(sql,data)
			if wss.Data[6][index] is None or math.isnan(wss.Data[6][index]) :
				continue
			sql = "Replace into stocks_basic(windcode,quarter,btype,content) values (%(windcode)s,%(quarter)s,%(type)s,%(value)s)"
			data = {
					'windcode':windcode,
					'quarter': qdstr,
					'type': 11,
					'value': wss.Data[6][index],
				}
			cursor.execute(sql,data)
			
		Connect.commit()	
	
	for md in wssd.Data[0][-4:-1]:		
		qdstr = md.strftime("%Y-%m-%d")
		print 'mainforce',md	
		sql = "select quarter from stocks_basic where btype=13 order by quarter desc limit 1"
		cursor.execute(sql)
		if cursor.rowcount != 0 :
			quarter = cursor.fetchone()[0]
			if mi != len(wssd.Data[0])-2 and quarter> md :
				continue
		mat = "rptDate="+md.strftime("%Y%m%d") 
		
		wss = WindPy.wss(windcode_all, "holder_pctbyssfund",mat)
		
		if  not CheckWindData(wss):
			continue				
		
		for index  in range(len(wss.Codes)):
			windcode = wss.Codes[index]			
			if  wss.Data[0][index] is None or math.isnan(wss.Data[0][index]) :
				continue
			sql = "Replace into stocks_basic(windcode,quarter,btype,content) values (%(windcode)s,%(quarter)s,%(type)s,%(value)s)"
			data = {
					'windcode':windcode,
					'quarter': qdstr,
					'type': 13,
					'value': wss.Data[0][index],
				}
			cursor.execute(sql,data)
			
		Connect.commit()	

	StocksBasicYearData(6,year,"roe_basic"        ,"rptDate"  ,5 ,windcode_all,Connect,WindPy)
	StocksBasicYearData(6,year,"grossprofitmargin","rptDate"  ,10,windcode_all,Connect,WindPy)
	StocksBasicYearData(1,year,"or_ttm"           ,"tradeDate",12,windcode_all,Connect,WindPy)
	
	tdo = WindPy.tdaysoffset(-1, current_date, "")
	if not CheckWindData(tdo):
		return	
	mat = "tradeDate="+tdo.Data[0][0].strftime("%Y%m%d")

	wss=WindPy.wss(windcode_all, "ev,or_ttm,pe_ttm",mat)
	if  not CheckWindData(wss):
		return				
	mat = tdo.Data[0][0].strftime("%Y-%m-%d")
	for index  in range(len(wss.Codes)):
		windcode = wss.Codes[index]	
		for j in range(3):		
			if math.isnan( wss.Data[j][index] ):
				continue
			sql = "Replace into stocks_basic(windcode,quarter,btype,content) values (%(windcode)s,%(quarter)s,%(type)s,%(value)s)"
			data = {
				'windcode':windcode,
				'quarter': mat,
				'type': 6+j,
				'value': wss.Data[j][index]
			}
			cursor.execute(sql,data)
	Connect.commit()
	md = current_date.date().replace(year-1,12,31)
	sql = "select date from stocks_basic_other where typename='SEGMENT_SALES' order by date desc limit 1"
	cursor.execute(sql)
	if cursor.rowcount != 0 :
		quarter = cursor.fetchone()[0]
		if quarter> md :
			return

	norm = "segment_sales"
	mat = "rptDate=%d1231;order=2" %(year-1)
	wss = WindPy.wss(windcode_all,norm, mat )	
	if  not CheckWindData(wss):
		return				
		
	for index  in range(len(wss.Codes)):
		windcode = wss.Codes[index]	
		for fi in range(len(wss.Fields)):		
			# if math.isnan( wss.Data[fi][index] ):
			# 	continue
			sql = "Replace into stocks_basic_other(windcode,date,typename,content) values (%(windcode)s,%(quarter)s,%(type)s,%(value)s)"
			data = {
				'windcode':windcode,
				'quarter': "%d-12-31" %(year-1),
				'type': wss.Fields[fi],
				'value': wss.Data[fi][index]
			}
		cursor.execute(sql,data)
	Connect.commit()

	norm = "briefing,ipo_date"	
	wss = WindPy.wss(windcode_all,norm )	
	if  not CheckWindData(wss):
		return				
		
	for index  in range(len(wss.Codes)):
		windcode = wss.Codes[index]	
		for fi in range(len(wss.Fields)):		
			# if math.isnan( wss.Data[fi][index] ):
			# 	continue
			sql = "Replace into stocks_basic_other(windcode,date,typename,content) values (%(windcode)s,%(quarter)s,%(type)s,%(value)s)"
			data = {
				'windcode':windcode,
				'quarter': current_date,
				'type': wss.Fields[fi],
				'value': wss.Data[fi][index]
			}
		cursor.execute(sql,data)
	Connect.commit()



def GetTableName(MarketType,KType):
	if (MarketType==0):
		TableName = "futures_history"
	else:
		if KType>6:
			TableName = "stocks_history_days"
		else:
			TableName = "stocks_history_minutes"
	return TableName

# @timer
def StocksOHLC_N(windcode,tablename,ktype,connect,limit=None,StartDate=None):
	if ktype >6 :
		select_sql = "select date,open,high,low,close,volume,pct_chg,chg,cap from "+tablename+" where windcode=%(code)s and ktype=%(ktype)s "
	else:	
		select_sql = "select date,open,high,low,close,volume,pct_chg,chg,amount  from "+tablename+" where windcode=%(code)s and ktype=%(ktype)s "
	
	select_data = {
	   'code':str(windcode),
	   'ktype':ktype,
	   # 'limit':limit,
	}
	if StartDate is not None:
		select_sql += " and date> %(date)s" 
		select_data.update({'date':StartDate,})	
	if limit is not None:
		select_sql += " order by date desc limit %d " %(limit)
	else:
		select_sql += " order by date asc"
	cursor = connect.cursor()
	cursor.execute(select_sql,select_data)
	indicator=[]
	if ktype > 6:
		for sdate,sopen,shigh,slow,sclose,svolume,spct,schg,scap in cursor:
			if limit is not None:
				indicator.insert(0,{
					'date':sdate,
					'open':sopen,
					'high':shigh,
					'low' :slow,
					'close':sclose,
					'volume':svolume,
					'pct_chg':spct,
					'chg':schg,
					'cap':scap,
				})
			else:
				indicator.append({
					'date':sdate,
					'open':sopen,
					'high':shigh,
					'low' :slow,
					'close':sclose,
					'volume':svolume,
					'pct_chg':spct,
					'chg':schg,
					'cap':scap,
				})
	else:
		for sdate,sopen,shigh,slow,sclose,svolume,spct,schg,samount in cursor:
			if limit is not None:
				indicator.insert(0, {
					'date':sdate,
					'open':sopen,
					'high':shigh,
					'low' :slow,
					'close':sclose,
					})
			else:
				indicator.append({
					'date':sdate,
					'open':sopen,
					'high':shigh,
					'low' :slow,
					'close':sclose,
					})
	return indicator

def StocksTech(Indicator,Default=True,Limit=None):
	Insert_Before_Num = 500

	high = [ Indicator[x]['high'  ] for x in range(0, len(Indicator) ) ] 
	low  = [ Indicator[x]['low'   ] for x in range(0, len(Indicator) ) ] 
	close =[ Indicator[x]['close' ] for x in range(0, len(Indicator) ) ] 
	# volume =[ Indicator[x]['volume' ] for x in range(0, len(Indicator) ) ] 
	if Default:
		
		high = numpy.array(high, dtype = numpy.float)
		low  = numpy.array(low , dtype = numpy.float)
		close= numpy.array(close,dtype = numpy.float)
	    # volume= numpy.array(volume,dtype=numpy.float)
    	
		macd=FourIndicator.MACD_Default(close)
		kdj_fast = FourIndicator.KDJ_Default(high,low,close)
		kdj_slow = FourIndicator.KDJ_Default(high,low,close,fastk_period=21,  slowk_period=9,  fastd_period =9)
	else:

		high0 = [  high[0]  ] * Insert_Before_Num
		low0  = [   low[0]  ] * Insert_Before_Num
		close0 =[ close[0]  ] * Insert_Before_Num
		# volume0 =[ volume[0]  ] * Insert_Before_Num
		high0.extend(high)
		low0.extend(low)
		close0.extend(close)
		# volume0.extend(volume)
		high = numpy.array(high0, dtype = numpy.float)
		low  = numpy.array(low0 , dtype = numpy.float)
		close= numpy.array(close0,dtype = numpy.float)
		# volume= numpy.array(volume0,dtype=numpy.float)
		del high0 , low0 , close0# , volume0
		macd=FourIndicator.MACD(close)
		kdj_fast = FourIndicator.KDJ(high,low,close)
		kdj_slow = FourIndicator.KDJ(high,low,close,fastk_period=21,  slowk_period=9,  fastd_period =9)
	diff = macd['DIFF']
	dea  = macd['DEA' ] 
	bar  = macd['MACD']
	kdj_fast_k = kdj_fast['K'] 
	kdj_fast_d = kdj_fast['D']
	kdj_fast_j = kdj_fast['J']
	kdj_slow_k = kdj_slow['K'] 
	kdj_slow_d = kdj_slow['D']
	kdj_slow_j = kdj_slow['J']
	pb = FourIndicator.PB(close)
	pb1 = pb['PB1'] 
	pb2 = pb['PB2'] 
	pb6 = pb['PB6'] 
	# vma = FourIndicator.MA(volume,time_period=30)
	if Limit is None:
		rtn_num = 2000 
	else:
		rtn_num = Limit 
	rtn =  {
	        'diff' : diff[-rtn_num: ] ,
	        'dea'  : dea [-rtn_num: ] ,
	        'macd' : bar [-rtn_num: ] ,
	        # 'vma'  : vma [-rtn_num: ] ,
	        'fast_k':kdj_fast_k[-rtn_num: ],
	        'fast_d':kdj_fast_d[-rtn_num: ],
	        'fast_j':kdj_fast_j[-rtn_num: ],
	        'slow_k':kdj_slow_k[-rtn_num: ],
	        'slow_d':kdj_slow_d[-rtn_num: ],
	        'slow_j':kdj_slow_j[-rtn_num: ],
	        'pb1':pb1[-rtn_num: ],
	        'pb2':pb2[-rtn_num: ],
	        'pb6':pb6[-rtn_num: ],
	        'isStrong':False,
	        'isReady' :False,
	        }
	del high,low,close#,volume
	del diff,dea,bar,macd
	del kdj_fast_k,kdj_fast_d,kdj_fast_j,kdj_fast
	del kdj_slow_k,kdj_slow_d,kdj_slow_j,kdj_slow
	del pb1,pb2,pb6,pb
	return rtn

def StocksOHLC(windcode,MarketType,ktype,connect):
	if (MarketType==0):
		tablename = "futures_history"
	else:
		if ktype>6:
			tablename = "stocks_history_days"
			select_sql = "select date,open,high,low,close from "+tablename+" where prod_code=%(code)s and ktype=%(ktype)s order by date asc"
		else:
			tablename = "stocks_history"
			select_sql = "select date,open_price,high_price,low_price,close_price from "+tablename+" where prod_code=%(code)s and ktype=%(ktype)s order by date asc"
	select_data = {
	   'code':windcode,
	   'ktype':ktype,
	}	
	cursor = connect.cursor()
	cursor.execute(select_sql,select_data)
	indicator=[]
	for sd,so,sh,sl,sc in cursor:
		indicator.append({
			'date':sd,
			'open':so,
			'high':sh,
			'low' :sl,
			'close':sc,
			})
	return indicator

def StocksBoardImport(FileName,Connect,WindPy):
	
	cursor = Connect.cursor()    
	sql = " insert into stocks_board(sectorid,name,type,windcode,update_date) values(%(id)s,%(name)s,%(type)s,%(windcode)s,%(update_date)s)"
	f = open(FileName,'r')#'E:/Outsourcing/LieFeng/requirement/board.txt'
	update_date = datetime.now().strftime("%Y-%m-%d")
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
		StocksBoardList(sid,update_date,Connect,WindPy)
	Connect.commit()
	cursor.close()	
	f.close()

def StocksBoardList(sectorid,update_date,Connect,WindPy):	
	
	sql = "date=%s;sectorid=%s" % (update_date,sectorid)
	wsdata = WindPy.wset("sectorconstituent",sql)
	if wsdata.ErrorCode != 0 or len(wsdata.Data)<2:
		return
	cursor = Connect.cursor()
	sql = "Replace into stocks_boardlist(id,sectorid,windcode,name) Values(%(id)s,%(sectorid)s,%(windcode)s,%(name)s)"
	for i in range(0, len(wsdata.Data[0]) ):
		data = {
			'id': i,
			'sectorid':sectorid,
			'windcode':wsdata.Data[1][i],
			'name': wsdata.Data[2][i]
		}
		cursor.execute(sql,data)
	Connect.commit()
	cursor.close()	

# def StocksBoardStrongCompare(board):
# 	rtn = 0
# 	if board['pb1'] > board['pb2'] and board['pb2'] > board['pb6']:
# 		rtn = 30
# 	return rtn+ board['days']

def StocksBoardStrongCompare(board):
	rtn = 0
	if board['pb1'][-1] > board['pb2'][-1] and board['pb2'] [-1]> board['pb6'][-1]:
		rtn = 30
	return rtn+ board['days']

def StocksTradeDay(current_date,Windpy):
	wsdata = WindPy.tdays(current_date,current_date,"")
	if wsdata.ErrorCode != 0 :
		return False
	if wsdata.Data is None :
		return False
	else:
		return True

def StocksGetLastData(windcode,ktype,MarketType,current_date,Connect,WindPy):
	if (MarketType==0):
		tablename = "futures_history"
	else:
		tablename = "stocks_history"
	boardlist = []	
	if ktype != 8 :
		return None
	
	last_date = []
	for i in range(1,8):
		td = timedelta(days=i)
		temp = current_date - td
		if (temp.strftime('%U') == current_date.strftime('%U')) :
			last_date.append(temp)
		else:
			break;
	if len(last_date) == 0 :
		return None 
		# if StocksTradeDay(current_date,WindPy):
		# 	return None
		# sql_history  ="select close_price from "+ tablename +" where ktype=%(ktype)s and prod_code=%(code)s order by date desc limit 1"
		# cursor_history = Connect.cursor()
		# cursor_history.execute(sql_history,{
		# 	'ktype':ktype-1,
		# 	'code':windcode,
		# })
		# temp =	cursor_history.fetchone()[0]
		# return {
		# 	'open' :temp,
		# 	'high' :temp,
		# 	'low'  :temp,
		# 	'close':temp,
		# }
		
	sql = "select open_price,high_price,low_price,close_price from "+ tablename +" where ktype=%(ktype)s and prod_code=%(code)s and date>=%(date)s order by date asc  "
	indicator = []
	cursor = Connect.cursor()
	para = {
		'ktype':ktype-1,
		'code':windcode,
		'date':last_date[-1].date(),
		}
	cursor.execute(sql,para)
	for  open_price,high_price,low_price,close_price in cursor :
		indicator.append({
			'open' :open_price,
			'high' :high_price,
			'low'  :low_price,
			'close':close_price,
			})
	cursor.close()
	if len(indicator) == 0 :		
		return None
	return {'open' :indicator[0]['open'],
			'high' :max([x['high'] for x in indicator ]),
			'low'  :min([x['low']  for x in indicator ]),
			'close':indicator[-1]['close'],}

def StocksSortVolume(stocklist,WindPy):
	enddate= datetime.now()
	wsdata = WindPy.tdaysoffset(-29, enddate, "")
	if not CheckWindData(wsdata) :
		return stocklist
	startdate = wsdata.Data[0][0]
	for index in range(0, len(stocklist)):
		wsdata = WindPy.wsd(stocklist[index]['windcode'], "VMA,volume", startdate, enddate, "VMA_N=30;PriceAdj=F")
		# print wsdata
		if not CheckWindData(wsdata) :
			stocklist[index]['days'] = 0
		else:
			if wsdata.Data[0][0] is None :
				stocklist[index]['days'] = 0
			else:		
				stocklist[index]['days'] = len([x for x in range(len(wsdata.Data[0])) if (wsdata.Data[1][x]/100)>(wsdata.Data[0][x])] )
	
	return  sorted(stocklist, key=StocksBoardStrongCompare,reverse=True)


def StocksBoardData(ktype,MarketType,Connect,WindPy):
	if (MarketType==0):
		tablename = "futures_history"
	else:
		tablename = "stocks_history"
	boardlist = []
	
	cursor = Connect.cursor()
	sql = "select  name, windcode,sectorid,type  from stocks_board"
	cursor.execute(sql)
	cursor_history = Connect.cursor()
	current_date = datetime.now()
	wsdata=WindPy.tdaysoffset(-1, current_date, "")	
	# print wsdata	
	enddatestr= wsdata.Data[0][0].strftime("%Y-%m-%d")	
	# valid = CheckDateTimeValid(MarketType,current_date,WindPy)
	for bname,windcode,sectorid,btype  in cursor:
		# if valid :
		last_data=StocksGetLastData(windcode,ktype,MarketType,current_date,Connect,WindPy)
		if last_data is not None :					
			sql_history = "select open_price,high_price,low_price,close_price from "+ tablename +" where ktype=%(ktype)s and prod_code=%(code)s order by date asc  "	
			cursor_history.execute(sql_history,{
				'ktype':ktype,
				'code':windcode,
				})	
			# print windcode
			indicator = []				
			for open_price,high_price,low_price,close_price in cursor_history:
				indicator.append({
					'open' :open_price,
					'high' :high_price,
					'low'  :low_price,
					'close':close_price,
					})				
			indicator.append(last_data)
			high = numpy.array([ indicator[x]['high' ] for x in range(0, len(indicator) ) ], dtype = numpy.float)
			low  = numpy.array([ indicator[x]['low'  ] for x in range(0, len(indicator) ) ], dtype = numpy.float)
			close =numpy.array([ indicator[x]['close'] for x in range(0, len(indicator) ) ], dtype = numpy.float)
			macd=FourIndicator.MACD(close)
			kdj_fast = FourIndicator.KDJ(high,low,close)
			kdj_slow = FourIndicator.KDJ(high,low,close,fastk_period=21,  slowk_period=9,  fastd_period =9)
			diff = macd['DIFF']
			dea  = macd['DEA' ] 
			kdj_fast_k = kdj_fast['K'] 
			kdj_fast_d = kdj_fast['D']
			kdj_slow_k = kdj_slow['K'] 
			kdj_slow_d = kdj_slow['D'] 
			pb = FourIndicator.PB(close)
			pb1 = pb['PB1'] 
			pb2 = pb['PB2'] 
			pb6 = pb['PB6'] 
		else:
			sql_history = "select diff,dea,pb1,pb2,pb6,fast_k,fast_d ,slow_k,slow_d "
			
			sql_history += " from "+tablename+" where ktype=%(ktype)s and prod_code=%(code)s order by date desc limit 300"
			cursor_history.execute(sql_history,{
				'ktype':ktype 	,
				'code' :windcode,
			})
			diff = dea = kdj_fast_k = kdj_fast_d = kdj_slow_k = kdj_slow_d = pb1 = pb2 = pb6 = []
			for h_diff,h_dea,h_pb1,h_pb2,h_pb6,h_kdj_fast_k,h_kdj_fast_d,h_kdj_slow_k,h_kdj_slow_d in cursor_history :
				diff.insert(0,h_diff)
				dea.insert (0,h_dea )
				pb1.insert (0,h_pb1 )
				pb2.insert (0,h_pb2 )
				pb6.insert (0,h_pb6 )
				kdj_fast_k.insert(0,h_kdj_fast_k)
				kdj_fast_d.insert(0,h_kdj_fast_d)
				kdj_slow_k.insert(0,h_kdj_slow_k)
				kdj_slow_d.insert(0,h_kdj_slow_d)
		
		# if diff>dea and dea>0  and kdj_k>kdj_d and (pb1>pb6 and pb2>pb6): #dif>dea

		wsdata=WindPy.wsd(windcode, "pct_chg,mkt_cap_ard", enddatestr, enddatestr, "Period=W;PriceAdj=F")
		boardlist.append({
				'name':bname,
				'windcode': windcode,
				'sectorid': sectorid,
				'update_date' :current_date,
				'pct_chg' : wsdata.Data[0][0],
				'cap': wsdata.Data[1][0],
				'type' : btype,
				'diff' : diff ,
				'dea'  : dea  ,
				'fast_k':kdj_fast_k,
				'fast_d':kdj_fast_d,
				'slow_k':kdj_slow_k,
				'slow_d':kdj_slow_d,
				'pb1':pb1,
				'pb2':pb2,
				'pb6':pb6,
				'isStrong':False,
				'isReady' :False,
				})

	cursor_history.close()
	cursor.close()
	return boardlist

def StocksGetLastDate(ktype,current_date):
	last_date = []
	if ktype ==8:		
		for i in range(1,6):
			td = timedelta(days=i)
			temp = current_date - td
			if (temp.strftime('%U') == current_date.strftime('%U')) :
				last_date.append(temp)
			else:
				break;
	return last_date	
	
def StocksGetLastData_New(board,ktype,last_date):
	
	boardlist = []	

	ds='ta'+str(ktype-1)
	length = len(last_date)
	indicator = []
	if length == 0 or len(board[ds])<length:
		return indicator
	for index in range(length):
		index =  (length-index)*-1
		indicator.append({
			'open' :board[ds][index]['open' ],
			'high' :board[ds][index]['high' ],
			'low'  :board[ds][index]['low'  ],
			'close':board[ds][index]['close'],
			})
	
	if len(indicator) == 0 :		
		return None
	return {
		'open' :indicator[0]['open'],
		'high' :max([x['high'] for x in indicator ]),
		'low'  :min([x['low']  for x in indicator ]),
		'close':indicator[-1]['close'],}

def StocksBoardData_New(boardcode_dict,ktype,Connect,WindPy,FileName=None,Default=True):
	Insert_Before_Num = 500
	boardlist = []
	
	cursor = Connect.cursor()
	sql = "select  name,sectorid,type  from stocks_board where windcode= %(code)s"
	
	Flag = False
	if FileName is not None:
		Flag = True
		f = open(FileName,'w')

	current_date = datetime.now()	
	last_date = StocksGetLastDate(ktype,current_date)
	wsdata=WindPy.tdaysoffset(-1, current_date, "")	
	# print wsdata	
	ds = 'ta'+str(ktype)
	# valid = CheckDateTimeValid(MarketType,current_date,WindPy)
	for windcode in  boardcode_dict :
		
		# print windcode
		cursor.execute(sql,{'code':windcode,})
		temp = cursor.fetchone()
		bname = temp[0] 
		sectorid = temp[1]
		btype  = temp[2]
		board = boardcode_dict[windcode][ds]
		# print windcode,boardcode_dict[index]
		# if  len(board) < 50  :
		# 	continue
		
		
		high = [ board[x]['high'  ] for x in range(0, len(board) ) ] 
		low  = [ board[x]['low'   ] for x in range(0, len(board) ) ] 
		close= [ board[x]['close' ] for x in range(0, len(board) ) ] 
		# volume =[ board[x]['volume' ] for x in range(0, len(board) ) ] 
		if Flag:
			f.write(" windcode %s high %f low %f close %f" %(windcode,high[-1],low[-1],close[-1]))
		if len(last_date)>0 and board[-1]['date'] < last_date[-1]:
			last_data=StocksGetLastData_New( boardcode_dict[windcode] ,ktype,last_date)
			if last_data is not None and len(last_data) >0 :			
				high.append(last_data['high'])		
				
				low.append(last_data['low'])		
				
				close.append(last_data['close'])
		if Default :
			high = numpy.array(high, dtype = numpy.float)
			low  = numpy.array(low , dtype = numpy.float)
			close= numpy.array(close,dtype = numpy.float)
			# volume= numpy.array(volume,dtype=numpy.float)
			macd=FourIndicator.MACD_Default(close)
			kdj_fast = FourIndicator.KDJ_Default(high,low,close)
			kdj_slow = FourIndicator.KDJ_Default(high,low,close,fastk_period=21,  slowk_period=9,  fastd_period =9)				

		else:
			high0 = [  high[0]  ] * Insert_Before_Num
			low0  = [   low[0]  ] * Insert_Before_Num
			close0 =[ close[0]  ] * Insert_Before_Num
			# volume0 =[ volume[0]  ] * Insert_Before_Num
			high0.extend(high)
			low0.extend(low)
			close0.extend(close)
			# volume0.extend(volume)
			high = numpy.array(high0, dtype = numpy.float)
			low  = numpy.array(low0 , dtype = numpy.float)
			close= numpy.array(close0,dtype = numpy.float)
			# volume= numpy.array(volume0,dtype=numpy.float)
			del high0 , low0 , close0# , volume0
			macd=FourIndicator.MACD(close)
			kdj_fast = FourIndicator.KDJ(high,low,close)
			kdj_slow = FourIndicator.KDJ(high,low,close,fastk_period=21,  slowk_period=9,  fastd_period =9)	
		diff = macd['DIFF']
		dea  = macd['DEA' ] 
		kdj_fast_k = kdj_fast['K'] 
		kdj_fast_d = kdj_fast['D']
		kdj_slow_k = kdj_slow['K'] 
		kdj_slow_d = kdj_slow['D'] 
		pb = FourIndicator.PB(close)
		pb1 = pb['PB1'] 
		pb2 = pb['PB2'] 
		pb6 = pb['PB6'] 
		# vma = FourIndicator.MA(volume,time_period=30)
		if Flag:
			f.write(" diff %f dea %f fast_k %f fast_d %f slow_k %f slow_d %f  pb1 %f pb2 %f  pb6 %f \n" %(diff[-1],dea[-1],kdj_fast_k[-1],kdj_fast_d[-1],kdj_slow_k[-1],kdj_slow_d[-1],pb1[-1],pb2[-1],pb6[-1]))
		# wsdata=WindPy.wsd(windcode, "pct_chg,mkt_cap_ard", current_date, current_date, "Period=W;PriceAdj=F")
		boardlist.append({
				'name':bname,
				'windcode': windcode,
				'sectorid': sectorid,
				'update_date' :current_date,
				'pct_chg' : board[-1]['pct_chg'],#wsdata.Data[0][0],
				'cap':      board[-1]['cap'    ],#wsdata.Data[1][0],
				'type' : btype,
				'diff' : diff[-100: ] ,
				'dea'  : dea [-100: ] ,
				# 'vma'  : vma [-100: ] ,
				'fast_k':kdj_fast_k[-100: ],
				'fast_d':kdj_fast_d[-100: ],
				'slow_k':kdj_slow_k[-100: ],
				'slow_d':kdj_slow_d[-100: ],
				'pb1':pb1[-100: ],
				'pb2':pb2[-100: ],
				'pb6':pb6[-100: ],
				'isStrong':False,
				'isReady' :False,
				})
		del high,low,close#,volume
		del diff,dea,macd
		del kdj_fast_k,kdj_fast_d,kdj_fast
		del kdj_slow_k,kdj_slow_d,kdj_slow
		del pb1,pb2,pb6,pb
	gc.collect()
	if Flag:
		f.close()	
	cursor.close()
	return boardlist

def StocksBoardCalc(BoardList,KDJFast=True):
	for index in range(0,len(BoardList)):
		board = BoardList[index]
		if KDJFast:
			W_K= board['fast_k'][-1]
			W_D= board['fast_d'][-1]
		else:
			W_K= board['slow_k'][-1]
			W_D= board['slow_d'][-1]
		# print board['windcode'],W_K,W_D
		if W_K<=W_D :
			continue
		
		W_DIF = board['diff'][-1]
		W_DEA = board['dea' ][-1]
		W_PB1 = board['pb1' ][-1]
		W_PB2 = board['pb2' ][-1]
		W_PB6 = board['pb6' ][-1]
		if W_DIF<W_DEA :
			if W_PB1 > W_PB6 and W_PB2 > W_PB6 :
				# print 'Ready ',board['windcode']
				BoardList[index]['isReady'] = True
				continue
		if W_DIF>W_DEA and W_DEA>0:
			if W_PB1 > W_PB2 and W_PB2 > W_PB6 :
				# print 'Strong ',board['windcode']
				BoardList[index]['isStrong'] = True
				continue

def StocksBoardStrong1(ktype,MarketType,Connect,WindPy,KDJFast=True):
	if (MarketType==0):
		tablename = "futures_history"
	else:
		tablename = "stocks_history"
	boardlist = []
	
	cursor = Connect.cursor()
	sql = "select  name, windcode,sectorid,type  from stocks_board"
	cursor.execute(sql)
	cursor_history = Connect.cursor()
	current_date = datetime.now()
	valid = CheckDateTimeValid(MarketType,current_date,WindPy)
	for bname,windcode,sectorid,btype  in cursor:
		if valid :
			last_data=StocksDataLastData(windcode,ktype,MarketType,current_date,WindPy,Connect)
			if last_data is None :
				sql_history  ="select close_price from "+ tablename +" where ktype=%(ktype)s and prod_code=%(code)s order by date desc limit 1"
				cursor_history.execute(sql_history,{
					'ktype':ktype-1,
					'code':windcode,
				})
				temp =	cursor_history.fetchone()[0]
				last_data = {
					'open' :temp,
					'high' :temp,
					'low'  :temp,
					'close':temp,
				}
			sql_history = "select open_price,high_price,low_price,close_price from "+ tablename +" where ktype=%(ktype)s and prod_code=%(code)s order by date asc  "	
		
			cursor_history.execute(sql_history,{
				'ktype':ktype,
				'code':windcode,
				})	
			# print windcode
			indicator = []
			close_list= []	
			for open_price,high_price,low_price,close_price in cursor_history:
				indicator.append({
					'open' :open_price,
					'high' :high_price,
					'low'  :low_price,
					'close':close_price,
					})
				close_list.append(close_price)
			if (last_data is not None)  :
				indicator.append(last_data)
			macd=FourIndicator.MACD(indicator)
			if KDJFast:
				kdj = FourIndicator.KDJ(indicator)
			else:
				kdj = FourIndicator.KDJ(indicator,fastk_period=21,  slowk_period=9,  fastd_period =9)
			diff = macd['DIFF'][-1]
			dea  = macd['DEA' ][-1]
			kdj_k = kdj['K'][-1]
			kdj_d = kdj['D'][-1]
			pb = FourIndicator.PB(close_list)
			pb1 = pb['PB1'][-1]
			pb2 = pb['PB2'][-1]
			pb6 = pb['PB6'][-1]
		else:
			sql_history = "select diff,dea,pb1,pb2,pb6"
			if KDJFast :
				sql_history +=",fast_k,fast_d "
			else:
				sql_history +=",slow_k,slow_d "
			sql_history += " from "+tablename+" where ktype=%(ktype)s and prod_code=%(code)s order by date desc limit 1"
			cursor_history.execute(sql_history,{
				'ktype':ktype,
				'code':windcode,
			})
			for diff,dea,pb1,pb2,pb6,kdj_k,kdj_d in cursor_history:
				break
		if diff is None:
			continue
		if diff>dea and dea>0  and kdj_k>kdj_d and (pb1>pb6 and pb2>pb6): #dif>dea
			wsdata=w.tdaysoffset(-1, current_date, "")		
			enddatestr= wsdata.Data[0][0].strftime("%Y-%m-%d")			
			wsdata=WindPy.wsd(windcode, "pct_chg,mkt_cap_ard", enddatestr, enddatestr, "Period=W;PriceAdj=F")
			boardlist.append({
					'name':bname,
					'windcode': windcode,
					'sectorid': sectorid,
					'update_date' :current_date,
					'pct_chg' : wsdata.Data[0][0],
					'cap': wsdata.Data[1][0],
					'type' : btype,
					'k':kdj_k,
					'd':kdj_d,
					'pb1':pb1,
					'pb2':pb2,
					'pb6':pb6,
					})

	cursor_history.close()
	cursor.close()
	#sort	
	
	return  StocksSortVolume(boardlist, WindPy)

def StocksBoardStrong2(ktype,MarketType,WindPy,connect,KDJFast=True):
	if (MarketType==0):
		tablename = "futures_history"
	else:
		tablename = "stocks_history"
	boardlist = []
	
	cursor = connect.cursor()
	sql = "select  name, windcode,sectorid,type  from stocks_board"
	cursor.execute(sql)
	cursor_history = connect.cursor()
	current_date = datetime.now()
	valid = CheckDateTimeValid(MarketType,current_date,WindPy)
	for bname,windcode,sectorid,btype  in cursor:
		if valid :
			last_data=StocksDataLastData(windcode,ktype,MarketType,current_date,WindPy,connect)
			if last_data is None :
				sql_history  ="select close_price from "+ tablename +" where ktype=%(ktype)s and prod_code=%(code)s order by date desc limit 1"
				cursor_history.execute(sql_history,{
					'ktype':ktype-1,
					'code':windcode,
				})
				temp =	cursor_history.fetchone()[0]
				last_data = {
					'open' :temp,
					'high' :temp,
					'low'  :temp,
					'close':temp,
				}
			sql_history = "select open_price,high_price,low_price,close_price from "+ tablename +" where ktype=%(ktype)s and prod_code=%(code)s order by date asc  "	
		
			cursor_history.execute(sql_history,{
				'ktype':ktype,
				'code':windcode,
				})	
				# print windcode
			indicator = []
			close_list= []	
			for open_price,high_price,low_price,close_price in cursor_history:
				indicator.append({
					'open' :open_price,
					'high' :high_price,
					'low'  :low_price,
					'close':close_price,
					})
				close_list.append(close_price)
			if  (last_data is not None)  :
				indicator.append(last_data)
			macd=FourIndicator.MACD(indicator)
			if KDJFast:
				kdj = FourIndicator.KDJ(indicator)
			else:
				kdj = FourIndicator.KDJ(indicator,fastk_period=21,  slowk_period=9,  fastd_period =9)
			diff = macd['DIFF'][-1]
			dea  = macd['DEA' ][-1]
			kdj_k = kdj['K'][-1]
			kdj_d = kdj['D'][-1]
			pb = FourIndicator.PB(close_list)
			pb1 = pb['PB1'][-1]
			pb2 = pb['PB2'][-1]
			pb6 = pb['PB6'][-1]
		else:
			sql_history = "select diff,dea,pb1,pb2,pb6"
			if KDJFast :
				sql_history +=",fast_k,fast_d "
			else:
				sql_history +=",slow_k,slow_d "
			sql_history += " from "+tablename+" where ktype=%(ktype)s and prod_code=%(code)s order by date desc limit 1"
			cursor_history.execute(sql_history,{
				'ktype':ktype,
				'code':windcode,
			})
			for diff,dea,pb1,pb2,pb6,kdj_k,kdj_d in cursor_history:
				break
		if diff is None:
			continue
		if dea>diff and diff>0 and kdj_k>kdj_d and (pb1>pb6 and pb2>pb6): #dif>dea
			wsdata=w.tdaysoffset(-1, current_date, "")		
			enddatestr= wsdata.Data[0][0].strftime("%Y-%m-%d")			
			wsdata=WindPy.wsd(windcode, "pct_chg,mkt_cap_ard", enddatestr, enddatestr, "Period=W;PriceAdj=F")
			boardlist.append({
					'name':bname,
					'windcode': windcode,
					'sectorid': sectorid,
					'update_date' :current_date,
					'pct_chg' : wsdata.Data[0][0],
					'cap': wsdata.Data[1][0],
					'type' : btype,
					'k':kdj_k,
					'd':kdj_d,
					'pb1':pb1,
					'pb2':pb2,
					'pb6':pb6,
					})

	cursor_history.close()
	cursor.close()
	#sort	
	
	return  StocksSortVolume(boardlist, WindPy)
#finished,not debug ,change soon.	
def StocksBoardReady(ktype,MarketType,WindPy,connect,KDJFast=True):
	if (MarketType==0):
		tablename = "futures_history"
	else:
		tablename = "stocks_history"
	boardlist = []
	cursor = connect.cursor()
	sql = "select  name, windcode,sectorid,type,update_date from stocks_board"
	cursor.execute(sql)
	cursor_history = connect.cursor()
	current_date = datetime.now()
	valid = CheckDateTimeValid(MarketType,current_date,WindPy)
	for bname,windcode,sectorid,btype  in cursor:
		if valid :
			last_data=StocksDataLastData(windcode,ktype,MarketType,current_date,WindPy,connect)
			if last_data is None :
				sql_history  ="select close_price from "+ tablename +" where ktype=%(ktype)s and prod_code=%(code)s order by date desc limit 1"
				cursor_history.execute(sql_history,{
					'ktype':ktype-1,
					'code':windcode,
				})
				temp =	cursor_history.fetchone()[0]
				last_date = {
					'open' :temp,
					'high' :temp,
					'low'  :temp,
					'close':temp,
				}
		sql_history = "select open_price,high_price,low_price,close_price from "+ tablename +" where ktype=%(ktype)s and prod_code=%(code)s order by date asc  "	
	
		cursor_history.execute(sql_history,{
			'ktype':ktype,
			'code':windcode,
			})	
		# print windcode
		indicator = []
		close_list= []	
		for open_price,high_price,low_price,close_price in cursor_history:
			indicator.append({
				'open' :open_price,
				'high' :high_price,
				'low'  :low_price,
				'close':close_price,
				})
			close_list.append(close_price)
		if valid and (last_data is not None)  :
			indicator.append(last_data)
			macd=FourIndicator.MACD(indicator)
			if KDJFast:
				kdj = FourIndicator.KDJ(indicator)
			else:
				kdj = FourIndicator.KDJ(indicator,fastk_period=21,  slowk_period=9,  fastd_period =9)
			diff = macd['DIFF'][-1]
			dea  = macd['DEA' ][-1]
			kdj_k = kdj['K'][-1]
			kdj_d = kdj['D'][-1]
			pb = FourIndicator.PB(close_list)
			pb1 = pb['PB1'][-1]
			pb2 = pb['PB2'][-1]
			pb6 = pb['PB6'][-1]
		else:
			sql_history = "select diff,dea,pb1,pb2,pb6"
			if KDJFast :
				sql_history +=",fast_k,fast_d "
			else:
				sql_history +=",slow_k,slow_d "
			sql_history += " from "+tablename+" where ktype=%(ktype)s and prod_code=%(code)s order by date desc limit 1"
			cursor_history.execute(sql_history,{
				'ktype':ktype,
				'code':windcode,
			})
			for diff,dea,pb1,pb2,pb6,kdj_k,kdj_d in cursor_history:
				continue
		if diff is None:
			continue
		if diff>0 and kdj_k<kdj_d and ( pb1>pb2 and pb2 > pb6 ) :
				boardlist.append({
					'name':bname,
					'windcode': windcode,
					'sectorid': sectorid,
					'update_date' :update_date,
					'type' : btype,
					'k':kdj_k,
					'd':kdj_d,
					'pb1':pb1,
					'pb2':pb2,
					'pb6':pb6,
					})
				break
	cursor_history.close()
	cursor.close()
	#sort
	return  sorted(boardlist, key=lambda board: board['k'])

def StocksData(Windcode_Set,ktype,MarketType,Connect,WindPy,Sectorid=None):
	if (MarketType==0):
		tablename = "futures_history"
	else:
		tablename = "stocks_history"
	stocklist = []
	cursor = Connect.cursor()
	cursor_history = Connect.cursor()
	current_date = datetime.now()
	# valid = CheckDateTimeValid(MarketType,current_date,WindPy)
	for windcode  in Windcode_Set:
		# if valid :
		last_data=StocksGetLastData(windcode,ktype,MarketType,current_date,Connect,WindPy)
		if last_data is not None :			
			sql_history = "select open_price,high_price,low_price,close_price from "+ tablename +" where ktype=%(ktype)s and prod_code=%(code)s order by date asc  "	
			cursor_history.execute(sql_history,{
				'ktype':ktype,
				'code':windcode,
				})	
			# print windcode
			indicator = []
			for open_price,high_price,low_price,close_price in cursor_history:
				indicator.append({
					'open' :open_price,
					'high' :high_price,
					'low'  :low_price,
					'close':close_price,
					})				
			indicator.append(last_data)
			if len(indicator)<52:
				continue
			high = numpy.array([ indicator[x]['high' ] for x in range(0, len(indicator) ) ], dtype = numpy.float)
			low  = numpy.array([ indicator[x]['low'  ] for x in range(0, len(indicator) ) ], dtype = numpy.float)
			close =numpy.array([ indicator[x]['close'] for x in range(0, len(indicator) ) ], dtype = numpy.float)
			macd=FourIndicator.MACD(close)
			kdj_fast = FourIndicator.KDJ(high,low,close)
			kdj_slow = FourIndicator.KDJ(high,low,close,fastk_period=21,  slowk_period=9,  fastd_period =9)
			diff = macd['DIFF']
			dea  = macd['DEA' ] 
			kdj_fast_k = kdj_fast['K'] 
			kdj_fast_d = kdj_fast['D']
			kdj_slow_k = kdj_slow['K'] 
			kdj_slow_d = kdj_slow['D'] 
			pb = FourIndicator.PB(close)
			pb1 = pb['PB1'] 
			pb2 = pb['PB2'] 
			pb6 = pb['PB6'] 
		else:
			sql_history = "select diff,dea,pb1,pb2,pb6,fast_k,fast_d,slow_k,slow_d"
			
			sql_history += " from "+tablename+" where ktype=%(ktype)s and prod_code=%(code)s order by date desc limit 300"
			cursor_history.execute(sql_history,{
				'ktype':ktype,
				'code':windcode,
			})
			diff = dea = kdj_fast_k = kdj_fast_d = kdj_slow_k = kdj_slow_d = pb1 = pb2 = pb6 = []
			for h_diff,h_dea,h_pb1,h_pb2,h_pb6,h_kdj_fast_k,h_kdj_fast_d,h_kdj_slow_k,h_kdj_slow_d in cursor_history :
				diff.insert(0,h_diff)
				dea.insert (0,h_dea )
				pb1.insert (0,h_pb1 )
				pb2.insert (0,h_pb2 )
				pb6.insert (0,h_pb6 )
				kdj_fast_k.insert(0,h_kdj_fast_k)
				kdj_fast_d.insert(0,h_kdj_fast_d)
				kdj_slow_k.insert(0,h_kdj_slow_k)
				kdj_slow_d.insert(0,h_kdj_slow_d)
		
		sql = "select name from stocks_code where windcode='"+windcode+"'"
		cursor.execute(sql)
		# if diff>0 and kdj_k>kdj_d and ( diff>dea ) :
		stock = {
			'name': cursor.fetchone()[0],
			'windcode': windcode,
			'type':'stocktype',
			'diff' : diff ,
			'dea'  : dea  ,
			'fast_k':kdj_fast_k,
			'fast_d':kdj_fast_d,
			'slow_k':kdj_slow_k,
			'slow_d':kdj_slow_d,
			'pb1':pb1,
			'pb2':pb2,
			'pb6':pb6,
			'isStrong':False,
			'isReady' :False,
		}
		if Sectorid is not None:
			stock['sectorid'] = Sectorid
		stocklist.append(stock)
	cursor.close()		
	cursor_history.close()
	
	#sort
	return  stocklist

def StocksData_New(Windcode_Dict,ktype,Connect,WindPy,Sectorid=None,Default=True):
	Insert_Before_Num = 500
	stocklist = []
	cursor = Connect.cursor()
	f = open('StockData.txt','w')
	current_date = datetime.now()
	last_date = StocksGetLastDate(ktype,current_date)

	ds ='ta'+str(ktype)
	for windcode  in  Windcode_Dict :
		board = Windcode_Dict[windcode]		
		# if len(board['ta8']) < 50  :
		# 	continue
		high  = [ board[ds][x]['high'  ] for x in range(0, len(board[ds]) ) ] 
		low   = [ board[ds][x]['low'   ] for x in range(0, len(board[ds]) ) ] 
		close = [ board[ds][x]['close' ] for x in range(0, len(board[ds]) ) ] 
		volume= [ board[ds][x]['volume'] for x in range(0, len(board[ds]) ) ] 	
		if len(close) ==0 :
			continue	
		if len(last_date)>0 and board[ds][-1]['date'] < last_date[-1]:
			last_data=StocksGetLastData_New( board ,ktype,last_date)

			if last_data is not None and len(last_data) > 0:			
				high.append(last_data['high'])		
				
				low.append(last_data['low'])		
				
				close.append(last_data['close'])
		if Default :
			high = numpy.array(high, dtype = numpy.float)
			low  = numpy.array(low , dtype = numpy.float)
			close= numpy.array(close,dtype = numpy.float)
			volume= numpy.array(volume,dtype=numpy.float)
			macd=FourIndicator.MACD_Default(close)
			kdj_fast = FourIndicator.KDJ_Default(high,low,close)
			kdj_slow = FourIndicator.KDJ_Default(high,low,close,fastk_period=21,  slowk_period=9,  fastd_period =9)				
		else:
			high0 = [  high[0]  ] * Insert_Before_Num
			low0  = [   low[0]  ] * Insert_Before_Num
			close0 =[ close[0]  ] * Insert_Before_Num
			volume0 =[ volume[0]  ] * Insert_Before_Num
			high0.extend(high)
			low0.extend(low)
			close0.extend(close)
			volume0.extend(volume)
			high = numpy.array(high0, dtype = numpy.float)
			low  = numpy.array(low0 , dtype = numpy.float)
			close= numpy.array(close0,dtype = numpy.float)
			volume= numpy.array(volume0,dtype=numpy.float)
			del high0 , low0 , close0 , volume0
			macd=FourIndicator.MACD(close)
			kdj_fast = FourIndicator.KDJ(high,low,close)
			kdj_slow = FourIndicator.KDJ(high,low,close,fastk_period=21,  slowk_period=9,  fastd_period =9)			
		diff = macd['DIFF']
		dea  = macd['DEA' ] 
		bar  = macd['MACD']
		kdj_fast_k = kdj_fast['K'] 
		kdj_fast_d = kdj_fast['D']
		kdj_fast_j = kdj_fast['J']
		# print windcode,ktype,kdj_slow
		kdj_slow_k = kdj_slow['K'] 
		kdj_slow_d = kdj_slow['D']
		kdj_slow_j = kdj_slow['J']
		pb = FourIndicator.PB(close)
		pb1 = pb['PB1'] 
		pb2 = pb['PB2'] 
		pb6 = pb['PB6'] 
		vma = FourIndicator.MA(volume,time_period=30)
		f.write("windcode:%s high:%f low:%f close:%f diff:%f dea:%f macd:%f K:%f D:%f J:%f PB1:%f PB2:%f PB6:%f \n"%
			(windcode,high[-1],low[-1],close[-1],diff[-1],dea[-1],bar[-1],kdj_fast_k[-1],kdj_fast_d[-1],kdj_fast_j[-1],pb1[-1],pb2[-1],pb6[-1]))
		# sql = "select name from stocks_code where windcode='"+windcode+"'"
		# cursor.execute(sql)
		
		stock = {
			'name': board['name'],#cursor.fetchone()[0],
			'stars': board['stars'],
			'windcode': windcode,
			'type':'stocktype',
			'diff' : diff[-100:] ,
			'dea'  : dea [-100:] ,
			'macd' : bar [-100:] ,
			'fast_k':kdj_fast_k[-100:],
			'fast_d':kdj_fast_d[-100:],
			'fast_j':kdj_fast_j[-100:],
			'slow_k':kdj_slow_k[-100:],
			'slow_d':kdj_slow_d[-100:],
			'slow_j':kdj_slow_j[-100:],
			'pb1':pb1[-100:],
			'pb2':pb2[-100:],
			'pb6':pb6[-100:],
			'vma'  : vma [-100: ] ,
			'isStrong':False,
			'isReady' :False,
			'pct_chg' : board[ds][-1]['pct_chg'],
			'cap':      board[ds][-1]['cap'    ],
		}
		del high,low,close,volume
		del diff,dea,bar,macd,vma
		del kdj_fast_k,kdj_fast_d,kdj_fast_j,kdj_fast
		del kdj_slow_k,kdj_slow_d,kdj_slow_j,kdj_slow
		del pb1,pb2,pb6,pb
		
		# wsdata=WindPy.wsd(windcode, "pct_chg,mkt_cap_ard", current_date, current_date, "Period=W;PriceAdj=F")
		# if  CheckWindData(wsdata):
		# 	stock['pct_chg'] = wsdata.Data[0][0] 
		# 	stock['cap' ] = wsdata.Data[1][0] 
		if Sectorid is not None:
			stock['sectorid'] = Sectorid
		stocklist.append(stock)
	f.close()
	gc.collect()
	cursor.close()		
	
	
	#sort
	return  stocklist

def StocksBoardStockData(sectorid,Connect):	
	stocklist = set()
	cursor = Connect.cursor()
	sql = "select  name, windcode,sectorid  from stocks_boardlist where sectorid=%(sectorid)s"
	cursor.execute(sql,{'sectorid': sectorid })
	
	for bname,windcode,sectorid  in cursor:
		stocklist.add(windcode)

	return stocklist

def StocksBoardStockCalc(StockList,KDJFast=True):
	stock_select=[]
	for board  in StockList :
				
		if KDJFast:
			W_K= board['fast_k'][-1]
			W_D= board['fast_d'][-1]
		else:
			W_K= board['slow_k'][-1]
			W_D= board['slow_d'][-1]
		if W_K<=W_D :
			continue
		W_DIF = board['diff'][-1]
		W_DEA = board['dea' ][-1]
		W_PB1 = board['pb1' ][-1]
		W_PB2 = board['pb2' ][-1]
		W_PB6 = board['pb6' ][-1]
		if W_DIF>0 and W_DIF>W_DEA :
			if W_PB1 > W_PB2 and W_PB2 > W_PB6 :
				stock_select.append(board)
				continue
	return stock_select

def StocksSortPctchg(StocksList,daysoffset,MaxValue,MinValue,WindPy):
	#exclude suspend stocks
	# current_date = datetime.now()
	# if valid :
	# 	enddatestr = current_date.strftime("%Y-%m-%d")
	# else :
	# 	wsdata=w.tdaysoffset(-1, current_date, "")		
	# 	enddatestr= wsdata.Data[0][0].strftime("%Y-%m-%d")
	# wsdata = WindPy.wset("tradesuspend","startdate="+enddatestr+";enddate="+enddatestr+";field=wind_code")
	# if  CheckWindData(wsdata) :	
	# 	StocksList = [ stock for stock in StocksList if stock['windcode'] not in wsdata.Data[0]  ]	
		
	#10 days pct_chg
	# wsdata = WindPy.tdaysoffset(-10, current_date, "")
	# if not CheckWindData(wsdata) :
	# 	return StocksList
	# startdate = wsdata.Data[0][0]
	enddatestr = datetime.now().strftime("%Y-%m-%d")
	for index in range(len(StocksList)) :
		stock = StocksList[index]
		wsdata = WindPy.wss(StocksList[index]['windcode'], "pct_chg_nd", "days="+str(daysoffset)+";tradeDate="+enddatestr)
		if not CheckWindData(wsdata) :
			StocksList[index]['pct_chg_nd'] = -100			
			continue
		else:	
			if 	wsdata.Data[0][0] > MaxValue or wsdata.Data[0][0] < MinValue:	
				StocksList[index]['pct_chg_nd'] = -100				
				continue
			else:
				StocksList[index]['pct_chg_nd'] = wsdata.Data[0][0]
		if stock['pb2'][-1] > stock['pb6'][-1] and stock['pb1'][-1] > stock['pb2'][-1] :
			StocksList[index]['pct_chg_nd'] += 30
	# StocksList=  [ stock for stock in StocksList if stock['pct_chg_nd'] != -100 ]

	return sorted(StocksList, key=lambda stock:stock['pct_chg_nd'],reverse=True)

def StocksStrong(board,ktype,MarketType,WindPy,connect,KDJFast=True):
	if (MarketType==0):
		tablename = "futures_history"
	else:
		tablename = "stocks_history"
	stocklist = []
	cursor = connect.cursor()
	sql = "select  name, windcode,sectorid  from stocks_boardlist where sectorid=%(sectorid)s"
	cursor.execute(sql,{'sectorid':board['sectorid']})
	cursor_history = connect.cursor()
	current_date = datetime.now()
	valid = CheckDateTimeValid(MarketType,current_date,WindPy)
	for bname,windcode,sectorid  in cursor:
		if valid :
			last_data=StocksDataLastData(windcode,ktype,MarketType,current_date,WindPy,connect)
			if last_data is None :
				sql_history  ="select close_price from "+ tablename +" where ktype=%(ktype)s and prod_code=%(code)s order by date desc limit 1"
				cursor_history.execute(sql_history,{
					'ktype':ktype-1,
					'code':windcode,
				})
				temp =	cursor_history.fetchone()[0]
				last_data = {
					'open' :temp,
					'high' :temp,
					'low'  :temp,
					'close':temp,
				}
			sql_history = "select open_price,high_price,low_price,close_price from "+ tablename +" where ktype=%(ktype)s and prod_code=%(code)s order by date asc  "	
		
			cursor_history.execute(sql_history,{
				'ktype':ktype,
				'code':windcode,
				})	
			
			indicator = []
			close_list= []	
			for open_price,high_price,low_price,close_price in cursor_history:
				indicator.append({
					'open' :open_price,
					'high' :high_price,
					'low'  :low_price,
					'close':close_price,
					})
				close_list.append(close_price)
			if last_data is not None  :
				indicator.append(last_data)
			macd=FourIndicator.MACD(indicator)
			if KDJFast:
				kdj = FourIndicator.KDJ(indicator)
			else:
				kdj = FourIndicator.KDJ(indicator,fastk_period=21,  slowk_period=9,  fastd_period =9)
			diff = macd['DIFF'][-1]
			dea  = macd['DEA' ][-1]
			kdj_k = kdj['K'][-1]
			kdj_d = KDJ['D'][-1]
			pb = FourIndicator.PB(close_list)
			pb1 = pb['PB1'][-1]
			pb2 = pb['PB2'][-1]
			pb6 = pb['PB6'][-1]
		else:
			sql_history = "select diff,dea,pb1,pb2,pb6"
			if KDJFast :
				sql_history +=",fast_k,fast_d "
			else:
				sql_history +=",slow_k,slow_d "
			sql_history += " from "+tablename+" where ktype=%(ktype)s and prod_code=%(code)s order by date desc limit 1"
			cursor_history.execute(sql_history,{
				'ktype':ktype,
				'code':windcode,
			})
			for diff,dea,pb1,pb2,pb6,kdj_k,kdj_d in cursor_history:
				break

		if diff>0 and kdj_k>kdj_d and ( diff>dea ) :
				stocklist.append({
					'name':bname,
					'windcode': windcode,
					'sectorid': sectorid,
					'type':'type',
					'pb1':pb1,
					'pb2':pb2,
					'pb6':pb6
					})
				
	cursor_history.close()
	cursor.close()
	#sort
	return  StocksSortPctchg(stocklist,10,0.3,0,WindPy)

def StocksStrong1(ktype,MarketType,WindPy,connect,KDJFast=True):
	if (MarketType==0):
		tablename = "futures_history"
	else:
		tablename = "stocks_history"
	stocklist = []
	cursor = connect.cursor()
	sql = "select  name, windcode,sectorid  from stocks_boardlist "
	cursor.execute(sql,{'sectorid':board['sectorid']})
	cursor_history = connect.cursor()
	current_date = datetime.now()
	valid = CheckDateTimeValid(MarketType,current_date,WindPy)
	for bname,windcode,sectorid  in cursor:
		if valid :
			last_data=StocksDataLastData(windcode,ktype,MarketType,current_date,WindPy,connect)
			if last_data is None :
				sql_history  ="select close_price from "+ tablename +" where ktype=%(ktype)s and prod_code=%(code)s order by date desc limit 1"
				cursor_history.execute(sql_history,{
					'ktype':ktype-1,
					'code':windcode,
				})
				temp =	cursor_history.fetchone()[0]
				last_data = {
					'open' :temp,
					'high' :temp,
					'low'  :temp,
					'close':temp,
				}
			sql_history = "select open_price,high_price,low_price,close_price from "+ tablename +" where ktype=%(ktype)s and prod_code=%(code)s order by date asc  "	
		
			cursor_history.execute(sql_history,{
				'ktype':ktype,
				'code':windcode,
				})	
			# print windcode
			indicator = []
			close_list= []	
			for open_price,high_price,low_price,close_price in cursor_history:
				indicator.append({
					'open' :open_price,
					'high' :high_price,
					'low'  :low_price,
					'close':close_price,
					})
				close_list.append(close_price)
			if last_data is not None  :
				indicator.append(last_data)
			macd=FourIndicator.MACD(indicator)
			if KDJFast:
				kdj = FourIndicator.KDJ(indicator)
			else:
				kdj = FourIndicator.KDJ(indicator,fastk_period=21,  slowk_period=9,  fastd_period =9)
			diff = macd['DIFF'][-1]
			dea  = macd['DEA' ][-1]
			kdj_k = kdj['K'][-1]
			kdj_d = KDJ['D'][-1]
			pb = FourIndicator.PB(close_list)
			pb1 = pb['PB1'][-1]
			pb2 = pb['PB2'][-1]
			pb6 = pb['PB6'][-1]
		else:
			sql_history = "select diff,dea,pb1,pb2,pb6"
			if KDJFast :
				sql_history +=",fast_k,fast_d "
			else:
				sql_history +=",slow_k,slow_d "
			sql_history += " from "+tablename+" where ktype=%(ktype)s and prod_code=%(code)s order by date desc limit 1"
			cursor_history.execute(sql_history,{
				'ktype':ktype,
				'code':windcode,
			})
			for diff,dea,pb1,pb2,pb6,kdj_k,kdj_d in cursor_history:
				break
		if diff is None:
			continue
		if diff>0 and kdj_k>kdj_d and ( diff>dea ) and pb1>pb6 and pb2>pb6 :
				stocklist.append({
					'name':bname,
					'windcode': windcode,
					'sectorid': sectorid,
					'type':'type',
					})
				
	cursor_history.close()
	cursor.close()
	#sort
	return  StocksSortPctchg(stocklist,10,0.3,0,WindPy)

def StocksStrong2(ktype,MarketType,WindPy,connect,KDJFast=True):
	if (MarketType==0):
		tablename = "futures_history"
	else:
		tablename = "stocks_history"
	stocklist = []
	cursor = connect.cursor()
	sql = "select  name, windcode,sectorid  from stocks_boardlist "
	cursor.execute(sql,{'sectorid':board['sectorid']})
	cursor_history = connect.cursor()
	current_date = datetime.now()
	valid = CheckDateTimeValid(MarketType,current_date,WindPy)
	for bname,windcode,sectorid  in cursor:
		if valid :
			last_data=StocksDataLastData(windcode,ktype,MarketType,current_date,WindPy,connect)
			if last_data is None :
				sql_history  ="select close_price from "+ tablename +" where ktype=%(ktype)s and prod_code=%(code)s order by date desc limit 1"
				cursor_history.execute(sql_history,{
					'ktype':ktype-1,
					'code':windcode,
				})
				temp =	cursor_history.fetchone()[0]
				last_data = {
					'open' :temp,
					'high' :temp,
					'low'  :temp,
					'close':temp,
				}
			sql_history = "select open_price,high_price,low_price,close_price from "+ tablename +" where ktype=%(ktype)s and prod_code=%(code)s order by date asc  "	
		
			cursor_history.execute(sql_history,{
				'ktype':ktype,
				'code':windcode,
				})	
			# print windcode
			indicator = []
			close_list= []	
			for open_price,high_price,low_price,close_price in cursor_history:
				indicator.append({
					'open' :open_price,
					'high' :high_price,
					'low'  :low_price,
					'close':close_price,
					})
				close_list.append(close_price)
			if last_data is not None  :
				indicator.append(last_data)
			macd=FourIndicator.MACD(indicator)
			if KDJFast:
				kdj = FourIndicator.KDJ(indicator)
			else:
				kdj = FourIndicator.KDJ(indicator,fastk_period=21,  slowk_period=9,  fastd_period =9)
			diff = macd['DIFF'][-1]
			dea  = macd['DEA' ][-1]
			kdj_k = kdj['K'][-1]
			kdj_d = KDJ['D'][-1]
			pb = FourIndicator.PB(close_list)
			pb1 = pb['PB1'][-1]
			pb2 = pb['PB2'][-1]
			pb6 = pb['PB6'][-1]
		else:
			sql_history = "select diff,dea,pb1,pb2,pb6"
			if KDJFast :
				sql_history +=",fast_k,fast_d "
			else:
				sql_history +=",slow_k,slow_d "
			sql_history += " from "+tablename+" where ktype=%(ktype)s and prod_code=%(code)s order by date desc limit 1"
			cursor_history.execute(sql_history,{
				'ktype':ktype,
				'code':windcode,
			})
			for diff,dea,pb1,pb2,pb6,kdj_k,kdj_d in cursor_history:
				break
		if diff is None:
			continue
		if dea>0 and kdj_k>kdj_d and ( dea>diff ) and pb1>pb6 and pb2>pb6 :
				stocklist.append({
					'name':bname,
					'windcode': windcode,
					'sectorid': sectorid,
					'type':'type',
					})
				
	cursor_history.close()
	cursor.close()
	#sort
	return  StocksSortPctchg(stocklist,10,0.3,0,WindPy)

def GetSuspendStockSet(EndDate,daysoffset,WindPy):
	Suspend_Stocks= set()
	wsdata = WindPy.tdaysoffset(daysoffset, EndDate, "")
	if not CheckWindData(wsdata) :
		return Suspend_Stocks
	starttime = wsdata.Data[0][0]
	wsdata=WindPy.wset("tradesuspend","startdate="+starttime.strftime("%Y-%m-%d")+";enddate="+EndDate.strftime("%Y-%m-%d"))
	if  CheckWindData(wsdata):		
		for index in range(0,len(wsdata.Data[0])):
			Suspend_Stocks.add( wsdata.Data[1][index] )
	return Suspend_Stocks

def GetLimitToFreeStockSet(Curreent_Date,monthsoffset,WindPy):
	Suspend_Stocks= set()
	wsdata = WindPy.tdaysoffset(monthsoffset*-1, Curreent_Date, "Period=M")
	if not CheckWindData(wsdata) :
		return Suspend_Stocks
	starttime = wsdata.Data[0][0]
	wsdata = WindPy.tdaysoffset(monthsoffset, Curreent_Date, "Period=M")
	if not CheckWindData(wsdata) :
		return Suspend_Stocks
	endtime = wsdata.Data[0][0]
	wsdata=WindPy.wset("limitingtofreeofcompanydetail","startdate="+starttime.strftime("%Y-%m-%d")+";enddate="+endtime.strftime("%Y-%m-%d")+";sectorid=a001010100000000;search=;field=wind_code,percent_beforechange")
	
	if  CheckWindData(wsdata):		
		for index in range(0,len(wsdata.Data[0])):
			if math.isnan(wsdata.Data[1][index]) or wsdata.Data[1][index]<20.0:
				continue
			Suspend_Stocks.add( wsdata.Data[0][index] )
	return Suspend_Stocks


def StocksCalc(StockList,KDJFast=True):
	for index in range(0,len(StockList)):

		board = StockList[index]
		W_DIF = board['diff'][-1]
		W_DEA = board['dea' ][-1]
		W_PB1 = board['pb1' ][-1]
		W_PB2 = board['pb2' ][-1]
		W_PB6 = board['pb6' ][-1]
		if KDJFast:
			W_K= board['fast_k'][-1]
			W_D= board['fast_d'][-1]
		else:
			W_K= board['slow_k'][-1]
			W_D= board['slow_d'][-1]
		if W_K>W_D  and W_DIF>W_DEA and W_DEA>0 and W_PB1 > W_PB2 and W_PB2 > W_PB6:
			StockList[index]['isStrong'] = True
			continue
		# if board['stars'] > 4  and W_K<30:
		# 	print board['windcode'],board['stars'],W_K,W_D,W_DIF
		if board['stars'] >4 and W_K<W_D and W_K<30 and W_DIF>0 and  W_PB1 > W_PB6 and W_PB2 > W_PB6 :
			StockList[index]['isReady'] = True
			

def StocksReadyIndexCalc(StockList,MaxValue,MinValue,Require_Num=30):
	num_list = [ 0 ] * Require_Num
	for index in range(0,len(StockList)):
		board = StockList[index]
		# print 'StocksReadyIndexCalc',board['windcode'],len(board['diff'])
		if len(board['diff']) < Require_Num:
			Require_Num = len(board['diff'])
		for ri in  range( Require_Num ):			
			W_DIF = board['diff'][-1-ri]
			W_DEA = board['dea' ][-1-ri]
			
			if W_DIF<= W_DEA :
				continue
			WB_DIF = board['diff'][-2-ri]
			WB_DEA = board['dea' ][-2-ri]
			if WB_DIF>= WB_DEA:
				continue			
			if W_DIF> MinValue or  W_DEA< MaxValue :
				num_list[ri] = num_list[ri] + 1
	return num_list

def StocksStrongIndexCalc(StockList,Require_Num=30,KDJFast=True):
	num_list = [ 0 ] * Require_Num
	for index in range(0,len(StockList)):
		board = StockList[index]
		# print 'StocksStrongIndexCalc',board['windcode'],len(board['diff'])
		if len(board['diff']) < Require_Num:
			Require_Num = len(board['diff'])

		for ri in  range( Require_Num ):			
			W_DIF = board['diff'][-1-ri]
			W_DEA = board['dea' ][-1-ri]
			if KDJFast:
				W_K= board['fast_k'][-1-ri]
				W_D= board['fast_d'][-1-ri]
			else:
				W_K= board['slow_k'][-1-ri]
				W_D= board['slow_d'][-1-ri]
							
			if W_K>W_D and W_DIF> 0 and  W_DEA>0 :
				num_list[ri] = num_list[ri] + 1
	return num_list
		
def StocksFundIndexCalc(Windcode_Dict,ktype,StockList,Default=True,Require_Num=30):
	Insert_Before_Num = 500
	rt=[]
	for ri in range(Require_Num):
		rt.append({'num':0,'windcode':[]}) 
	
	ds='ta'+str(ktype)
	for index in range(0,len(StockList)):
		stock = StockList[index]
		windcode = stock['windcode']
		board =Windcode_Dict[ windcode ]
		close =[ board[ds][x]['close' ] for x in range(0, len(board[ds]) ) ]
		c0 = [ close[0] ] * Insert_Before_Num
		c0.extend(close)
		close = numpy.array(c0, dtype = numpy.float)		
		ma33 = FourIndicator.MA( close,time_period=33 )
		sopen =[ board[ds][x]['open' ] for x in range(0, len(board[ds]) ) ]
		s0 = [ sopen[0] ] * Insert_Before_Num
		s0.extend(sopen)
		volume=[ board[ds][x]['volume' ] for x in range(0, len(board[ds]) ) ] 
		v0 = [ volume[0] ] * Insert_Before_Num
		v0.extend(volume)	
		for ri in  range( Require_Num ):
			# if ri==0 and windcode=='600804.SH':
			# 	print windcode, c0[-1], c0[-3],c0[-4], c0[-6],ma33[-1]
			if c0[-4-ri] == 0 or c0[-6-ri] == 0 :
				continue
			if c0[-1-ri] <= s0[-1-ri] :
				continue
			
			if c0[-1-ri] <= c0[-3-ri] or c0[-1-ri] <=ma33[-1-ri]:
				continue
			
			if c0[-1-ri]/c0[-4-ri] <=1.02  :
				continue

				
			vma = stock['vma'][-1-ri]
			
			if v0[-3-ri]<= vma or v0[-2-ri]<=vma :
				continue
			if v0[-1-ri] /vma <= 1.8 :
				continue
			aa = max(v0[-33-ri:-3-ri])
			# if ri==0 and windcode=='600804.SH':
			# 	print windcode,v0[-1],v0[-2],aa
			if v0[-1-ri]<=aa and  v0[-2-ri]<=aa :
				continue
			if  c0[-1-ri]/c0[-6-ri] <1.2 and c0[-1-ri]/c0[-4-ri] <1.15 :
				rt[ri]['windcode'].append(windcode)
			rt[ri]['num'] = rt[ri]['num'] + 1
		del c0,close,v0,volume
	# for ri in range( Require_Num ):
	# 	print rt[ri]
	gc.collect()
	return rt

def StocksBullLong(StockList,KDJFast=True):
	bulllong_set = set()
	for index in range(0,len(StockList)):
		board = StockList[index]
		if KDJFast:
			W_K= board['fast_k'][-1]
			W_D= board['fast_d'][-1]
		else:
			W_K= board['slow_k'][-1]
			W_D= board['slow_d'][-1]
		if W_K<=W_D :
			continue
		W_DIF = board['diff'][-1]
		W_DEA = board['dea' ][-1]
		
		if W_DIF>0 and W_DEA>0:
			bulllong_set.add(board)
			continue
	return bulllong_set

def StocksBasicCalc(Windcode_Dict,Connect,WindPy):
	current_date = datetime.now()	
	datestr = current_date.strftime("%Y-%m-%d")
	cursor = Connect.cursor()	
	wl = {}
	for windcode in Windcode_Dict:
		 
		stars = 0 
		b = []
		sql = "select content,quarter,btype from stocks_basic where windcode=%(code)s and btype=%(type)s order by quarter desc limit 4"
		for index in range(6) :
			temp = []
			data = {
				'code':windcode,
				'type':index,
			}
			cursor.execute(sql,data)
			for content,quarter,btype in cursor:
				temp.insert(0,content)
			b.append(temp)
		# print windcode,b
		b1 = b[0][1:]
		if len(filter(lambda x: x>=10 ,b1 ))==3 and len(filter(lambda x: x>=20 ,b1 ))>1:
			stars += 1
		
		if len(b[1])>0 and b[1][-1]>=20:
			stars +=  1
		# print b[2][-1]
		if len(b[2])>0 and b[2][-1]>=0:
			stars +=  1
		if len(b[4])>0 and b[4][-1]>=0:
			stars +=  1		
		if len(filter(lambda x: x<=60 ,b[3] ))==4:
			stars +=  1
		
		if len(b[5])>0 and b[5][-1]>=4:
			stars += 1
		wl[windcode] = stars
		sql = "Replace into stocks_basic(windcode,quarter,btype,content) values (%(windcode)s,%(quarter)s,%(type)s,%(value)s)"
		data = {
			'windcode':windcode,
			'quarter': datestr,
			'type': 9,
			'value': stars
		}
		cursor.execute(sql,data)
	Connect.commit()
	return wl

def GetStocksBasic(Windcode_Dict,Connect):
	current_date = datetime.now()	
	datestr = current_date.strftime("%Y-%m-%d")
	cursor = Connect.cursor()	
	wl = {}
	for windcode in Windcode_Dict:
		 
		stars = 0 
		sql = "select content,quarter,btype from stocks_basic where windcode=%(code)s and btype=%(type)s order by quarter desc limit 1"
		data = {
			'code':windcode,
			'type':9,
		}
		cursor.execute(sql,data)
		if cursor.rowcount >0 :
			stars = cursor.fetchone()[0]	
		wl[windcode] = stars
		
	
	return wl	

def StocksBreakCentral(Windcode_Dict,StocksList7,StocksList8,offset=0,KDJFast=True):
	#offset = -3#0 # -1
	select_code = []
	for index  in range( len(StocksList8) ):
		stock = StocksList8[index]
		windcode = stock['windcode']
		if KDJFast:
			W_K= stock['fast_k'][-1-offset]
			W_D= stock['fast_d'][-1-offset]
		else:
			W_K= stock['slow_k'][-1-offset]
			W_D= stock['slow_d'][-1-offset]
		if W_K<=W_D :
			continue
		W_PB1 = stock['pb1'][offset]
		W_PB2 = stock['pb2'][offset]
		W_PB6 = stock['pb6'][offset]
		if W_PB1>W_PB6 and W_PB2>W_PB6:
			select_code.append(windcode)
	# print len(select_code)
	ktype = 7
	ds='ta'+str(ktype)
	rl = []
	
	for index in range(0,len(StocksList7)):
		stock = StocksList7[index]
		windcode = stock['windcode']
		if windcode not in select_code:
			continue
		
		if stock['pct_chg'] <7 :
			continue 
		# print windcode 
		board =Windcode_Dict[ windcode ]
		volume =[ board[ds][x]['volume'] for x in range(0, len(board[ds]) ) ]
		
		avg_volume   = sum( volume[-6-offset:-1-offset] ) / 5
		
		
		# if volume[-1-offset] < max( volume[-30-offset:-1-offset] ) :
		# 	continue
		
		close  =[ board[ds][x]['close' ] for x in range(0, len(board[ds]) ) ]
		high   =[ board[ds][x]['high'  ] for x in range(0, len(board[ds]) ) ]
		low    =[ board[ds][x]['low'   ] for x in range(0, len(board[ds]) ) ]
		if not(close[-1] == board[ds][-1]['open'] and close[-1] >= close[-2]*1.095 ):
			if volume[-1-offset] < avg_volume*1.5 :
				continue
		if len(high[-51-offset:-1-offset]) ==0 or len(low [-51-offset:-1-offset]) == 0 :
			continue
		high60 = max( high[-51-offset:-1-offset] )
		if close[-1] <= high60  :
			continue
		# print windcode
		low60  = min( low [-51-offset:-1-offset] )
		if (high60-low60)/low60 * 100 > 30 :
			continue
		# print windcode
		rl.append(windcode)		
	
	return rl

def StocksMainForceCalc(Windcode_Dict,Connect,WindPy):	
	current_date = datetime.today()
	code_dict={}	
	cursor = Connect.cursor()
	cursor_history = Connect.cursor()
	# sql13 ="select quarter from stocks_basic where btype=13 and windcode=%(code)s order by quarter desc limit 2"
	sql0 = "select quarter from stocks_basic where btype=0 and windcode=%(code)s order by quarter desc limit 2"
	sql = "select content,quarter from stocks_basic where btype=13 and windcode=%(code)s order by quarter desc limit 2"
	sql_history = "select max(high),min(low) from stocks_history_days where ktype=7 and date>=%(date_min)s and date<=%(date_max)s and windcode=%(code)s"
	for windcode in Windcode_Dict :
		da = {'code':windcode,}
		cursor.execute(sql,da)
		rc = cursor.rowcount
		if rc ==0 :
			continue
		bc = cursor.fetchone()
		if  bc[0]< 1 :
			continue
		quarter_current = bc[1]
		if rc == 2 :
			bb = cursor.fetchone()
			quarter_before  = bb[1]
		
		cursor.execute(sql0,da)
		if cursor.rowcount !=2 :
			continue 
		quarter_data_current = cursor.fetchone()[0]
		quarter_data_before = cursor.fetchone()[0]			
		
		
		
		starttime = quarter_current
		endtime = current_date
		typename = ''
		if quarter_current == quarter_data_current :
			if rc==1  or ( rc==2 and quarter_before<quarter_data_before ):
				starttime= quarter_data_before
				endtime = quarter_current
				typename = 'new'
		if quarter_current < quarter_data_current :
			continue
		
		
		if rc==2 and bc[0] > bb[0]  :
			starttime = quarter_before
			endtime   = quarter_current
			typename='inc'
		if rc==2 and bc[0] == bb[0] :
			starttime = quarter_before
			endtime   = quarter_data_current
			typename='equal'
		if typename=='':
			continue
		
		da = {
			'date_max':endtime,
			'date_min':starttime ,
			'code':windcode,
		}
		
		cursor_history.execute(sql_history,da)		
		temp = cursor_history.fetchone()
		
		high = temp[0]
		low = temp[1]
		if high is None or low is None:
			continue
		price = (high+low) /2
		high_list = [] 
		da = {
			'date_max':current_date,
			'date_min':starttime ,
			'code':windcode,
		}
		cursor_history.execute(sql_history,da)
				
		high = cursor_history.fetchone()[0]
		if (high-price)/price*100 > 20 :
			continue
		code_dict[ windcode ] = {
					'type' : typename,
					'ratio' : bc[0],
					'cost_price': price,
					'target_price': price * 1.3,
				}
		
	cursor_history.close()
	sql = "delete from stocks_mainforce where date=%(date)s"
	cursor.execute(sql,{'date':current_date,})
	sql = "Replace into stocks_mainforce(windcode,date,type,ratio,cost,target) values(%(code)s,%(date)s,%(type)s,%(ratio)s,%(cost)s,%(target)s)"
	# f= open('zl.txt','w')
	b0 , b1 , b2 = 0, 0 ,0
	for code in code_dict:
		da = {
			'code' : code,
			'date' : current_date,
			'type' : code_dict[code]['type'],
			'ratio': code_dict[code]['ratio'],
			'cost' : code_dict[code]['cost_price'],
			'target': code_dict[code]['target_price'],
		}
		cursor.execute(sql,da)
		if code_dict[code]['type'] == 'new':
			b0 += 1
		if code_dict[code]['type'] == 'inc':
			b1 += 1
		if code_dict[code]['type'] == 'equal':
			b2 += 1
	# 	f.write( "windcode: %s type:%s \n" % (code,code_dict[code]['type']))
	# f.write( "%d %d %d " %(b0,b1,b2) )
	print "New:%d Inc:%d Equal:%d \n" %(b0,b1,b2)
	# f.close()
	Connect.commit()
	cursor.close()


def main():
	windcode_set = ["000001.SH",'399300.SZ',"399006.SZ","150051.SZ","399005.SZ","399905.SZ"]
	'''
			
	for i in [0,2,5,6,7,8]:
		StocksHistoryUpdate(windcode_set,i,base_days=365*3)
	'''
	#StocksHistoryUpdate(["000001.SH"],7,base_days=365*3)
	# StocksHistoryUpdate(windcode_set ,0,base_days=365*3)
	

if __name__ == '__main__':
 	main()

