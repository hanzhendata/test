from datetime import datetime,timedelta
import time
from functools import wraps
import requests
import variable


def  NanToZero(m):
	if m!=m:
		return 0
	else:
		return m

def  timer(function):
	@wraps(function)
	def function_timer(*args,**kwargs):
		t0 = time.time()
		result=function(*args,**kwargs)
		t1 = time.time()
		print "Total runtime %s:%s seconds" %( function.func_name, str(t1-t0) )
		return result
	return function_timer

def  CheckWindData(Wsdata,Message=""):
	if Wsdata.ErrorCode !=0:
		print 	"ErrorCode=",Wsdata.ErrorCode
		print   "ErrorData ",Wsdata.Data
		if len(Message) >0 :
			print  Message
		return False
	return True

def  CheckKtype(ktype,rl):
	#if (ktype<0 or ktype>6):
	#	return -1
	if ktype==0:
		Barsize=1			
	elif ktype== 1: 
		Barsize=3
	elif	ktype==2: # 5 minute
		Barsize = 5
	elif ktype==3 : # 10 minute
		Barsize=10
	elif ktype==4 :
		Barsize = 15
	elif ktype==5 :
		Barsize = 30
	elif ktype==6 : #60 minute
		Barsize=60
	elif ktype==7 : #day
		Barsize = 1
		SingleTime = timedelta(days=1)
	elif ktype == 8: #week
		Barsize='W'
		SingleTime = timedelta(days=7)
	elif ktype == 9 : #month
		Barsize = 'M'
		SingleTime = timedelta(days=30)
	else:
		print "KType exceed range!"
		return	-1			
	if ktype<7:
		SingleTime = timedelta(minutes=Barsize)
	rl.append(Barsize)
	rl.append(SingleTime) 
	return 0

def  GetFirstTime(windcode,WindPy):
	wsi = WindPy.wss(windcode,"launchdate,ipo_date")
	if not CheckWindData(wsi) :
		return None
	launchdate= wsi.Data[0][0]
	ipo_date  = wsi.Data[1][0]
	if ipo_date> launchdate:
		return ipo_date
	else: 
		return launchdate

def	 CheckDateValid(date,WindPy):
	wsdata=WindPy.tdayscount(date, date, "")
	if  not CheckWindData(wsdata,"tdayscount Error"):      
		return -1
	return wsdata.Data[0][0] #== 0 : # trade day 1
         

def CheckDateTimeValid(MarketType,current_date,WindPy):
	if MarketType ==0:
		#futures
		return False
	else:
		wsdata=WindPy.tdayscount(current_date, current_date, "")
		if  not CheckWindData(wsdata):		
			return False
		if wsdata.Data[0][0] == 0 : # trade day 1
			return False
		current_date = current_date.replace(second=0,microsecond = 0)
		s1 = current_date.replace(hour= 9,minute=30)
		e1 = current_date.replace(hour=11,minute=30)
		s2 = current_date.replace(hour=13,minute=00)
		e2 = current_date.replace(hour=15,minute=00)
		if current_date>=s1 and current_date<=e1 :
			return True
		if current_date>=s2 and current_date<=e2 :
			return True
		return False

def SingleUpdate(windcode,ktype,cursor,w,starttime,endtime,td,SingleTime,Barsize,MarketType):
	if (MarketType==0):
		tablename = "futures_history"
	else:
		tablename = "stocks_history"
	insert_sql = ("INSERT INTO  " + tablename +
			      "(prod_code, ktype, date,"
			      " open_price,high_price,low_price,close_price,"
			      "fast_k,fast_d,fast_j,"
			      "diff,dea,macd,ema12,ma5,slow_k,slow_d ,slow_j ,b2, b1,b3 ,ema26,ma60,"
			      "ema4,pb1, ema6,pb2, ema9,pb3, ema13, pb4,ema18,pb5, ema24,pb6)"			      
			      " VALUES ( %(code)s, "+str(ktype)+", %(time)s, "
			      "%(open_price)s,%(high_price)s,%(low_price)s,%(close_price)s,"
			      "%(fast_k)s,%(fast_d)s,%(fast_j)s,"
			      "%(diff)s,%(dea)s,%(macd)s,%(ema12)s,%(ma5)s, %(slow_k)s,%(slow_d)s,%(slow_j)s,%(b2)s, %(b1)s, %(b3)s, "
			      "%(ema26)s,%(ma60)s,%(ema4)s,%(pb1)s,%(ema6)s,%(pb2)s,%(ema9)s,%(pb3)s,"
			      "%(ema13)s,%(pb4)s,%(ema18)s,%(pb5)s,%(ema24)s,%(pb6)s )")
	kl = ['open_price' , 'high_price', 'low_price', 'close_price', 'fast_k', 'fast_d', 'fast_j',
			 'diff',  'dea', 'macd' ,'ema12',  'ma5', 'slow_k', 'slow_d', 'slow_j', 'b2', 'b1', 'b3',
			  'ema26','ma60','ema4','pb1','ema6','pb2','ema9','pb3','ema13','pb4','ema18','pb5','ema24','pb6']
	#get starttime
	select_sql = "select date from "+tablename+" where prod_code=%(code)s and ktype=%(ktype)s order by date desc limit 1"
	select_data = {
	   'code':windcode,
	   'ktype':ktype,
	}		        
	cursor.execute(select_sql,select_data)
	if (cursor.rowcount==0):
		starttime = endtime - td
	else:
		starttime = cursor.fetchone()[0] + SingleTime
	# print starttime
	#get data from WindPy
	firsttime = GetFirstTime(windcode,w)
	if firsttime is not None and starttime< firsttime:
	    starttime = firsttime
	print starttime,firsttime
	#endtime = endtime.date()
	ss = time.time()
	if ktype<7:
		wsdata =w.wsi(windcode, "open,high,low,close,KDJ,MACD,EXPMA,MA",starttime,endtime,
			"KDJ_N=9;KDJ_M1=3; KDJ_M2=3;KDJ_IO=0;MACD_L=26;MACD_S=12;MACD_N=9;MACD_IO=0;EXPMA_N=12;MA_N=5;BarSize="+str(Barsize)+";Fill=Previous;PriceAdj=F")
		wsi =w.wsi(windcode, "KDJ,BOLL,EXPMA,MA",
			starttime, 		endtime,
			"KDJ_N=21;KDJ_M1=9;KDJ_M2=9;KDJ_IO=0;BOLL_N=26;BOLL_Width=2;BOLL_IO=0;EXPMA_N=26;MA_N=60;BarSize="+str(Barsize)+";Fill=Previous;PriceAdj=F")
	else:
    
		wsdata = w.wsd(windcode, "open,high,low,close",
			starttime,endtime,
			"Period="+str(Barsize)+";Fill=Previous;PriceAdj=F")
		wsi = w.wsd(windcode,"KDJ",
			starttime,endtime,
			"KDJ_N=9;KDJ_M1=3; KDJ_M2=3;KDJ_IO=0;Period="+str(Barsize)+";Fill=Previous;PriceAdj=F"
			)
		for i in range(0, len(wsi.Data) ):
			wsdata.Data.append(wsi.Data[i])
		wsi = w.wsd(windcode,"MACD",
			starttime,endtime,
			"MACD_L=26;MACD_S=12;MACD_N=9;MACD_IO=0;Period="+str(Barsize)+";Fill=Previous;PriceAdj=F"
			)
		for i in range(0, len(wsi.Data) ):
			wsdata.Data.append(wsi.Data[i])
		wsi = w.wsd(windcode,"EXPMA,MA",
			starttime,endtime,
			"EXPMA_N=12;MA_N=5;Period="+str(Barsize)+";Fill=Previous;PriceAdj=F"
			)
		for i in range(0, len(wsi.Data) ):
			wsdata.Data.append(wsi.Data[i])
		wsi = w.wsd(windcode,"KDJ",
			starttime,endtime,
			"KDJ_N=21;KDJ_M1=9; KDJ_M2=9;KDJ_IO=0;Period="+str(Barsize)+";Fill=Previous;PriceAdj=F"
			)
		for i in range(0, len(wsi.Data) ):
			wsdata.Data.append(wsi.Data[i])
		wsi = w.wsd(windcode, "BOLL",
			starttime, endtime,
			"BOLL_N=26;BOLL_Width=2;BOLL_IO=0;Period="+str(Barsize)+";Fill=Previous;PriceAdj=F")
		for i in range(0, len(wsi.Data) ):
			wsdata.Data.append(wsi.Data[i])
		wsi = w.wsd(windcode,"EXPMA,MA",
			starttime,endtime,
			"EXPMA_N=26;MA_N=60;Period="+str(Barsize)+";Fill=Previous;PriceAdj=F"
			)

	if  not CheckWindData(wsdata):		
		return
	for i in range(0, len(wsi.Data) ):
		wsdata.Data.append(wsi.Data[i])
	for i in [4,6,9,13,18,24]:
		if ktype<7:
			wsi =w.wsi(windcode, "EXPMA", starttime, endtime,
			    "EXPMA_N="+str(i)+";BarSize="+str(Barsize)+";Fill=Previous;PriceAdj=F")
			wsima2=w.wsi(windcode, "MA", starttime, endtime,
			    "MA_N="+str(i*2)+";BarSize="+str(Barsize)+";Fill=Previous;PriceAdj=F")
			wsima4=w.wsi(windcode, "MA", starttime, endtime,
			    "MA_N="+str(i*4)+";BarSize="+str(Barsize)+";Fill=Previous;PriceAdj=F")
		else:
			wsi = w.wsd(windcode, "EXPMA",    starttime,    endtime,
			"EXPMA_N="+str(i)+";Period="+str(Barsize)+";Fill=Previous;PriceAdj=F")
			wsima2=w.wsd(windcode, "MA", starttime, endtime,
			    "MA_N="+str(i*2)+";BarSize="+str(Barsize)+";Fill=Previous;PriceAdj=F")
			wsima4=w.wsd(windcode, "MA", starttime, endtime,
			    "MA_N="+str(i*4)+";BarSize="+str(Barsize)+";Fill=Previous;PriceAdj=F")
		wsdata.Data.append(wsi.Data[0])
		pb=[]
		for index in range(0,len(wsi.Data[0])):
			pb.append( (wsi.Data[0][index] + wsima2.Data[0][index] + wsima4.Data[0][index])/3 )		
		wsdata.Data.append(pb)
	se = time.time()
	print 'WindPy %s runs %0.2f seconds.' % (windcode, (se - ss))
	#data to liefeng database
	ss = time.time()
	insert_num = 0
	for wtime in wsdata.Times: 
		index  = wsdata.Times.index(wtime)
		history_data = {
			'code'		:windcode,
			'time'		:wtime - timedelta(microseconds=wtime.microsecond),					
		}		
		for i in range(0,len(kl)):
			history_data[ kl[i] ] = NanToZero(wsdata.Data[i][index])				
		cursor.execute(insert_sql,history_data)
		insert_num = insert_num +1
	se = time.time()
	print 'Mysql %s runs %0.2f seconds.' % (windcode, (se - ss))
	print insert_num


def SingleUpdateMin(windcode,ktype,cursor,w,starttime,endtime,td,SingleTime,Barsize,MarketType,isAutoTrans=False):
	if (MarketType==0):
		tablename = "futures_history"
	else:
		tablename = "stocks_history"
	insert_sql = ("INSERT INTO  " + tablename +
			      "(prod_code, ktype, date,"
			      " open_price,high_price,low_price,close_price ) VALUES "		      
			      " ( %(code)s, "+str(ktype)+", %(time)s, "
			      "%(open_price)s,%(high_price)s,%(low_price)s,%(close_price)s )")
	kl = ['open_price' , 'high_price', 'low_price', 'close_price']
	#get starttime
	select_sql = "select date from "+tablename+" where prod_code=%(code)s and ktype=%(ktype)s order by date desc limit 1"
	select_data = {
	   'code':windcode,
	   'ktype':ktype,
	}		        
	cursor.execute(select_sql,select_data)
	if (cursor.rowcount==0):
		starttime = endtime - td
	else:
		starttime = cursor.fetchone()[0] + SingleTime
	print starttime
	#get data from WindPy
	firsttime = GetFirstTime(windcode,w)
	if starttime< firsttime:
	    starttime = firsttime
	print starttime,firsttime
	#get data from WindPy
	ss = time.time()
	if ktype<7:
		wsdata =w.wsi(windcode, "open,high,low,close",
			starttime, 		endtime,
		    "BarSize="+str(Barsize)+";Fill=Previous;PriceAdj=F")		
	else:
		wsdata = w.wsd(windcode, "open,high,low,close",
			starttime,endtime,
			"Period="+str(Barsize)+";Fill=Previous;PriceAdj=F")
	se = time.time()
	print 'WindPy %s runs %0.2f seconds.' % (windcode, (se - ss))
	if not CheckWindData(wsdata):		
		return
	cursor.execute("START TRANSACTION;SET autocommit=0;")
	#data to liefeng database
	insert_num = 0
	ss = time.time()
	for wtime in wsdata.Times: 
		index  = wsdata.Times.index(wtime)
		history_data = {
			'code'		:windcode,
			'time'		:wtime - timedelta(microseconds=wtime.microsecond),					
		}		
		for i in range(0,len(kl)):
			history_data[ kl[i] ] = NanToZero(wsdata.Data[i][index])				
		cursor.execute(insert_sql,history_data)
		insert_num = insert_num +1
	cursor.execute("COMMIT;")
	se = time.time()
	print 'Mysql  %s runs %0.2f seconds.' % (windcode, (se - ss))
	print insert_num


def SingleUpdateMin_LoadDataInfile(windcode,ktype,cursor,w,starttime,endtime,td,SingleTime,Barsize,MarketType,isAutoTrans=False):
	if (MarketType==0):
		tablename = "futures_history"
	else:
		tablename = "stocks_history"
	kl = ['open_price' , 'high_price', 'low_price', 'close_price']
	#get starttime
	select_sql = "select date from "+tablename+" where prod_code=%(code)s and ktype=%(ktype)s order by date desc limit 1"
	select_data = {
	   'code':windcode,
	   'ktype':ktype,
	}		        
	cursor.execute(select_sql,select_data)
	if (cursor.rowcount==0):
		starttime = endtime - td
	else:
		starttime = cursor.fetchone()[0] + SingleTime
	# print starttime
	#get data from WindPy
	firsttime = GetFirstTime(windcode,w)
	if firsttime is not None and  starttime< firsttime:
	    starttime = firsttime
	# print starttime,firsttime
	#get data from WindPy
	ss = time.time()
	if ktype<7:
		wsdata =w.wsi(windcode, "open,high,low,close",
			starttime, 		endtime,
		    "BarSize="+str(Barsize)+";Fill=Previous;PriceAdj=F")		
	else:
		wsdata = w.wsd(windcode, "open,high,low,close",
			starttime,endtime,
			"Period="+str(Barsize)+";Fill=Previous;PriceAdj=F")
	se = time.time()
	# print 'WindPy %s runs %0.2f seconds.' % (windcode, (se - ss))
	if not CheckWindData(wsdata):		
		return

	 
	# print wsdata
	
	f = open("temp.txt",'w')
	ss = time.time()
	insert_num = 0	
	for wtime in wsdata.Times: 
		index  = wsdata.Times.index(wtime)
		if wsdata.Data[0][index] is None :
			continue					
		temp = "%s,%d,%s,%.2f,%.2f,%.2f,%.2f\n" % (windcode,ktype,
			wtime.strftime("%Y-%m-%d %H:%M:%S"),wsdata.Data[0][index],
			wsdata.Data[1][index],wsdata.Data[2][index],wsdata.Data[3][index])
		insert_num += 1
		f.write(temp)
	f.close()
	
	# print insert_num
	se = time.time()
	# print 'FileSave  %s runs %0.2f seconds.' % (windcode, (se - ss))
	ss = time.time()
	cursor.execute("START TRANSACTION;")
	cursor.execute("SET autocommit=0;")
	#data to liefeng database	
	temp ="load data local infile 'temp.txt'  into table stocks_history character set utf8 fields terminated by ','  lines terminated by '\n' (`prod_code`,`ktype`,`date`,`open_price`,`high_price`,`low_price`,`close_price`);"
	cursor.execute(temp)
	cursor.execute("COMMIT;")
	se = time.time()
	# print 'Mysql  %s runs %0.2f seconds.' % (windcode, (se - ss))


def UpdateTechicalAnalysis_LoadDataInfile(Windcode,MarketType,Ktype,Connector,Indicator,Ta):
	if (MarketType==0):
		tablename = "futures_history"
	else:
		tablename = "stocks_history"
	kl = ['open_price' , 'high_price', 'low_price', 'close_price',
	      "fast_k","fast_d","fast_j","diff","dea","macd","ema12","ma5",
	      "slow_k","slow_d","slow_j","b2", "b1","b3" ,"ema26","ma60",
		  "ema4","pb1", "ema6","pb2","ema9","pb3", "ema13", "pb4","ema18","pb5", "ema24","pb6"	]
	strformat = "%s,%d,%s,"+ "%.2f,"*(len(kl)-1)+"%.2f\n"
	f = open("temp.txt",'w')
	ss = time.time()
	insert_num = 0	
	for index in range(0,len(Indicator)): 
			
		temp = strformat  % (Windcode,Ktype,Indicator[index]['date'].strftime("%Y-%m-%d %H:%M:%S"),
			Indicator[index]['open'],Indicator[index]['high'],Indicator[index]['low'],Indicator[index]['close'],
			Ta['FAST_K'][index],Ta['FAST_D'][index],Ta['FAST_J'][index],Ta['DIFF'][index],Ta['DEA'][index],Ta['MACD'][index],
			Ta['EMA_FAST'][index],Ta['MA5'][index],Ta['SLOW_K'][index],Ta['SLOW_D'][index],Ta['SLOW_J'][index],
			Ta['B1'][index],Ta['B2'][index],Ta['B3'][index],Ta['EMA_SLOW'][index],Ta['MA60'][index],Ta['EMA4'][index],
			Ta['PB1'][index],Ta['EMA6'][index],Ta['PB2'][index],Ta['EMA9'][index],Ta['PB3'][index],Ta['EMA13'][index],
			Ta['PB4'][index],Ta['EMA18'][index],Ta['PB5'][index],Ta['EMA24'][index],Ta['PB6'][index]
			)
		insert_num += 1
		f.write(temp)
	f.close()
	print insert_num
	se = time.time()
	print 'FileSave  %s runs %0.2f seconds.' % (Windcode, (se - ss))
	strformat = reduce(lambda x,y: x+',`'+y+'`',kl)
	ss = time.time()
	cursor = Connector.cursor()
	cursor.execute("START TRANSACTION;")
	cursor.execute("SET autocommit=0;")
	#data to liefeng database	
	temp ="load data local infile 'temp.txt' replace into table stocks_history character set utf8 fields terminated by ','  lines terminated by '\n' (`prod_code`,`ktype`,`date`,"+strformat+");"
	cursor.execute(temp)
	cursor.execute("COMMIT;")
	se = time.time()
	print 'Mysql  %s runs %0.2f seconds.' % (Windcode, (se - ss))
	cursor.close()

def SingleUpdateDays_LoadDataInfile(windcode,ktype,cursor,WindPy,starttime,endtime,SingleTime ):
	if ktype<7:
		return
	select_sql = "select date from stocks_history_days where windcode=%(code)s and ktype=%(ktype)s order by date desc limit 1"
	select_data = {
	   'code':windcode,
	   'ktype':ktype,
	}		        
	cursor.execute(select_sql,select_data)
	if (cursor.rowcount>0):
		starttime = cursor.fetchone()[0] + SingleTime
	
	#get data from WindPy
	firsttime = GetFirstTime(windcode,WindPy)
	print firsttime
	if firsttime is not None and  starttime< firsttime:  
	    starttime = firsttime	
	if starttime.year <1900 :
		if '.WI' in windcode and '886' in windcode:
			starttime = datetime(year=2000,month=1,day=1)
		else:
			return
	print starttime

	if ktype==7:
		para = "showblank=0;PriceAdj=F"
	elif ktype==8:
		para = "showblank=0;Period=W;PriceAdj=F"
	else:
		return
	ss = time.time()
	wsdata = WindPy.wsd( windcode , "open,high,low,close,volume,pct_chg,chg,mkt_cap_ard",starttime,endtime,para )
	se = time.time()
	print 'WindPy %s runs %0.2f seconds.' % (windcode, (se - ss))
	if not CheckWindData(wsdata):		
		return
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
		temp = "%s,%d,%s,%f,%f,%f,%f,%f,%f,%f,%f\n" % (windcode,ktype,
			wtime.strftime("%Y-%m-%d %H:%M:%S"),wsdata.Data[0][index],
			wsdata.Data[1][index],wsdata.Data[2][index],wsdata.Data[3][index],
			wsdata.Data[4][index],wsdata.Data[5][index],wsdata.Data[6][index],wsdata.Data[7][index])
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
	temp ="load data local infile '"+filename+"' ignore into table stocks_history_days character set utf8 fields terminated by ','  lines terminated by '\n' (`windcode`,`ktype`,`date`,`open`,`high`,`low`,`close`,`volume`,`pct_chg`,`chg`,`cap`);"
	cursor.execute(temp)
	cursor.execute("COMMIT;")
	se = time.time()
	print 'Mysql  %s runs %0.2f seconds.' % (windcode, (se - ss))

def SingleUpdateMinutes_LoadDataInfile(windcode,ktype,cursor,WindPy,starttime,endtime,SingleTime,Barsize ):
	if ktype>7:
		return
	select_sql = "select date from stocks_history_minutes where windcode=%(code)s and ktype=%(ktype)s order by date desc limit 1"
	select_data = {
	   'code':windcode,
	   'ktype':ktype,
	}		        
	cursor.execute(select_sql,select_data)
	if (cursor.rowcount>0):
		starttime = cursor.fetchone()[0] + SingleTime
	
	#get data from WindPy
	firsttime = GetFirstTime(windcode,WindPy)
	# print firsttime
	if firsttime is not None and  starttime< firsttime:  
	    starttime = firsttime	
	if starttime.year <1900 :
		return
	print starttime
	# tdo=WindPy.tdaysoffset(-1, endtime, "")
	# if not CheckWindData(tdo):
	# 	return
	# print tdo.Data[0][0]
	# if tdo.Data[0][0] > starttime :
	# 	print 'Database already has data!'
	# 	return
	para = "BarSize="+str(Barsize)+";Fill=Previous;PriceAdj=F"
	
	ss = time.time()
	
	wsdata = WindPy.wsi( windcode , "open,high,low,close,volume,amt,chg,pct_chg",starttime,endtime,para )
	se = time.time()
	print 'WindPy %s runs %0.2f seconds.' % (windcode, (se - ss))
	if not CheckWindData(wsdata):		
		return
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
		temp = "%s,%d,%s,%f,%f,%f,%f,%f,%f,%f,%f\n" % (windcode,ktype,
			wtime.strftime("%Y-%m-%d %H:%M:%S"),wsdata.Data[0][index],
			wsdata.Data[1][index],wsdata.Data[2][index],wsdata.Data[3][index],
			wsdata.Data[4][index],wsdata.Data[5][index],wsdata.Data[6][index],wsdata.Data[7][index])
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
	temp ="load data local infile '"+filename+"' ignore into table stocks_history_minutes character set utf8 fields terminated by ','  lines terminated by '\n' (`windcode`,`ktype`,`date`,`open`,`high`,`low`,`close`,`volume`,`amount`,`pct_chg`,`chg`);"
	cursor.execute(temp)
	cursor.execute("COMMIT;")
	se = time.time()
	print 'Mysql  %s runs %0.2f seconds.' % (windcode, (se - ss))

def WindKtypeToTdxCategory(ktype):
    nCategory = -1
    if ktype==0 :
        nCategory = 8
    elif ktype==2 :
        nCategory = 0
    elif ktype==4 :
        nCategory = 1
    elif ktype==5 :
        nCategory = 2
    elif ktype==6 :
        nCategory = 3
    elif ktype==7 :
        nCategory = 4
    elif ktype==8 :
        nCategory = 5
    elif ktype==9 :
        nCategory = 6
    elif ktype==10 :
        nCategory = 10
    elif ktype==11 :
        nCategory = 11
    return nCategory

def EventDrivenStocks():
    global stocksEventDriven
    url = "https://www.hanzhendata.com/ihanzhendata/logicstocks/all"
    response = requests.request("GET", url)
    stocksEventDriven = []
    if response.status_code == 200 :
        data = response.json()
        # print 'EventDrivenStocks ',response.status_code,data
        if data['status'] == 1 :
            stocksEventDriven = data['data']
    requests.request("GET", url, headers={'Connection':'close'})
    return stocksEventDriven

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

def GetStocksSType(Connector,stype,ktype,stocksCalcDate,ExistedFlag=False):
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

def GetStocksBasic(Connector,stocks):
   
    stocksBasic = []
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
    return stocksBasic

def UserStocks():
    stocksUser = {}
    url = "http://www.hanzhendata.com/ihanzhendata/stock/user_stock"
    response = requests.request("GET", url)
    if response.status_code == 200 :
        data = response.json()
        print 'UserStocks ',response.status_code,data
        if data['status'] == 1 :
            for stock_user in data['data'] :
                uid =  stock_user['uid']
                dtime = datetime.strptime( stock_user['time'],"%Y-%m-%d %H:%M:%S")
                if stocksUser.get(uid) is None:
                    
                    stocksUser[ uid ]= {stock_user['windcode']:{'warning_status':4,'warning_date':dtime,'warning_price':stock_user['average_price'],'amount':stock_user['amount']},}
                else:
                    stocksUser[ uid ].update({
                        stock_user['windcode']:{
                        'warning_status':4,
                        'warning_date':dtime,
                        'warning_price':
                        stock_user['average_price'],
                        'amount':stock_user['amount']}})
            # stocksUser = data['data']
    requests.request("GET", url, headers={'Connection':'close'})    
    return stocksUser

def WarningMessage_Delete(builddate):
    delete_url="http://www.hanzhendata.com/ihanzhendata/warning/deleteworder"       
    delete_url=delete_url+"?start=" +builddate.strftime("%Y-%m-%d")+"&end=" + (builddate+timedelta(days=1)).strftime("%Y-%m-%d")
    response = requests.request("GET", delete_url)
    if response.status_code == 200 :
        print 'Delete WarningMessage ',response.status_code,response.json()
    requests.request("GET", delete_url, headers={'Connection':'close'})
    cursor = Connector.cursor()
    sql = "delete from stocks_warning where "
    sql = sql + "date>'"+(builddate-timedelta(days=1)).replace(hour=16).strftime("%Y-%m-%d %H:%M:%S") + "' and date<'"+(builddate+timedelta(days=1)).replace(hour=8).strftime("%Y-%m-%d %H:%M:%S")+"' "
    print sql
    cursor.execute(sql)
    Connector.commit()
    cursor.close()

def SendWarningMessage(message):
    url = "http://www.hanzhendata.com/ihanzhendata/warning/buyorder"

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

def Get5MintueNumber(current_time):
	worktime=[]	
	for wt in variable.Stocks_WorkingTime:
		worktime.append({
			'begin':datetime.strptime(wt['begin'],"%H%M"),
			'end'  :datetime.strptime(wt['end']  ,"%H%M"),
			})
	rt = 0
	for wt in worktime:
		begin = current_time.replace( hour=wt['begin'].hour,  minute=wt['begin'].minute )
		end   = current_time.replace( hour=  wt['end'].hour  ,  minute=  wt['end'].minute )
		bs = (current_time-begin).total_seconds()
		es = (end-current_time).total_seconds()
		if bs>=0 and es>0 :
			rt = bs / 300
			break
	return rt


