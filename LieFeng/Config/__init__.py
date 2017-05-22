from datetime import datetime,timedelta
import time
from functools import wraps
from variable  import *
from common    import *
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

BreakVolumePermitDays = 10
BreakCentralPermitDays = 10

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