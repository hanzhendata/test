#!/usr/bin/env python
# -*- coding: utf-8 -*-

import  numpy 
import  talib  
import  mysql.connector
import  math
from Config import *
'''
author: fenglixin
update date:2016-10-3 3:05
tips: 1 minute slice to other minute data
'''

class NumpyMySQLConverter(mysql.connector.conversion.MySQLConverter):
    """ A mysql.connector Converter that handles Numpy types """

    def _float32_to_mysql(self, value):
        return float(value)

    def _float64_to_mysql(self, value):
        return float(value)

    def _int32_to_mysql(self, value):
        return int(value)

    def _int64_to_mysql(self, value):
        return int(value)

def SliceMinuteUpdate(prod_code,MarketType,ktype_list,connect,tafunc):
	#if ktype ==6 :
	#	return SingleSlice60Minute(prod_code,MarketType,connect)
	cursor = connect.cursor()
	select_cursor = connect.cursor()
	if (MarketType==0):
		tablename = "futures_history"
	else:
		tablename = "stocks_history"
	
	sql = "select date,open_price,high_price,low_price,close_price from "+tablename+" where prod_code=%(prod_code)s and ktype=0 order by date asc" 
	sql_data= {
		#'tablename' :tablename,
		'prod_code' : prod_code,
	} 
	
	for ktype in ktype_list :
		rl=[]
		if (CheckKtype(ktype,rl)!=0):
			continue		
		Barsize = rl[0]
		SingleTime = rl[1]	
		select_sql = "select date from %s where prod_code='%s' and ktype=%d order by date desc limit 1" % (tablename,prod_code,ktype)
		select_cursor.execute(select_sql)
		if (select_cursor.rowcount==0):
			zero_sql = sql 
		else:
			begindate = select_cursor.fetchone()[0] 
			zero_sql = "select date,open_price,high_price,low_price,close_price from "+tablename+" where prod_code=%(prod_code)s and ktype=0 and date>%(date)s order by date asc"
			sql_data['date'] = begindate.strftime('%Y-%m-%d %H:%M:%S')
		cursor.execute(zero_sql, sql_data)
		if cursor.rowcount==0 :
			continue
		indicator = []
		high1 =  []
		low1 = []
		tempdict = {}

		if(ktype != 6 ):
			for date,open_price,high_price,low_price,close_price in cursor:
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
					tempdict[ 'close'] = close_price
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
			for date,open_price,high_price,low_price,close_price in cursor:
				minute = date.time().minute
				hour  = date.time().hour
				high1.append(high_price)
				low1.append(low_price)
				if ( hour<12 and minute % (Barsize/2) == 1 ) or (hour>11 and minute % Barsize == 1)  :
					tempdict['open'] = open_price
				elif ( hour<12 and minute % (Barsize/2) == 0 ) or (hour>11 and minute % Barsize == 0) :		
					if len(high1)!=Barsize :
						if hour<12 and minute == 0 :
							continue
						high1 = []
						low1 = []
						continue
					tempdict['close'] = close_price
					if hour==13 and minute == 0 :
						date = date.replace(hour=11,minute=30)
					tempdict['date'] = date
					tempdict['high'] = max(high1)
					tempdict['low'] = min(low1)

					indicator.append(tempdict)
					high1 = []
					low1  = []
					tempdict = {}
		if tafunc is not None :
			tafunc(indicator,prod_code,MarketType,ktype,connect)
	cursor.close()	
	return True
def SingleSliceMinute(prod_code,MarketType,ktype,connect):
	if ktype ==6 :
		return SingleSlice60Minute(prod_code,MarketType,connect)
	cursor = connect.cursor()
	rl=[]
	#ktype = 2
	if (CheckKtype(ktype,rl)!=0 or ktype>6):
		return None		
	Barsize = rl[0]
	SingleTime = rl[1]
	if (MarketType==0):
		tablename = "futures_history"
	else:
		tablename = "stocks_history"
	sql = "select date,open_price,high_price,low_price,close_price from %s where prod_code='%s' and ktype=0 order by date asc"  % (tablename,prod_code)
	cursor.execute(sql)
	indicator = []
	high1 =  []
	low1 = []
	tempdict = {}
	for date,open_price,high_price,low_price,close_price in cursor:
		minute = date.time().minute
		high1.append(high_price)
		low1.append(low_price)
		if (minute % Barsize) == 1 :
			tempdict['open'] = open_price
		elif (minute % Barsize) == 0 :		
			if len(high1)!=Barsize :
				high1 = []
				low1 = []
				continue
			tempdict['close'] = close_price
			if date.time().hour == 13 and minute == 0 :
				date = date.replace(hour=11,minute=30)
			tempdict['date'] = date
			tempdict['high'] = max(high1)
			tempdict['low'] = min(low1)
			indicator.append(tempdict)
			high1 = []
			low1  = []
			tempdict = {}
	cursor.close()	
	return indicator
def SingleSlice60Minute(prod_code,MarketType,connect):
	cursor = connect.cursor()
	rl=[]
	ktype = 6
	if (CheckKtype(ktype,rl)!=0):
		return None		
	Barsize = rl[0]
	SingleTime = rl[1]
	if (MarketType==0):
		tablename = "futures_history"
	else:
		tablename = "stocks_history"
	sql = "select date,open_price,high_price,low_price,close_price from %s where prod_code='%s' and ktype=0 order by date asc"  % (tablename,prod_code)
	cursor.execute(sql)
	indicator = []
	high1 =  []
	low1 = []
	tempdict = {}
	for date,open_price,high_price,low_price,close_price in cursor:
		minute = date.time().minute
		hour = date.time().hour
		high1.append(high_price)
		low1.append(low_price)
		if ( hour<12 and minute % (Barsize/2) == 1 ) or (hour>11 and minute % Barsize == 1)  :
			tempdict['open'] = open_price
		elif ( hour<12 and minute % (Barsize/2) == 0 ) or (hour>11 and minute % Barsize == 0) :		
			if len(high1)!=Barsize :
				if hour<12 and minute == 0 :
					continue
				high1 = []
				low1 = []
				continue
			tempdict['close'] = close_price
			if hour == 13 and minute == 0 :
				date = date.replace(hour=11,minute=30)
			tempdict['date'] = date
			tempdict['high'] = max(high1)
			tempdict['low'] = min(low1)
			indicator.append(tempdict)
			high1 = []
			low1  = []
			tempdict = {}
	cursor.close()	
	return indicator

def CalcTechicalAnalysis(indicator,prod_code,MarketType,ktype,connect,fastperiod=12, slowperiod=26, signalperiod=9,fast_fastk_period=9, fast_slowk_period=3, fast_fastd_period =3,slow_fastk_period= 21, slow_slowk_period=9, slow_fastd_period=9,isSaved=True,UpdateFlag=False):
	if len(indicator)==0 :
		return None
	high = numpy.array([ indicator[x]['high'] for x in range(0, len(indicator) ) ], dtype = numpy.float)
	low  = numpy.array([ indicator[x]['low' ] for x in range(0, len(indicator) ) ], dtype = numpy.float)
	close =numpy.array([ indicator[x]['close'] for x in range(0, len(indicator)) ], dtype = numpy.float)
	ema12 = talib.EMA(close,fastperiod)
	ema26 = talib.EMA(close,slowperiod)
	dif = ema12-ema26
	dea = talib.EMA(numpy.array(dif,dtype= numpy.float),signalperiod)
	bar = (dif-dea) * 2 

	if isSaved :
		if (MarketType==0):
			tablename = "futures_history"
		else:
			tablename = "stocks_history"
		if UpdateFlag :
			insert_sql = ("Update " + tablename +
			      " set open_price=%(open_price)s,high_price=%(high_price)s,low_price=%(low_price)s,"
			      "close_price=%(close_price)s,"
			      " fast_k=%(fast_k)s, fast_d=%(fast_d)s, fast_j=%(fast_j)s,"
			      "diff=%(diff)s,dea=%(dea)s,macd=%(macd)s,ema12=%(ema12)s,ma5=%(ma5)s,"
			      "slow_k=%(slow_k)s,slow_d=%(slow_d)s,"
			      "slow_j=%(slow_j)s,b2=%(b2)s, b1= %(b1)s, b3=%(b3)s, "
			      "ema26=%(ema26)s,ma60=%(ma60)s,ema4=%(ema4)s,pb1=%(pb1)s,ema6=%(ema6)s,pb2=%(pb2)s,ema9=%(ema9)s,"
			      "pb3=%(pb3)s,ema13=%(ema13)s,pb4=%(pb4)s,ema18=%(ema18)s,"
			      "pb5=%(pb5)s,ema24=%(ema24)s,pb6=%(pb6)s  "
			      " where prod_code=%(code)s and ktype="+str(ktype)+" and date=%(time)s")

		else:
			insert_sql = ("INSERT INTO  " + tablename +
			      "(prod_code, ktype, date,"
			      " open_price,high_price,low_price,close_price,"
			      "fast_k,fast_d,fast_j,"
			      "diff,dea,macd,ema12,ma5,slow_k,slow_d ,slow_j ,b2, b1,b3 ,ema26,ma60,"
			      "ema4,pb1, ema6,pb2, ema9,pb3, ema13, pb4,ema18,pb5, ema24,pb6)"			      
			      " VALUES ( %(code)s, "+str(ktype)+", %(time)s, "
			      "%(open_price)s,%(high_price)s,%(low_price)s,%(close_price)s,"
			      "%(fast_k)s,%(fast_d)s,%(fast_j)s,"
			      "%(diff)s,%(dea)s,%(macd)s,%(ema12)s,%(ma5)s, %(slow_k)s,%(slow_d)s,"
			      "%(slow_j)s,%(b2)s, %(b1)s, %(b3)s, "
			      "%(ema26)s,%(ma60)s,%(ema4)s,%(pb1)s,%(ema6)s,%(pb2)s,%(ema9)s,%(pb3)s,"
			      "%(ema13)s,%(pb4)s,%(ema18)s,%(pb5)s,%(ema24)s,%(pb6)s )")
		
		fast_k,fast_d,fast_j=MyKDJ(high,low,close,fast_fastk_period, fast_slowk_period, fast_fastd_period)
		slow_k,slow_d,slow_j=MyKDJ(high,low,close,slow_fastk_period, slow_slowk_period, slow_fastd_period)
		cursor = connect.cursor()
		ma5 = talib.SMA(close, timeperiod=5)
		ma60 = talib.SMA(close, timeperiod=60)
		b1, b2, b3 = talib.BBANDS(close, timeperiod=26, nbdevup=1, nbdevdn=1, matype=0) 
		ema = []
		pb = []
		for i in [4,6,9,13,18,24]:
			emapb = talib.EMA(close,i)
			ma1 = talib.SMA(close, timeperiod=2*i)
			ma2 = talib.SMA(close, timeperiod=4*i)
			ema.append(emapb)
			pb.append( (emapb+ma1+ma2)/3 )
		for index in range(0,len(indicator)):
			history_data = {
				'code'		: prod_code,
				'time'		: indicator[index]['date'],	
				'open_price': indicator[index]['open'],
				'high_price': indicator[index]['high'],
			    'low_price':  indicator[index]['low'],
				'close_price': indicator[index]['close'],
				'diff'		: NanToZero(dif[index]),
				'dea'       : NanToZero(dea[index]),
				'macd'		: NanToZero(bar[index]),
				'ema12' : NanToZero(ema12[index]),
			    'ema26' : NanToZero(ema26[index]),
				'fast_k' : NanToZero(fast_k[index]),
				'fast_d' : NanToZero(fast_d[index]),
				'fast_j' : NanToZero(fast_j[index]),
				'slow_k' : NanToZero(slow_k[index]),
				'slow_d' : NanToZero(slow_d[index]),
				'slow_k' : NanToZero(slow_k[index]),
				'slow_j' : NanToZero(slow_j[index]),
				'ma5'	:  NanToZero(ma5[index]) ,
				'ma60'	:  NanToZero(ma60[index]),
				'b1'	:  NanToZero(b1[index]),
				'b2'	:  NanToZero(b2[index]),
				'b3'	:  NanToZero(b3[index]),			
			}
			j = 0
			for i in [4,6,9,13,18,24]:
				history_data [ 'ema'+str(i) ]	= NanToZero(ema[j][index])
				history_data [ 'pb'+str(j+1)]	= NanToZero(pb[j][index] )
				j = j + 1				
			cursor.execute(insert_sql,history_data)
		connect.commit()
		cursor.close()
	else :
		return [ema12,ema26,dif,dea,bar]


def SMA_CN(close, timeperiod) :
    close = numpy.nan_to_num(close)
    return reduce(lambda x, y: ((timeperiod - 1) * x + y) / timeperiod, close)
    

def KDJ_CN(high, low, close, fastk_period, slowk_period, fastd_period) :
    kValue, dValue = talib.STOCHF(high, low, close, fastk_period, fastd_period=1, fastd_matype=0)
    
    kValue = numpy.array(map(lambda x : SMA_CN(kValue[:x], slowk_period), range(1, len(kValue) + 1)))
    dValue = numpy.array(map(lambda x : SMA_CN(kValue[:x], fastd_period), range(1, len(kValue) + 1)))
    
    jValue = 3 * kValue - 2 * dValue
    
    # func = lambda arr : numpy.array([0 if x < 0 else (100 if x > 100 else x) for x in arr])
    
    # kValue = func(kValue)
    # dValue = func(dValue)
    # jValue = func(jValue)
    return kValue, dValue, jValue
def MySMA(close,timeperiod) :
	close = numpy.nan_to_num(close)
	if len(close) <= timeperiod :
		return numpy.array( [numpy.nan]*len(close) ) 	
	temp = 0
	SMA = [0.0]
	for index in range( 0 ,len(close)) :
		SMA.append( (SMA[-1] *(timeperiod-1)+close[index])/timeperiod )
	return numpy.array(SMA[1:])
def MyKDJ(high, low, close, fastk_period, slowk_period, fastd_period) :
    kValue, dValue = talib.STOCHF(high, low, close, fastk_period=fastk_period, fastd_period=1, fastd_matype=0)
    kValue  = MySMA(kValue,slowk_period)
    dValue  = MySMA(kValue,fastd_period)    
    
    jValue = 3 * kValue - 2 * dValue    

    return kValue, dValue, jValue
def RSI_CN(close, timeperiod) :
    diff = map(lambda x, y : x - y, close[1:], close[:-1])
    diffGt0 = map(lambda x : 0 if x < 0 else x, diff)
    diffABS = map(lambda x : abs(x), diff)
    diff = numpy.array(diff)
    diffGt0 = numpy.array(diffGt0)
    diffABS = numpy.array(diffABS)
    diff = numpy.append(diff[0], diff)
    diffGt0 = numpy.append(diffGt0[0], diffGt0)
    diffABS = numpy.append(diffABS[0], diffABS)
    rsi = map(lambda x : SMA_CN(diffGt0[:x], timeperiod) / SMA_CN(diffABS[:x], timeperiod) * 100 )
    return numpy.array(rsi)

def myMACD(price, fastperiod=12, slowperiod=26, signalperiod=9):
    ewma12 = pd.ewma(price,span=fastperiod)
    ewma26 = pd.ewma(price,span=slowperiod)
    dif = ewma12-ewma26
    dea = pd.ewma(dif,span=signalperiod)
    bar = (dif-dea) *2
    return dif,dea,bar
 
def  myema(close,N):	 
	 if len(close) <N :
		 return Nan
	 yn = 0
	 k = 2.0/(N+1)
	 for index in range(1,len(close)):
	     if index<N :
		 yn = yn + close[index]
	     elif index == N :
		yn = float( (yn+ close[N])/N)  
	     else :
		ynplus = ( k * float(close[index] )+ (1-k)*yn ) 
		if len(close) - index <10 :
		     print  index,ynplus
		yn = ynplus
	 return True

def myema_special(close,N,yn):	 
	k = 2.0/(N+1)
	ynplus = ( k * float(close) + (1-k)*yn ) 
	return ynplus

def SliceMinuteUpdate_New(prod_code,ktype_list,connect,UpdateFlag = True):
	
	cursor = connect.cursor()
	select_cursor = connect.cursor()
	tablename = "stocks_history_minutes"
	
	sql = "select date,open,high,low,close,volume,amount,pct_chg,chg from "+tablename+" where windcode=%(prod_code)s and ktype=0 order by date asc" 
	sql_data= {
		#'tablename' :tablename,
		'prod_code' : prod_code,
	} 
	if UpdateFlag :
		filename ="temp.txt"
		f = open(filename,'w')	
	
	
	 
	for ktype in ktype_list :
		rl=[]
		if (CheckKtype(ktype,rl)!=0):
			continue		
		Barsize = rl[0]
		SingleTime = rl[1]	
		select_sql = "select date from %s where windcode='%s' and ktype=%d order by date desc limit 1" % (tablename,prod_code,ktype)
		select_cursor.execute(select_sql)
		if (select_cursor.rowcount==0):
			zero_sql = sql 
		else:
			begindate = select_cursor.fetchone()[0] 
			zero_sql = "select date,open,high,low,close,volume,amount,pct_chg,chg from "+tablename+" where windcode=%(prod_code)s and ktype=0 and date>%(date)s order by date asc"
			sql_data['date'] = begindate.strftime('%Y-%m-%d %H:%M:%S')
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
				if ( hour<12 and minute % (Barsize/2) == 1 ) or (hour>11 and minute % Barsize == 1)  :
					tempdict['open'] = open_price
				elif ( hour<12 and minute % (Barsize/2) == 0 ) or (hour>11 and minute % Barsize == 0) :		
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
		temp ="load data local infile '"+filename+"' ignore into table stocks_history_minutes character set utf8 fields terminated by ','  lines terminated by '\n'  (`windcode`,`ktype`,`date`,`open`,`high`,`low`,`close`,`volume`,`amount`,`pct_chg`,`chg`);"
		cursor.execute(temp)
		cursor.execute("COMMIT;")
	cursor.close()	
	return True

def SliceMinute_WindPy(windcode,WindPy,starttime,endtime):
	
	wsi = WindPy.wsi(windcode,"open,high,low,close,volume",starttime,endtime,"PriceAdj=F")	
	if not CheckWindData(wsi,Message= windcode+' WSI Error') :
		return None
	W= [ ]
	for index in range( len(wsi.Times)):
		if math.isnan(wsi.Data[0][index]) or math.isnan(wsi.Data[1][index]) or \
			math.isnan(wsi.Data[2][index]) or math.isnan(wsi.Data[3][index])  :#or math.isnan(wsi.Data[4][index])
			continue
		W.append ({
			'date' : wsi.Times[index],
			'open' : wsi.Data[0][index],
			'high' : wsi.Data[1][index],
			'low'  : wsi.Data[2][index],
			'close': wsi.Data[3][index],
			'volume':wsi.Data[4][index],		
		})
	if len(W)==0:
		# print SliceMinute_WindPy.__name__,windcode,wsi
		return None
	return {
		'date':W[-1]['date'],
		'open':W[0 ]['open'],
		'high':max([ x['high'] for x in W ]),
		'low' :min([ x['low']  for x in W ]),
		'close':W[-1]['close'],
	}


#prepare database environment variable
def main():
	cnx = mysql.connector.connect(**config)
	cnx.set_converter_class(NumpyMySQLConverter)
	windcode = '399905.SZ'#
	MarketType = 1
	ktype_list = [2,4,5,6]
	SliceMinuteUpdate(windcode,ktype_list,cnx,MarketType,CalcTechicalAnalysis)
	'''
	windcode = '000001.SH'
	for ktype in [6,2,4,5] :
		indicator = SingleSliceMinute( windcode,ktype,cnx,MarketType)
		if indicator == None :
			continue
		CalcTechicalAnalysis(indicator,windcode,ktype,cnx,MarketType)
	'''	
	cnx.close()
	'''
	#closel = [ indicator[x]['close'] for x in range(0, len(indicator) ) ]
	#map(lamba x:print x['date'],x['close'],indicator)       
	#macd,macdsignal, macdhist = myMACD(pd.Series(closel)) 
	#ema12 = talib.EMA(numpy.array(closel,dtype= numpy.float),12) 
	#print ema12
	#print myema(closel,12) 

	macd,macdsignal, macdhist = talib.MACDEXT(numpy.array(closel,dtype= numpy.float),fastperiod = 12,fastmatype=1,slowperiod=26,slowmatype=1, signalperiod=9, signalmatype=1) 
	print 'dif',macd 
	print  'dea',macdsignal 
	print  'macd',macdhist*2
	'''
if __name__ == '__main__':
 	main()
