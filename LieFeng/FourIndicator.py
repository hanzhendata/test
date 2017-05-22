import  mysql.connector
# import  logging
import  numpy 
import  talib 
import	Slice
# logging.basicConfig(filename='logger.log',level=logging.INFO)
from Config import *

def  GoldenDead(windcode,MarketType,ktype):
	"""
	Set Golden Dead 
	"""
	cnx = mysql.connector.connect(**config)
	cursor = cnx.cursor()
	cursor_update = cnx.cursor()
	if (MarketType==0):
		tablename= "futures_history"
	else:
		tablename="stocks_history"
	select_sql = "select date,dea,diff,fast_k,fast_d from " + tablename + " where prod_code=%(code)s and ktype=%(ktype)s order by date"
	select_data = {
		'code':windcode,
		'ktype':ktype,
	}
	# logging.info(  "Golden or Dead Cross : code " + (windcode)+" ktype "+str(ktype) )
	cursor.execute(select_sql, select_data )
	
	indicator = []
	for (dt,dea,diff,k,d) in cursor:
		history = {
			'date' : dt,
			'dea': dea,
			'diff': diff,			
		}
		indicator.append(history)
	for index in range(1,len(indicator)):
		gdbb = 0
		before,current = indicator[index-1], indicator[index]
		if index != len(indicator) - 1  :
			after = indicator[index+1]
			if ( current['diff'] == current['dea'] ):
				if  before['diff']>before['dea'] and after['diff']<before['dea']  :
					gdbb = 1 #dead point first
				elif before['diff']<before['dea'] and after['diff']>before['dea'] :
					gdbb = 2 #golden point first
				continue
		if current['diff'] < current['dea'] :
			if before['diff']>before['dea'] :
				gdbb =3  #dead point second			
		elif current['diff'] > current['dea'] :
			if before['diff'] < before['dea'] :
				gdbb = 4  #golden point second
			
		Update_sql = " Update " + tablename + " Set gdbb= %(gdbb)s Where prod_code=%(code)s and ktype=%(ktype)s and date=%(tdate)s"
		Update_data = {
			'code'   : windcode,
			'ktype' :  ktype,
			'tdate'	: indicator[index]['date'],
			'gdbb'	: gdbb,
		}
		#print Update_sql
		cursor_update.execute(Update_sql,Update_data)
	cnx.commit()
	cursor_update.close()
	cursor.close()
	cnx.close()
def  BullBearLongShort(windcode,ktype,MarketType):
	"""
	Set (Bull or Bear) and (Long or Short) 
	"""
	cnx = mysql.connector.connect(**config)
	cursor = cnx.cursor()
	cursor_update = cnx.cursor()
	if (MarketType==0):
		tablename= "futures_history"
	else:
		tablename="stocks_history"
	select_sql = "select date,dea,diff,fast_k,fast_d,gdbb from " + tablename + " where prod_code=%(code)s and ktype=%(ktype)s order by date"
	select_data = {
		'code':windcode,
		'ktype':ktype,
	}
	# logging.info(  "(Bull or Bear) and (Long or Short) : code " + (windcode)+" ktype "+str(ktype) )
	cursor.execute(select_sql, select_data )
	
	
	for (dt,dea,diff,k,d,gdbb) in cursor:
		history = {
			'date' : dt,
			'dea': dea,
			'diff': diff,			
		}
		if gdbb is None :
			gdbb = 0		
		if ( diff >0 and dea >0 ):
			if  k>d   :
				gdbb += 10 # bull long
			if k<d :
				gdbb += 20 #  bull short
		if diff<0 and dea <0 :
			if k>d :
				gdbb += 30  #bear long		
			if k<d :
				gdbb += 40  #golden point second
			
		Update_sql = " Update " + tablename + " Set gdbb= %(gdbb)s Where prod_code=%(code)s and ktype=%(ktype)s and date=%(tdate)s"
		Update_data = {
			'code'   : windcode,
			'ktype' :  ktype,
			"tdate"	: indicator[index]['date'],
			'gdbb'	: gdbb,
		}
		#print Update_sql
		cursor_update.execute(Update_sql,Update_data)
	cnx.commit()
	cursor_update.close()
	cursor.close()
	cnx.close()
 
def RDFlag(d1,d2,h1,h2):
	return (d1>=d2 and h1>=h2) or ( d1<=d2 and h1<=h2) or (d1<=d2 and h1>=h2)
	
def  RallyDivergence(windcode,MarketType,ktype):
	"""
	Rally Divergence 
	"""
	cnx = mysql.connector.connect(**config)
	cursor = cnx.cursor()
	
	if (MarketType==0):
		tablename= "futures_history"
	else:
		tablename=  "stocks_history"
	select_sql = "select date, high_price,fast_j,slow_j,diff,dea,gdbb from " + tablename + " where prod_code=%(code)s and ktype=%(ktype)s order by date "
	select_data = {
		'code'	:windcode,
		'ktype'	:ktype,
	}
	# logging.info ( "RallyDivergence code "+str(windcode)+" ktype "+str(ktype) )
	cursor.execute(select_sql, select_data )
	
	indicator = []
	for (dt,high,fast_j,slow_j,diff,dea,gdbb) in cursor:
		current = {
			'date'	: dt,
			'high'	: high,
			'fast_j': fast_j,
			'slow_j': slow_j,
			'diff'	: diff,
		    'dea'   : dea,
		    'gdbb'	: gdbb,
		}		
		indicator.append(current)
	cursor.close()
	cnx.close()
	deadindex = []
	
	f = open('Rallylog.txt','w')
	for index in range(0, len( indicator ) ):
		gdbb= indicator[index]['gdbb']
		if gdbb == 1 or gdbb == 3 :
			deadindex.append(index)
		else :
			continue
		if len(deadindex)<2 :
			continue
		# logging.info ( indicator[index]['date'].strftime('%Y-%m-%d %H:%M:%S')+' Dead point '+ str(index) )
		# check between gap more than 6
		temp = index
		di_range = [  ]
		for di in range(len(deadindex)-2,-1,-1):
			if (temp-deadindex[di]) < 6 :
				continue
			else :
				di_range.insert(0,deadindex[di])
				temp = deadindex[di]
				if len( di_range )>=3 :
					break			
				
		if len( di_range ) < 2 :
			continue
		#di_range= deadindex[-4:-1]
		di_range.append(index)
		
		
		high = []
		highindex = []
		jx = []
		jl  = []
		for di in range(1,len(di_range)):	
			logging.info( indicator[ di_range[di-1] ]['date'].strftime('%Y-%m-%d %H:%M:%S') )
			jx = [ x for x in  range(di_range[di-1]+1,di_range[di]) if indicator[x]['gdbb']==2 or indicator[x]['gdbb']==4] 
			if len(jx) == 0 :
				high.append( max( [ indicator[x]['high'] for x in  range(di_range[di-1]+1,di_range[di])  ] ) )			
				highindex.append( filter(lambda x: indicator[x]['high']==high[-1] ,range(di_range[di-1]+1,di_range[di]) )[0] )
			else :
				high.append( max( [ indicator[x]['high'] for x in  range(jx[-1],di_range[di])  ] ) )			
				highindex.append( filter(lambda x: indicator[x]['high']==high[-1] ,range(jx[-1],di_range[di]) )[0] )				
			#jl .append ([ x   for x in range(di_range[di-1],di_range[di]) if indicator[x]['fast_j']<=0 or indicator[x]['slow_j']<=0 ])
		if (  len(di_range)<4 ) :			
			d3 = indicator[ di_range[-3]  ]['diff']
			d2 = indicator[ di_range[-2]  ]['diff']
			d1 = indicator[ di_range[-1]  ]['diff']
			h2,h1 = high[0],high[1]
			hi2,hi1 = highindex[0], highindex[1]
			# if d1<d2 and h2<h1 and hi1 - hi2 > 8:
			# 	logging.info( indicator[index]['date'].strftime('%Y-%m-%d %H:%M:%S') + "            3 point rally divergence\n")
		else :
			#sx=d
			d4 = indicator[ di_range[-4]  ]['diff']
			d3 = indicator[ di_range[-3]  ]['diff']
			d2 = indicator[ di_range[-2]  ]['diff']
			d1 = indicator[ di_range[-1]  ]['diff']
			h3,h2,h1 = high[-3],high[-2],high[-1]
			hi3,hi2,hi1 = highindex[-3], highindex[-2], highindex[-1]
			# logging.info( str(di_range[-4]) + " " + str(di_range[-3]) + " " +str(di_range[-2]) + " " + str(di_range[-1]) + " ")
			# logging.info( str (h3) + "  "+str(h2) + " " + str(h1) )
			# logging.info( str(d4) + " " + str(d3) + " " +str(d2) + " " + str(d1) + " ")			
			if d1<d2 and h2<h1 and hi1-hi2>8:
				# logging.info( indicator[index]['date'].strftime('%Y-%m-%d %H:%M:%S') + "             3 point rally divergence\n")
				f.write( indicator[index]['date'].strftime('%Y-%m-%d %H:%M:%S') + "             3 point rally divergence\n")
			#elif d1<d3 and h3<h1 and hi1-hi3>8:
			#	logging.info( indicator[index]['date'].strftime('%Y-%m-%d %H:%M:%S') + "             3 2 point rally divergence\n")
			#	f.write( indicator[index]['date'].strftime('%Y-%m-%d %H:%M:%S') + "             3 2 point rally divergence\n")
			if d1<d2 and d2<d3 :#and d3<d4
				if h1>h2 and h2>h3 :
					# logging.info( indicator[index]['date'].strftime('%Y-%m-%d %H:%M:%S') + "           2 rally divergence\n")
					f.write( indicator[index]['date'].strftime('%Y-%m-%d %H:%M:%S') + "           2 rally divergence\n")
				'''	
					if  len(jl[0])==0 and len(jl[1])==0 :
						if  len(jl[2])!=0:
							logging.info(indicator[index]['date'].strftime('%Y-%m-%d %H:%M:%S') +" 1 rally divergence  J0")
							#f.write( indicator[index]['date'].strftime('%Y-%m-%d %H:%M:%S') +" 1 rally divergence  J0")	
						else:
							logging.info( indicator[index]['date'].strftime('%Y-%m-%d %H:%M:%S') + " 2 rally divergence")
							#f.write( indicator[index]['date'].strftime('%Y-%m-%d %H:%M:%S') + " 2 rally divergence")
			
			elif   d1<d3 and h1>h3:
				flag12 = RDFlag(d1,d2,h1,h2)
				flag23 = RDFlag(d2,d3,h2,h3) 
				if flag12 and flag23 :
					logging.info( indicator[index]['date'].strftime('%Y-%m-%d %H:%M:%S') +" 1 rally divergence sepcial 1")
					#f.write( indicator[index]['date'].strftime('%Y-%m-%d %H:%M:%S') +" 1 rally divergence sepcial 1")	
		'''
	f.close()		

	
def  FallDivergence(windcode,MarketType,ktype):
	"""
	Fall Divergence 
	"""
	cnx = mysql.connector.connect(**config)
	cursor = cnx.cursor()
	
	if (MarketType==0):
		tablename= "futures_history"
	else:
		tablename=  "stocks_history"
	select_sql = "select date, high_price,fast_j,slow_j,diff,dea,gdbb from " + tablename + " where prod_code=%(code)s and ktype=%(ktype)s order by date "
	select_data = {
		'code'	:windcode,
		'ktype'	:ktype,
	}
	# logging.info ( "FallDivergence code "+str(windcode)+" ktype "+str(ktype) )
	cursor.execute(select_sql, select_data )
	
	indicator = []
	for (dt,high,fast_j,slow_j,diff,dea,gdbb) in cursor:
		current = {
			'date'	: dt,
			'high'	: high,
			'fast_j': fast_j,
			'slow_j': slow_j,
			'diff'	: diff,
		    'dea'   : dea,
		    'gdbb'	: gdbb,
		}		
		indicator.append(current)
	cursor.close()
	cnx.close()
	deadindex = []
	
	f = open('Rallylog.txt','w')
	for index in range(0, len( indicator ) ):
		gdbb= indicator[index]['gdbb']
		if gdbb == 1 or gdbb == 3 :
			deadindex.append(index)
		else :
			continue
		if len(deadindex)<2 :
			continue
		# logging.info ( indicator[index]['date'].strftime('%Y-%m-%d %H:%M:%S')+' Dead point '+ str(index) )
		# check between gap more than 6
		temp = index
		di_range = [  ]
		for di in range(len(deadindex)-2,-1,-1):
			if (temp-deadindex[di]) < 6 :
				continue
			else :
				di_range.insert(0,deadindex[di])
				temp = deadindex[di]
				if len( di_range )>=3 :
					break			
				
		if len( di_range ) < 2 :
			continue
		#di_range= deadindex[-4:-1]
		di_range.append(index)
		
		
		high = []
		highindex = []
		jx = []
		jl  = []
		for di in range(1,len(di_range)):	
			# logging.info( indicator[ di_range[di-1] ]['date'].strftime('%Y-%m-%d %H:%M:%S') )
			jx = [ x for x in  range(di_range[di-1]+1,di_range[di]) if indicator[x]['gdbb']==2 or indicator[x]['gdbb']==4] 
			if len(jx) == 0 :
				high.append( max( [ indicator[x]['high'] for x in  range(di_range[di-1]+1,di_range[di])  ] ) )			
				highindex.append( filter(lambda x: indicator[x]['high']==high[-1] ,range(di_range[di-1]+1,di_range[di]) )[0] )
			else :
				high.append( max( [ indicator[x]['high'] for x in  range(jx[-1],di_range[di])  ] ) )			
				highindex.append( filter(lambda x: indicator[x]['high']==high[-1] ,range(jx[-1],di_range[di]) )[0] )				
			#jl .append ([ x   for x in range(di_range[di-1],di_range[di]) if indicator[x]['fast_j']<=0 or indicator[x]['slow_j']<=0 ])
		if (  len(di_range)<4 ) :			
			d3 = indicator[ di_range[-3]  ]['diff']
			d2 = indicator[ di_range[-2]  ]['diff']
			d1 = indicator[ di_range[-1]  ]['diff']
			h2,h1 = high[0],high[1]
			hi2,hi1 = highindex[0], highindex[1]
			# if d1<d2 and h2<h1 and hi1 - hi2 > 8:
			# 	logging.info( indicator[index]['date'].strftime('%Y-%m-%d %H:%M:%S') + "            3 point rally divergence\n")
		else :
			#sx=d
			d4 = indicator[ di_range[-4]  ]['diff']
			d3 = indicator[ di_range[-3]  ]['diff']
			d2 = indicator[ di_range[-2]  ]['diff']
			d1 = indicator[ di_range[-1]  ]['diff']
			h3,h2,h1 = high[-3],high[-2],high[-1]
			hi3,hi2,hi1 = highindex[-3], highindex[-2], highindex[-1]
			# logging.info( str(di_range[-4]) + " " + str(di_range[-3]) + " " +str(di_range[-2]) + " " + str(di_range[-1]) + " ")
			# logging.info( str (h3) + "  "+str(h2) + " " + str(h1) )
			# logging.info( str(d4) + " " + str(d3) + " " +str(d2) + " " + str(d1) + " ")			
			if d1<d2 and h2<h1 and hi1-hi2>8:
				# logging.info( indicator[index]['date'].strftime('%Y-%m-%d %H:%M:%S') + "             3 point rally divergence\n")
				f.write( indicator[index]['date'].strftime('%Y-%m-%d %H:%M:%S') + "             3 point rally divergence\n")
			#elif d1<d3 and h3<h1 and hi1-hi3>8:
			#	logging.info( indicator[index]['date'].strftime('%Y-%m-%d %H:%M:%S') + "             3 2 point rally divergence\n")
			#	f.write( indicator[index]['date'].strftime('%Y-%m-%d %H:%M:%S') + "             3 2 point rally divergence\n")
			if d1<d2 and d2<d3 :#and d3<d4
				if h1>h2 and h2>h3 :
					# logging.info( indicator[index]['date'].strftime('%Y-%m-%d %H:%M:%S') + "           2 rally divergence\n")
					f.write( indicator[index]['date'].strftime('%Y-%m-%d %H:%M:%S') + "           2 rally divergence\n")
				'''	
					if  len(jl[0])==0 and len(jl[1])==0 :
						if  len(jl[2])!=0:
							logging.info(indicator[index]['date'].strftime('%Y-%m-%d %H:%M:%S') +" 1 rally divergence  J0")
							#f.write( indicator[index]['date'].strftime('%Y-%m-%d %H:%M:%S') +" 1 rally divergence  J0")	
						else:
							logging.info( indicator[index]['date'].strftime('%Y-%m-%d %H:%M:%S') + " 2 rally divergence")
							#f.write( indicator[index]['date'].strftime('%Y-%m-%d %H:%M:%S') + " 2 rally divergence")
			
			elif   d1<d3 and h1>h3:
				flag12 = RDFlag(d1,d2,h1,h2)
				flag23 = RDFlag(d2,d3,h2,h3) 
				if flag12 and flag23 :
					logging.info( indicator[index]['date'].strftime('%Y-%m-%d %H:%M:%S') +" 1 rally divergence sepcial 1")
					#f.write( indicator[index]['date'].strftime('%Y-%m-%d %H:%M:%S') +" 1 rally divergence sepcial 1")	
		'''
	f.close()		

def RallyFall(date,ktype):
	return	True
		
def  RallyBreakCentral(windcode,MarketType,ktype):
	"""
	Fall Divergence 
	"""
	cnx = mysql.connector.connect(**config)
	cursor = cnx.cursor()
	
	if (MarketType==0):
		tablename= "futures_history"
	else:
		tablename=  "stocks_history"
	select_sql = "select date, close_price,open_price from " + tablename + " where prod_code=%(code)s and ktype=%(ktype)s order by date "
	select_data = {
		'code'	:windcode,
		'ktype'	:ktype,
	}
	# logging.info ( "RallyDivergence code "+str(windcode)+" ktype "+str(ktype) )
	cursor.execute(select_sql, select_data )
	
	indicator = []
	for (dt,close,mopen) in cursor:
		current = {
			'date'	: dt,
			'close'	: close,
			'open'	: mopen,
			'max'	: max(close,mopen),
		}		
		indicator.append(current)
	cursor.close()
	cnx.close()
	maxindex = []
	minindex = []
	
	for index in range(1, len( indicator )-1 ):		
		before,current,after = indicator[index-1]['max'], indicator[index]['max'], indicator[index+1]['max']
		if  before <current and  current> after :
			maxindex.append(index)
		if  before > current and current < after :
			minindex.append(index)
	if len(minindex)<2 or len(maxindex)<2 :
		# logging.info("wave max or min less 2")
		return
	f = open('fallbreak.txt','w')
	for index in range(1, len( indicator )-1 ):				
		if ( index in maxindex or index in minindex ):
			continue		
		before,current,after = indicator[index-1]['max'], indicator[index]['max'], indicator[index+1]['max']
		if  current<  after :
			continue
		# logging.info ( indicator[index]['date'].strftime('%Y-%m-%d %H:%M:%S')+'  '+ str(index) )
		lowindex  = [ x for x  in minindex if x<index and indicator[x]['max']< current ]
		if len(lowindex) <2 : 
			 continue
		lowmax =max( [ indicator[x]['max'] for x in lowindex  ] )
		highindex = [ x for x in  maxindex if x<index  and indicator[x]['max']> lowmax ]
		
		if  len(highindex): 
			 continue
		print lowindex,highindex,index
		if current>before and after > current : 
			# logging.info(str(index))
			high = [ indicator[x]['max'] for x in highindex  ]
			h1 = max(high)			
			h2 = max([ indicator[x]['max'] for x in highindex if indicator[x]['max'] <> h1 ] )
			if (current *2)>=(h1+h2) and RallyFall(indicator[index]['date'],2) : 
				f.write( indicator[index]['date'].strftime('%Y-%m-%d %H:%M:%S') +" 1 rally breaking \n")	
	
	f.close()		

def Golden(DL,beforeIndex,currentIndex,Param1,Param2,afterIndex=None):
	if DL[ Param1][currentIndex] == DL [Param2] [currentIndex]:
		if afterIndex is None:
			return False
		if DL[Param1][beforeIndex] < DL[Param2][beforeIndex] and DL[Param1][afterIndex] > DL[Param2][afterIndex]:
			return True
	if DL[Param1][currentIndex] > DL[Param2][currentIndex] and DL[Param1][beforeIndex] < DL[Param2][beforeIndex]:
		return True
	return False

def EMA_Iterate(Yn,Close,N):	 
	k = 2.0/(N+1)	
	return ( k * (Close) + (1-k)*Yn ) 

def MACD(Close,fast_period=12, slow_period=26, signal_period=9):
	if len(Close)==0 :
		return None
	ema12 = talib.EMA(Close,fast_period)
	ema26 = talib.EMA(Close,slow_period)
	dif = ema12-ema26
	dea = talib.EMA(numpy.array(dif,dtype= numpy.float),signal_period)
	bar = (dif-dea) * 2 
	macd =  {
		'EMA_FAST':ema12,
		'EMA_SLOW':ema26,
		'DIFF' :dif,
		'DEA'  :dea,
		'MACD' :bar}
	del ema12,ema26,dif,dea,bar
	return macd

def MACD_Default(Close,fast_period=12, slow_period=26, signal_period=9,default=0):
	ema12 = [Close[0]]
	ema26 = [Close[0]]
	dif = [0]
	dea = [default]
	bar = [(0-default)*2]
	for index in range(len(Close[1:])):		
		ema12.append( EMA_Iterate ( ema12[-1],Close[index+1],fast_period) )
		ema26.append( EMA_Iterate ( ema26[-1],Close[index+1],slow_period) )
		dif.append  ( ema12[-1] - ema26[-1]   )
		dea.append  ( EMA_Iterate ( dea[-1],dif[-1],signal_period) )
		bar.append  ( (dif[-1]-dea[-1])*2)
	return {
		'EMA_FAST':ema12,
		'EMA_SLOW':ema26,
		'DIFF' :dif,
		'DEA'  :dea,
		'MACD' :bar}

#fast_fastk_period=9, fast_slowk_period=3, fast_fastd_period =3,
#slow_fastk_period= 21, slow_slowk_period=9, slow_fastd_period=9,
def KDJ(High,Low,Close, fastk_period=9,  slowk_period=3,  fastd_period =3):
	if len(Close)==0 :
		return None
	k, d, j=Slice.MyKDJ(High,Low,Close, fastk_period, slowk_period,  fastd_period)
	kdj = {
		'K':k,
		'D':d,
		'J':j,
	}
	del k,d,j
	return kdj

def KDJ_Default(High,Low,Close, fastk_period=9,  slowk_period=3,  fastd_period =3,  default=50):
	length = len(Close)
	if length ==0 :
		return None
	if length < fastk_period :
		return {
			'K':numpy.array([default] * length),
			'D':numpy.array([default] * length),
			'J':numpy.array([default] * length),
		}

	k= [default] * (fastk_period-1)
	d= [default] * (fastk_period-1) 
	j= [default] * (fastk_period-1)
	sr = 1.0/slowk_period
	fr = 1.0/fastd_period
	for index in range(length-fastk_period+1):
		high = max( High[index:index+fastk_period] )
		low  = min( Low [index:index+fastk_period] )
		if high == low :
			rsv =  default
		else:
			rsv  = (Close[index+fastk_period-1] -low )/(high-low) * 100

		k.append( k[-1]*(1-sr) + rsv*sr  )
		d.append( d[-1]*(1-fr) + k[-1]*fr)
		j.append( 3 *k[-1] - 2*d[-1]     )
		
	return {
		'K':k,
		'D':d,
		'J':j,
	}
	
	
def BBANDS(Close , time_period=26,  nbdev_up=1,  nbdev_dn =1):	
	b1, b2, b3 = talib.BBANDS(Close, timeperiod=time_period, nbdevup=nbdev_up, nbdevdn=nbdev_dn, matype=0) 
	b = {
		'B1':b1,
		'B2':b2,
		'B3':b3,
	}
	del b1,b2,b3
	return b
	
def MA(Close,time_period):
	return talib.SMA(Close,timeperiod=time_period)

def PB(Close,param_list=[4,6,9,13,18,24]):
		
	rpb = {}
	j = 0
	for i in param_list:
		emapb = talib.EMA(Close,i)
		ma1 = talib.SMA(Close, timeperiod=2*i)
		ma2 = talib.SMA(Close, timeperiod=4*i)
		rpb [ 'EMA'+str(i) ] = emapb
		rpb [ 'PB'+str(j+1)] = (emapb+ma1+ma2)/3 
		j = j + 1
	del emapb,ma1,ma2
	return rpb

def  GoldenDead(indicator,index=-1):
	"""
	Calc Golden Dead 
	"""
	
	
	gdbb = 0

	if  len(indicator['diff']) < 2 :
		return gdbb	
	
				
	if indicator['diff'][index] < indicator['dea'][index] :
		if indicator['diff'][index-1]>indicator['dea'][index-1] :
			gdbb =3  #dead point second			
	elif indicator['dea'][index]<0 and indicator['diff'][index] >= indicator['dea'][index] :
		if indicator['diff'][index-1] < indicator['dea'][index-1] :
			gdbb = 4  # under 0 axis golden point second
			
	return gdbb	
	
def CalcTechicalAnalysis(indicator,fastperiod=12, slowperiod=26, signalperiod=9,
	    fast_fastk_period=9, fast_slowk_period=3, fast_fastd_period =3,
	    slow_fastk_period= 21, slow_slowk_period=9, slow_fastd_period=9,
	    bbands_time_perion=26, bbands_nbdev_up=1, bbands_nbdev_dn=1,pb_param_list=[4,6,9,13,18,24]
	    ):
	if len(indicator)==0 :
		return None
	high = numpy.array([ indicator[x]['high' ] for x in range(0, len(indicator) ) ], dtype = numpy.float)
	low  = numpy.array([ indicator[x]['low'  ] for x in range(0, len(indicator) ) ], dtype = numpy.float)
	close =numpy.array([ indicator[x]['close'] for x in range(0, len(indicator) ) ], dtype = numpy.float)
	ta = MACD(close,fast_period=fastperiod,slow_period=slowperiod,signal_period=signalperiod)
	kdj = KDJ(high,low,close,fastk_period = fast_fastk_period,slowk_period= fast_slowk_period,fastd_period= fast_fastd_period) 
	ta['FAST_K'] = kdj['K']
	ta['FAST_D'] = kdj['D']
	ta['FAST_J'] = kdj['J']
	kdj = KDJ(high,low,close,fastk_period = slow_fastk_period,slowk_period= slow_slowk_period,fastd_period= slow_fastd_period) 
	ta['SLOW_K'] = kdj['K']
	ta['SLOW_D'] = kdj['D']
	ta['SLOW_J'] = kdj['J']
	ta['MA5'] =  MA(close,5)
	ta['MA60'] = MA(close,60)
	ta.update( BBANDS(close,time_period=bbands_time_perion,nbdev_up=bbands_nbdev_up,nbdev_dn=bbands_nbdev_dn) )
	ta.update( PB(close,param_list= pb_param_list) )
	del high,low,close,kdj
	return ta

def LongShort(indicator,index=-1,KDJFast=True):
	short = 0 
	if len(indicator) <1 :
		return short
	# if ( indicator['diff'][-1]>0 and indicator['dea'][-1]>0 ) or \
	# 	( indicator['diff'][-1]<0 and indicator['dea'][-1]<0 ) :
	if KDJFast :
		if indicator['fast_k'][index] < indicator['fast_d'][index] :
			short = 1
		else:
			short = 2
	else:
		if indicator['slow_k'][index] < indicator['slow_d'][index] :
			short = 1
		else:
			short = 2
	return short

def  RallyDivergence(indicator,ohlc):
	"""
	Rally Divergence 
	"""

	deadindex = []
	rd = 0
	gdbb= GoldenDead(indicator)
	if gdbb !=  3 :
		return rd
	ilength = len(indicator)
	deadindex.insert(0,ilength-1)
	for index in range(ilength-2,-1,-1 ):
		gdbb= GoldenDead(indicator,index=index)
		if gdbb == 3 :
			deadindex.insert(0,index)
		else :
			continue
	if len(deadindex)<2 :
		return rd
	index = ilength -1
	temp  = index
	di_range = [  ]
	for di in range(len(deadindex)-2,-1,-1):
		if (temp-deadindex[di]) < 6 :
			continue
		else :
			di_range.insert(0,deadindex[di])
			temp = deadindex[di]
			if len( di_range )>=3 :
				break			
			
	if len( di_range ) < 2 :
		return rd
	#di_range= deadindex[-4:-1]
	di_range.append(index)
		
		
	high = []
	highindex = []
	jx = []
	jl  = []
	for di in range(1,len(di_range)):
		jx = []	
		for x in  range(di_range[di-1]+1,di_range[di])  :
			if GoldenDead( indicator[0:x+1] ) == 4 :
				jx.append(x) 
		if len(jx) == 0 :
			high.append( max( [ ohlc[x]['high'] for x in  range(di_range[di-1]+1,di_range[di])  ] ) )			
			highindex.append( filter(lambda x: ohlc[x]['high']==high[-1] ,range(di_range[di-1]+1,di_range[di]) )[0] )
		else :
			high.append( max( [ ohlc[x]['high'] for x in  range(jx[-1],di_range[di])  ] ) )			
			highindex.append( filter(lambda x: ohlc[x]['high']==high[-1] ,range(jx[-1],di_range[di]) )[0] )		
		
	if (  len(di_range)<4 ) :			
		d3 = indicator['diff'][ di_range[-3]  ]
		d2 = indicator['diff'][ di_range[-2]  ]
		d1 = indicator['diff'][ di_range[-1]  ]
		h2,h1 = high[0],high[1]
		hi2,hi1 = highindex[0], highindex[1]
		if d1<d2 and h2<h1 and hi1-hi2>8:				
			rd = 2		
	else :
		#sx=d
		d4 = indicator['diff'][ di_range[-4]  ]
		d3 = indicator['diff'][ di_range[-3]  ]
		d2 = indicator['diff'][ di_range[-2]  ]
		d1 = indicator['diff'][ di_range[-1]  ]
		h3,h2,h1 = high[-3],high[-2],high[-1]
		hi3,hi2,hi1 = highindex[-3], highindex[-2], highindex[-1]

		if d1<d2 and h2<h1 and hi1-hi2>8:				
			rd = 2
		#elif d1<d3 and h3<h1 and hi1-hi3>8:
		#	logging.info( indicator[index]['date'].strftime('%Y-%m-%d %H:%M:%S') + "             3 2 point rally divergence\n")
		#	f.write( indicator[index]['date'].strftime('%Y-%m-%d %H:%M:%S') + "             3 2 point rally divergence\n")
		if d1<d2 and d2<d3 :#and d3<d4
			if h1>h2 and h2>h3 :
				rd=2				
				if  len(jl[0])==0 and len(jl[1])==0 :
					if  len(jl[2])!=0:
						#" 1 rally divergence  J0")
						rd = 1	
					else:
						rd = 2
		
		elif   d1<d3 and h1>h3:
			flag12 = RDFlag(d1,d2,h1,h2)
			flag23 = RDFlag(d2,d3,h2,h3) 
			if flag12 and flag23 :
				rd = 1
		
	return rd	

def main():
	windcode = 	"399905.SZ"#"000001.SH"
	#for i in [2,5,6,8]:
		#GoldenDead(windcode,i,1)
	GoldenDead(windcode,1,6)
	BullBearLongShort(windcode,1,6)
	RallyDivergence(windcode,1,6)
	#FallDivergence   (windcode,1,6)
	#RallyBreakCentral(windcode,1,6)	


if __name__ == '__main__':
	main()	

