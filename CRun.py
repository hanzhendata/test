from LieFeng import Config,Slice,FourIndicator,Stock
from WindPy import w
from datetime import time,datetime, timedelta
import  mysql.connector
from multiprocessing import Pool

def BoardListUpdate(FileName,Connector,WindPy):
	cursor = Connector.cursor()    
	sql = " replace into stocks_board(sectorid,name,type,windcode,update_date) values(%(id)s,%(name)s,%(type)s,%(windcode)s,%(update_date)s)"
	f = open(FileName,'r')
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
		Stock.StocksBoardList(sid,update_date,Connector,WindPy)
	cnx.commit()
	cursor.close()
	f.close()
# @Config.timer
def Update(Connect,WindPy):
	datestr = datetime.now().strftime("%Y-%m-%d")
	#set windcode
	boardcode_set = set()
	windcode_set  = set()
	
	MarketType = 1
	sql = "select  name, windcode,sectorid from stocks_board"	
	# sql_list = "select name,windcode from stocks_boardlist where sectorid=%(sectorid)s"

	cursor = Connect.cursor()
	# cursor_l = Connect.cursor()
	cursor.execute(sql)
	for bname,windcode,sectorid in cursor:		
		boardcode_set.add(windcode)
		# cursor_l.execute(sql_list,{'sectorid':sectorid})
		# for lname,lwindcode in cursor_l:
		# 	windcode_set.add(lwindcode)		
	# cursor_l.close()
	wsdata=WindPy.wset("sectorconstituent","date="+datestr+";sectorid=a001010100000000")
	if not Config.CheckWindData(wsdata):
		return
	cursor.execute("delete from stocks_code")
	sql = "Insert into stocks_code(windcode,name,date) values(%(code)s,%(name)s,%(date)s)"
	for index in range(0,len(wsdata.Data[0])):
		windcode_set.add( wsdata.Data[1][index] )
		sdate = wsdata.Data[0][index]
		da = {
				'code' : wsdata.Data[1][index],
				'name' : wsdata.Data[2][index],
				'date' : sdate,
			}
		cursor.execute(sql,da)
	cnx.commit()
	#update 
	# for ktype in [7,8]:
	# 	Stock.StocksHistoryUpdate(boardcode_set ,ktype,Connect,WindPy,base_days=365*30,minflag=True)
	# 	Stock.StocksHistoryUpdate(windcode_set  ,ktype,Connect,WindPy,base_days=365*30,minflag=True)	
	# Stock.StocksHistoryUpdate(windcode_set ,0,Connect,WindPy,base_days=365*3 ,minflag=True)
	
	#slice and techincal analysis
	# ktype_list = [2,4,5,6]
	# for windcode in windcode_set :
	# 	Slice.SliceMinuteUpdate(windcode,MarketType,ktype_list,Connect,None)
	# ktype_list= [2,4,5,6,7,8]
	ktype_list= [7,8]
	wb_set = windcode_set | boardcode_set
	for ktype in ktype_list :
		for windcode in wb_set :
			indicator = Stock.StocksOHLC(windcode,MarketType,ktype,Connect)
			if  len(indicator)<52 :
				continue			
			print len(indicator),ktype,windcode
			ta = FourIndicator.CalcTechicalAnalysis(indicator)
			Config.UpdateTechicalAnalysis_LoadDataInfile(windcode,MarketType,ktype,Connect,indicator,ta)
			# Slice.CalcTechicalAnalysis(indicator,windcode,MarketType,ktype,Connect,UpdateFlag=True)
			# FourIndicator.GoldenDead(windcode,MarketType,ktype)
			# FourIndicator.BullBearLongShort(windcode,MarketType,ktype)
	
	print "Start stocks board calc-------------------------"
	#update stocks board  strong and ready
	sql = "delete from stocks_board_sr"
	cursor = Connect.cursor()
	cursor.execute(sql)
	sql = "delete from stocks_boardlist_sr"
	cursor.execute(sql)	
	# prepare the stocks recent data only ktype 8 currently	
	board8 = Stock.StocksBoardData(8,1,Connect,WindPy)
	# print board8	
	stock8 = Stock.StocksData(windcode_set,8,1,Connect,WindPy)
	
	suspend5 = Stock.GetSuspendStockSet(datetime.now(),-5,WindPy)
	# do calculator
	Stock.StocksBoardCalc(board8)
	stocksBoardStrong=[]
	stocksBoardReady=[]
	for board in board8:
		if board['isStrong']:
			stocksBoardStrong.append(board)	
			continue
		if board['isReady'] :
			stocksBoardReady.append(board)
			continue
	stocksBoardStrong = Stock.StocksSortVolume(stocksBoardStrong,WindPy)
	stocksBoardReady  = Stock.StocksSortVolume(stocksBoardReady ,WindPy)

	print len(stocksBoardStrong) , len(stocksBoardReady)
	for i in range(0,len(stocksBoardStrong)):
		sql = " insert into stocks_board_sr(id,sectorid,name,type,windcode,update_date,isStrong,isReady) values(%(id)s,%(sectorid)s,%(name)s,%(type)s,%(windcode)s,%(update_date)s,%(isStrong)s,%(isReady)s)"
		data ={
			'id'	:  i ,
			'sectorid':stocksBoardStrong[i]['sectorid'] ,
			'name':stocksBoardStrong[i]['name'] ,
			'windcode':stocksBoardStrong[i]['windcode'],
			'type': stocksBoardStrong[i]['type'],
			'update_date' : stocksBoardStrong[i]['update_date'],
			'isStrong': True,
			'isReady': False 
		}
		cursor.execute(sql , data)
		stocklist = Stock.StocksBoardStockData(data['sectorid'],Connect)
		stocklist -= suspend5		
		stocklist =Stock.StocksData(stocklist,8,1,Connect,WindPy,Sectorid=data['sectorid'])
		stocklist =Stock.StocksBoardStockCalc(stocklist)
		stocksBoardStrong[i]['stocklist'] = Stock.StocksSortPctchg(stocklist,-5,0.2,0.1,WindPy)
		for j in range(0,len(stocksBoardStrong[i]['stocklist'])):			
			stock = stocksBoardStrong[i]['stocklist'][j]
			sql = "Insert into stocks_boardlist_sr(id,sectorid,windcode,name,isStrong,isReady) Values(%(id)s,%(sectorid)s,%(windcode)s,%(name)s,%(isStrong)s,%(isReady)s)"
			data = {
				'id': j,
				'sectorid':stocksBoardStrong[i]['sectorid'],
				'windcode':stock['windcode'],
				'name': 'stockinboard',#stock['name'],
				'isStrong': True,
				'isReady': False ,
			}
			cursor.execute(sql , data)
		Connect.commit()
	for i in range(0,len(stocksBoardReady)):
		sql = " insert into stocks_board_sr(id,sectorid,name,type,windcode,update_date,isStrong,isReady) values(%(id)s,%(sectorid)s,%(name)s,%(type)s,%(windcode)s,%(update_date)s,%(isStrong)s,%(isReady)s)"
		data ={
			'id'	:  i ,
			'sectorid':stocksBoardStrong[i]['sectorid'] ,
			'name':stocksBoardStrong[i]['name'] ,
			'windcode':stocksBoardStrong[i]['windcode'],
			'type': stocksBoardStrong[i]['type'],
			'update_date' : stocksBoardStrong[i]['update_date'],
			'isStrong': False,
			'isReady': True 
		}
		cursor.execute(sql , data)
		stocklist = Stock.StocksBoardStockData(data['sectorid'],Connect)
		stocklist -= suspend5
		stocklist =Stock.StocksData(stocklist,8,1,Connect,WindPy,Sectorid=data['sectorid'])
		stocklist =Stock.StocksBoardStockCalc(stocklist)
		stocksBoardStrong[i]['stocklist'] = Stock.StocksSortPctchg(stocklist,-5,0.2,0.1,WindPy)
		for j in range(0,len(stocksBoardStrong[i]['stocklist'])):
			stock = stocksBoardStrong[i]['stocklist'][j]
			sql = "Insert into stocks_boardlist_sr(id,sectorid,windcode,name,isStrong,isReady) Values(%(id)s,%(sectorid)s,%(windcode)s,%(name)s,%(isStrong)s,%(isReady)s)"
			data = {
				'id': j,
				'sectorid':stocksBoardStrong[i]['sectorid'],
				'windcode':stock['windcode'],
				'name': stock['name'],
				'isStrong': False,
				'isReady':  True,
			}
			cursor.execute(sql , data)
		Connect.commit()	 
	
	print "Start stocks calc-------------------------"
	cursor.execute("delete from stocks_sr")
	windcode_list = []
	for stock in stock8:
		if stock['windcode'] in suspend5:
			continue
		windcode_list.append(stock)	
	Stock.StocksCalc(windcode_list)
	stocksStrong=[]
	stocksReady=[]
	for board in windcode_list:
		if board['isStrong']:
			stocksStrong.append(board)	
			continue
		if board['isReady'] :
			stocksReady.append(board)
			continue
	stocksStrong = Stock.StocksSortPctchg(stocksStrong,-5,0.2,0   ,WindPy)
	for i in range(0,len(stocksStrong)):
		sql = "Insert into stocks_sr(id,stype,ktype,windcode,name) values(%(id)s,%(stype)s,%(ktype)s,%(windcode)s,%(name)s)"
		
		data ={
			'id'	:   i,
			'stype' :   0,
			'ktype' :	8,
			'name':     stocksStrong[i]['name'] ,
			'windcode': stocksStrong[i]['windcode'],
		}
		cursor.execute(sql , data)
	Connect.commit()
	stocksReady  = Stock.StocksSortPctchg(stocksReady ,-5,0.2,-0.1,WindPy)  
	for i in range(0,len(stocksReady)):
		sql = "Insert into stocks_sr(id,stype,ktype,windcode,name) values(%(id)s,%(stype)s,%(ktype)s,%(windcode)s,%(name)s)"
		
		data ={
			'id'	:   i,
			'stype' :   1,
			'ktype' :	8,
			'name':     stocksReady[i]['name'] ,
			'windcode': stocksReady[i]['windcode'],
		}
		cursor.execute(sql , data)
	Connect.commit()
	cursor.close()

def BoardInit():
	cnx = mysql.connector.connect(**Config.config)
	Stock.StocksBoardImport('E:/Outsourcing/LieFeng/requirement/board.txt',cnx)#"E:/Python_LieFeng/board.txt"
	cnx.close()

def BoardSave(StocksBoard,Connect,WindPy,suspend5,stock8,isStrong,isReady,FileName):
	cursor = Connect.cursor()
	f = open(FileName,'w')
	for i in range(0,len(StocksBoard)):
		sql = " insert into stocks_board_sr(id,sectorid,name,type,windcode,update_date,isStrong,isReady,pct_chg,cap) values(%(id)s,%(sectorid)s,%(name)s,%(type)s,%(windcode)s,%(update_date)s,%(isStrong)s,%(isReady)s,%(pct_chg)s,%(cap)s)"
		data ={
			'id'	:  i ,
			'sectorid':StocksBoard[i]['sectorid'] ,
			'name':StocksBoard[i]['name'] ,
			'windcode':StocksBoard[i]['windcode'],
			'type': StocksBoard[i]['type'],
			'update_date' : StocksBoard[i]['update_date'],
			'pct_chg' : StocksBoard[i]['pct_chg'],
			'cap' : StocksBoard[i]['cap'],
			'isStrong': isStrong,
			'isReady': isReady 
		}
		f.write(("%d %s %s %s Strong:%d \n" %(i,data['windcode'],data['name'],data['type'],isStrong)).encode('utf-8'))
		cursor.execute(sql , data)
		stocklist = Stock.StocksBoardStockData(data['sectorid'],Connect)
		# print len(stocklist)
		stocklist -= suspend5
		sl = []
		for wd in  stock8  :
			# print wd['windcode']
			if wd['windcode']	in stocklist:
			 	if  'sectorid' in wd :
			 		wd['sectorid'] = "%s;%s" % (wd['sectorid'],data['sectorid'])
			 	else :
			 		wd['sectorid'] = data['sectorid']
			 	sl.append(wd)	
		# print len(sl),len(stocklist)
		stocklist =Stock.StocksBoardStockCalc(sl)
		# print len(sl),len(stocklist)
		if len(stocklist) == 0 :
			Connect.commit()
			continue
		StocksBoard[i]['stocklist'] = Stock.StocksSortPctchg(stocklist,-5,0.2,0.1,WindPy)
		for j in range(0,len(StocksBoard[i]['stocklist'])):			
			stock = StocksBoard[i]['stocklist'][j]
			
			sql = "Insert into stocks_boardlist_sr(id,sectorid,windcode,name,isStrong,isReady,pct_chg,cap) Values(%(id)s,%(sectorid)s,%(windcode)s,%(name)s,%(isStrong)s,%(isReady)s,%(pct_chg)s,%(cap)s)"
			data = {
				'id': j,
				'sectorid':data['sectorid'],
				'windcode':stock['windcode'],
				'name':   stock['name'],
				'pct_chg' : stock['pct_chg'],
				'cap' : stock['cap'],
				'isStrong': isStrong,
				'isReady': isReady ,
			}
			cursor.execute(sql , data)
			f.write( ("%d %s %s Strong:%d \n" %(j,data['windcode'],data['name'],isStrong) ).encode('utf-8') )
				
		Connect.commit()
	f.close()

def StockSave(Stocks,CalcDate,tablename,stype,ktype,Connect):
	sql = "Replace into "+tablename+"(id,stype,ktype,date,windcode,name,pct_chg,cap) values(%(id)s,%(stype)s,%(ktype)s,%(date)s,%(windcode)s,%(name)s,%(pct_chg)s,%(cap)s)"
	cursor = Connect.cursor()
	for i in range(0,len(Stocks)):		
		stock = Stocks[i]
		data ={
			'id'	:   i,
			'stype' :   stype,
			'ktype' :	ktype,
			'date'  :   CalcDate,
			'name':     stock['name'] ,
			'pct_chg' : stock['pct_chg'],
			'cap' :		stock['cap'],
			'windcode': stock['windcode'],
		}
		cursor.execute(sql , data)
	Connect.commit()
	cursor.close()

def IndexSave(DataList,typename,DateList,Connect):
	assert(len(DataList) == len(DateList))
	sql = "Replace into stocks_index(date,typename,num) values(%(date)s,%(name)s,%(num)s)"
	cursor = Connect.cursor()
	dl = len(DateList)
	for index in range( dl ):
		data= {
			'date': DateList[index],
			'name': typename,
			'num' : DataList[dl-index-1],
		}
		cursor.execute(sql,data)
	Connect.commit()
	cursor.close()

def SectorConstituent(DateStr,Sectorid,Message,WindPy):
	wsdata=WindPy.wset("sectorconstituent","date="+DateStr+";sectorid="+Sectorid)
	if not Config.CheckWindData(wsdata,Message=Message):
		return None
	return wsdata

def DaysCalc(Type,Connect,WindPy):
	global board8
	global stock8
	current_date = datetime.now()	
	datestr = current_date.strftime("%Y-%m-%d")
	
	boardcode_dict = {}	
	windcode_dict  = {}


	MarketType = 1
	sql = "select  name, windcode,sectorid from stocks_board where windcode like '886%' " #only hangye board 2017-05-06	
	
	cursor = Connect.cursor()
	
	cursor.execute(sql)
	for bname,windcode,sectorid in cursor:		
		boardcode_dict [windcode] = {'name':bname,}
		
	wsdata=SectorConstituent(datestr,"a001010100000000","All A stocks code",WindPy)	
	starter=SectorConstituent(datestr,"a001010r00000000","All A stocks code",WindPy)
	chinastock500=SectorConstituent(datestr,"1000008491000000","Zhongzhen500 stocks code",WindPy)
	hushen300=SectorConstituent(datestr,"1000000090000000","Hushen300 stocks code",WindPy)
	if wsdata is None or starter is None or chinastock500 is None or hushen300 is None:
		return
	cursor.execute("delete from stocks_code")
	wd_ipo = ""
	sql = "Replace into stocks_code(windcode,name,status,starter,zhongzheng500,hushen300,date) values(%(code)s,%(name)s,%(status)s,%(starter)s,%(zhongzheng500)s,%(hushen300)s,%(date)s)"
	sql_bo = "select content from stocks_basic_other where windcode=%(code)s and typename='IPO_DATE' order by date desc limit 1"
	for index in range(0,len(wsdata.Data[0])):		
		sdate = wsdata.Data[0][index]
		da = {
				'code' : wsdata.Data[1][index],
				'name' : wsdata.Data[2][index],
				'date' : sdate,
				'starter' : wsdata.Data[1][index] in starter.Data[1],
				'zhongzheng500' : wsdata.Data[1][index] in chinastock500.Data[1],
				'hushen300' : wsdata.Data[1][index] in hushen300.Data[1],
				'status': 0,
			}
		cursor.execute(sql,da)
		
		cursor.execute(sql_bo,da)
		if cursor.rowcount != 0 :
			ipodate =  cursor.fetchone()[0]	
			# print ipodate		
			if (current_date-datetime.strptime(ipodate , "%Y-%m-%d %H:%M:%S.%f")).days<180:
				da['status'] = 1
				cursor.execute(sql,da)
				continue
		else:
			wd_ipo += ";"+ da['code']
			continue
		
		windcode_dict  [da['code'] ] = { 
			'name':da['name'],
			'date': da['date'],
			'starter' : da['starter'],
			'zhongzheng500' : da['zhongzheng500'],
			'hushen300' : da['hushen300'],
			'status': da['status'],
			} 
	Connect.commit()
	# print '300568.SZ' in windcode_dict, windcode_dict['300568.SZ']
	# update not exist ipodate
	if len(wd_ipo) > 0 :
		wd_ipo = wd_ipo[1:]

		norm = "briefing,ipo_date"	
		wss = WindPy.wss(wd_ipo,norm )	
		if  not Config.CheckWindData(wss):
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
	# Stock.StocksBasicUpdate(windcode_dict,Connect,WindPy)

	# cursor = Connect.cursor()
	# sql = "select date from stocks_sr order by date desc limit 1"
	# cursor.execute(sql)
	# if cursor.rowcount>0:
	#     stocksCalcDate = cursor.fetchone()[0]

	# sql = "select date from stocks_history_days where ktype=7 order by date desc limit 1"
	# cursor.execute(sql)
	# if cursor.rowcount>0:
	#     stocksNewInfoDate = cursor.fetchone()[0]
	# if stocksNewInfoDate < stocksCalcDate :
	# 	print 'Update Date < CalcDate '
	# 	return
	if Config.CheckDateValid(datetime.now(),WindPy) ==1 :
	    TradeDayFlag = True
	else:
	    TradeDayFlag = False
	
	    return
	print TradeDayFlag    
	suspend0 = Stock.GetSuspendStockSet(current_date,0,WindPy)
	sql = "Replace into stocks_code(windcode,name,status,starter,zhongzheng500,hushen300,date) values(%(code)s,%(name)s,%(status)s,%(starter)s,%(zhongzheng500)s,%(hushen300)s,%(date)s)"
	print "stocks basic filter and limit to free-------------------------------"
	wl = Stock.GetStocksBasic(windcode_dict,Connect)
	
	limittofree = Stock.GetLimitToFreeStockSet(current_date,3,WindPy) #(-3,3) quarter 
	wd_temp = {}
	wd_mainforce = {}
	b4 = 0
	b5 = 0
	b6 = 0
	f= open("temp.txt",'w')
	for windcode in windcode_dict:
		wd = windcode_dict[windcode]
		stars = wl[windcode]
		wd['code'] = windcode
		if windcode in suspend0 :
			wd['status'] = 2			
			cursor.execute(sql,wd)
			continue
		if windcode in limittofree:
			wd['status'] = 3
			cursor.execute(sql,wd)
			continue
		if windcode in wl :
			wd_mainforce[windcode] = wd	
			wd_mainforce[windcode].update({'stars':wl[windcode]})		
			if stars>3:
				wd.update({'stars':wl[windcode]})
				wd_temp[windcode] = wd
			else:
				wd['status'] = 4
				cursor.execute(sql,wd)
			if stars ==4 :
				b4 +=1
			if stars == 5:
				b5 +=1
			if stars == 6:
				f.write(windcode+"\n")
				b6 +=1				
	Connect.commit()	
	f.close()
	print b4,b5,b6
	print len(windcode_dict),len(wd_temp),len(limittofree)
	windcode_dict = wd_temp
	del wl
	del wd_temp

	Stock.StocksMainForceCalc(wd_mainforce,Connect,WindPy)
	
	# ktype_list= [2,4,5,6,7,8]
	ktype_list= [7,8]	
	for ktype in ktype_list :		
		tablename = Stock.GetTableName(MarketType,ktype)
		for windcode in   boardcode_dict  :
			indicator = Stock.StocksOHLC_N(windcode,tablename,ktype,Connect)
			boardcode_dict[windcode]['ta'+str(ktype)] = indicator
		# for windcode in   windcode_dict  :
		# 	indicator = Stock.StocksOHLC_N(windcode,tablename,ktype,Connect)
		# 	windcode_dict[windcode]['ta'+str(ktype)] = indicator
		for windcode in   wd_mainforce  :
			indicator = Stock.StocksOHLC_N(windcode,tablename,ktype,Connect)
			wd_mainforce[windcode]['ta'+str(ktype)] = indicator
	
	if Type !=0 :
		starttime = datestr + " 11:30:00"
		endtime   = datestr + " 11:31:00"
		for windcode in   wd_mainforce  :
			wst = WindPy.wst(windcode,'open,high,low,last,volume',starttime,endtime,'')
			if wst.ErrorCode !=0 or len(wd_mainforce[windcode]['ta'+str(7)])>0:
				continue

			wd_mainforce[windcode]['ta'+str(7)].append({
				'date'  : wst.Times[0],
				'open'  : wst.Data[0][0],
				'high'  : wst.Data[1][0],
				'low'   : wst.Data[2][0],
				'close' : wst.Data[3][0],
				'volume': wst.Data[4][0],
				'pct_chg':0,
				'chg':0,
				'cap':0,
				})
	Require_Num = 3
	for ktype in ktype_list :
		for windcode in windcode_dict:
			windcode_dict[windcode]['ta'+str(ktype)] = wd_mainforce[windcode]['ta'+str(ktype)]
			if ktype == 7 :
				times = [windcode_dict[windcode]['ta7'][index]['date'] for index in range(len(windcode_dict[windcode]['ta7']))][-Require_Num:]
	print "Start stocks board calc-------------------------"
	#update stocks board  strong and ready
	
	# prepare the stocks recent data only ktype 8 currently	
	board7 = Stock.StocksBoardData_New(boardcode_dict,7,Connect,WindPy)
	board8 = Stock.StocksBoardData_New(boardcode_dict,8,Connect,WindPy,FileName='BoardData.txt')
	
	# print board8	
	stock7 = Stock.StocksData_New(windcode_dict,7,Connect,WindPy)
	stock8 = Stock.StocksData_New(windcode_dict,8,Connect,WindPy)
	stock_mainforce7 = Stock.StocksData_New(wd_mainforce,7,Connect,WindPy)
	stock_mainforce8 = Stock.StocksData_New(wd_mainforce,8,Connect,WindPy)
	suspend5 = Stock.GetSuspendStockSet(current_date,-5,WindPy)
	# do calculator
	Stock.StocksBoardCalc(board8)	
	stocksBoardStrong=[]
	stocksBoardReady=[]
	for board in board8:
		if board['isStrong']:
			stocksBoardStrong.append(board)	
			continue
		if board['isReady'] :
			stocksBoardReady.append(board)
			continue
	stocksBoardStrong = Stock.StocksSortVolume(stocksBoardStrong,WindPy)
	stocksBoardReady  = Stock.StocksSortVolume(stocksBoardReady ,WindPy)

	print len(stocksBoardStrong) , len(stocksBoardReady)
	sql = "delete from stocks_board_sr"
	cursor = Connect.cursor()
	cursor.execute(sql)
	sql = "delete from stocks_boardlist_sr"
	cursor.execute(sql)	
	BoardSave(stocksBoardStrong,Connect,WindPy,suspend5,stock_mainforce8,True,False,'BoardStrong.txt')
	BoardSave(stocksBoardReady, Connect,WindPy,suspend5,stock_mainforce8,False,True,'BoardReady.txt') 
	
	print "Start stocks calc-------------------------"
	stocks_tablename = "stocks_sr"
	# cursor.execute("delete from "+stocks_tablename + " where stype<2")
	tdo = WindPy.tdaysoffset(-Config.BreakCentralPermitDays,datestr,"")
	if Config.CheckWindData(tdo) :
		cursor.execute("delete from "+stocks_tablename + " where stype=2 and date<=%(date)s",{'date':tdo.Data[0][0]})
	tdo = WindPy.tdaysoffset(-Config.BreakVolumePermitDays,datestr,"")
	if Config.CheckWindData(tdo) :
		cursor.execute("delete from "+stocks_tablename + " where stype=3 and date<=%(date)s",{'date':tdo.Data[0][0]})
	windcode_list8 = []
	windcode_list7= []
	for stock in stock7:
		if stock['windcode'] in suspend5:
			continue
		windcode_list7.append(stock)	
	for stock in stock8:
		if stock['windcode'] in suspend5:
			continue
		windcode_list8.append(stock)		
	Stock.StocksCalc(windcode_list8)
	
	stocksStrong=[]
	stocksReady=[]
	for board in windcode_list8:
		if board['isStrong']:
			stocksStrong.append(board)	
			continue
		if board['isReady'] :
			windcode = board['windcode']			
			if windcode_dict[windcode]['ta8'][-1]['close']>board['pb6'][-1] :
				stocksReady.append(board)
			continue
	
	# for board in windcode_list8:
	# 	if board['isStrong'] and board['windcode']=='600420.SH':
	# 		m = -5
	# 		print 'diff', board['diff'][m: ]
	# 		print 'dea', board['dea'][m: ]
	# 		print 'pb1', board['pb1'][m: ]
	# 		print 'pb2', board['pb2'][m: ]
	# 		print 'pb6', board['pb6'][m: ]	
	# 		print 'fast_k', board['fast_k'][m: ]
	# 		print 'fast_d', board['fast_d'][m: ]	
	# 		print 'slow_k', board['slow_k'][m: ]
	# 		print 'slow_d', board['slow_d'][m: ]	

	# return 
	print len(stocksStrong),len(stocksReady)
	stocksStrong = Stock.StocksSortPctchg(stocksStrong,-5,0.2,0   ,WindPy)
	StockSave(stocksStrong,current_date,stocks_tablename,0,8,Connect)
	
	stocksReady  = sorted(stocksReady, key=lambda stock:stock['fast_k'][-1])#Stock.StocksSortPctchg(stocksReady ,-5,0.2,-0.1,WindPy)  
	StockSave(stocksReady,current_date,stocks_tablename,1,8,Connect)
	

	
	 
	rt = Stock.StocksFundIndexCalc(windcode_dict,7,windcode_list7,windcode_list8,Require_Num=Require_Num)
	FundIndexNumList = [ rt[x]['num'] for x in range(len(rt))]
	IndexSave(FundIndexNumList,'FundIndex',times,Connect) 

	StrongIndexNumList = Stock.StocksStrongIndexCalc(windcode_list8,Require_Num=Require_Num)
	print StrongIndexNumList
	IndexSave(StrongIndexNumList,'StrongIndex',times,Connect)
	ReadyIndexNumList = Stock.StocksReadyIndexCalc(windcode_list8,0.6,-0.3,Require_Num=Require_Num)
	IndexSave(ReadyIndexNumList,'ReadyIndex',times,Connect)
	
	if False:
		# print FundIndexNumList
		dl = len(times)
		f= open("FundIndex.txt",'w')
		for index in range( dl ):
			f.write("Date:%s FundIndex:%d \n" % (times[dl-index-1].strftime("%Y-%m-%d"),FundIndexNumList[index]))
			
			for si in range(len(rt[index]['windcode'])):
				f.write('  index:%d code:%s \n' %(si,rt[index]['windcode'][si]) )
		f.close()
	stocksBreakVolume=[]
	for board in windcode_list8:
		if board['windcode'] in  rt[0]['windcode']:
			stocksBreakVolume.append(board)
	StockSave(stocksBreakVolume,current_date,stocks_tablename,3,8,Connect)
	
	breakcentral = Stock.StocksBreakCentral(wd_mainforce,stock_mainforce7,stock_mainforce8)
	print breakcentral
	stocksBreakCentral=[]
	for board in stock_mainforce8:
		if board['windcode'] in breakcentral:
			stocksBreakCentral.append(board)	
	StockSave(stocksBreakCentral,current_date,stocks_tablename,2,8,Connect)		
	
	cursor.close()
	del windcode_list7,windcode_list8

def DaysFundIndex(Connect,WindPy):
	global board8
	global stock8
	current_date = datetime.now()	
	datestr = current_date.strftime("%Y-%m-%d")
	
	
	wd_mainforce  = {}

	
	MarketType = 1
	
		
	wsdata=WindPy.wset("sectorconstituent","date="+datestr+";sectorid=a001010100000000")
	if not Config.CheckWindData(wsdata):
		return
	num = 0
	for index in range(0,len(wsdata.Data[0])):		
		sdate = wsdata.Data[0][index]
		da = {
				'code' : wsdata.Data[1][index],
				'name' : wsdata.Data[2][index],
				'date' : sdate,
				'status': 0,
				
			}		
		num += 1
		# if num >100:
		# 	break
	
		wd_mainforce  [da['code'] ] = { 'name':da['name'],'date': da['date'],'status': 0,'stars' : 6,} 
	print num
	Require_Num = 1
	# ktype_list= [2,4,5,6,7,8]
	ktype_list= [7]	
	for ktype in ktype_list :		
		tablename = Stock.GetTableName(MarketType,ktype)
		for windcode in   wd_mainforce  :
			indicator = Stock.StocksOHLC_N(windcode,tablename,ktype,Connect)
			wd_mainforce[windcode]['ta'+str(ktype)] = indicator
			times = [wd_mainforce[windcode]['ta7'][index]['date'] for index in range(len(wd_mainforce[windcode]['ta7']))][-Require_Num:]

				
	
	#update stocks board  strong and ready
	
	
	# print board8	

	stock_mainforce7 = Stock.StocksData_New(wd_mainforce,7,Connect,WindPy)
	# stock_mainforce8 = Stock.StocksData_New(wd_mainforce,8,Connect,WindPy)
	suspend5 = Stock.GetSuspendStockSet(current_date,-5,WindPy)
	# do calculator

	
	print "Start stocks calc-------------------------"
	stocks_tablename = "stocks_sr"	
	
	windcode_list7= []
	for stock in stock_mainforce7:		
		windcode_list7.append(stock)	


	

	
	 
	rt = Stock.StocksFundIndexCalc(wd_mainforce,7,windcode_list7,Require_Num=Require_Num)
	FundIndexNumList = [ rt[x]['num'] for x in range(len(rt))]
	# IndexSave(FundIndexNumList,'FundIndex',times,Connect) 

	
	if True:
		print FundIndexNumList
		dl = len(times)
		f= open("FundIndex.txt",'w')
		for index in range( dl ):
			f.write("Date:%s FundIndex:%d \n" % (times[dl-index-1].strftime("%Y-%m-%d"),FundIndexNumList[index]))
			
			for si in range(len(rt[index]['windcode'])):
				f.write('  index:%d code:%s \n' %(si,rt[index]['windcode'][si]) )
		f.close()
	
	
	
	del windcode_list7


import cProfile
if __name__ == '__main__':
	import argparse
	parser = argparse.ArgumentParser(description='argparse tester')
	parser.add_argument('--dtype', type=int, help=" calc type ", default=0)
	args = parser.parse_args()
	dtype = args.dtype
	
	
	cnx = mysql.connector.connect(**Config.config)
	cnx.set_converter_class(Slice.NumpyMySQLConverter)
	
	w.start()
	print 'Calculate type:',dtype
	# BoardListUpdate('E:/Python_LieFeng/board.txt',cnx,w)
	# BoardListUpdate('E:/Python_LieFeng/board1.txt',cnx,w)	
	print "Start WindPy--------------"
	# prof = cProfile.Profile()
	# prof.enable()
	# Update(cnx,w)
	DaysCalc(dtype,cnx,w)
	# DaysFundIndex(cnx,w)
	# prof.create_stats()
	# prof.print_stats()
	w.stop()
	print "Stop  WindPy---------------"
	
	cnx.close()
