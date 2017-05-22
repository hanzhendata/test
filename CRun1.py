from LieFeng import Config,Slice,FourIndicator,Stock

import  mysql.connector

def Update():
	cnx = mysql.connector.connect(**Config.config)
	cnx.set_converter_class(Slice.NumpyMySQLConverter)
	#set windcode
	windcode_set = set()
	MarketType = 1
	sql = "select  name, windcode,sectorid from stocks_board"	
	sql_list = "select name,windcode from stocks_boardlist where sectorid=%(sectorid)s"

	cursor = cnx.cursor()
	cursor_l = cnx.cursor()
	cursor.execute(sql)
	for bname,windcode,sectorid in cursor:		
		windcode_set.add(windcode)
		cursor_l.execute(sql_list,{'sectorid':sectorid})
		for lname,lwindcode in cursor_l:
			windcode_set.add(lwindcode)		
	cursor_l.close()
	 
	#update 
	Stock.StocksHistoryUpdate(windcode_set ,0,base_days=365*3,minflag=False)
	Stock.StocksHistoryUpdate(windcode_set ,8,base_days=365*30,minflag=False)
	#slice and techincal analysis
	ktype_list = [2,4,5,6]
	for windcode in windcode_set :
		Slice.SliceMinuteUpdate(windcode,MarketType,ktype_list,cnx,None)
	ktype_list= [2,4,5,6,7,8]
	for ktype in ktype_list :
		for windcode in windcode_set :
			indicator = Stock.StocksOHLC(windcode,MarketType,ktype,cnx)
			Slice.CalcTechicalAnalysis(indicator,windcode,MarketType,ktype,cnx,UpdateFlag=True)
			# FourIndicator.GoldenDead(windcode,MarketType,ktype)
			# FourIndicator.BullBearLongShort(windcode,MarketType,ktype)
	#update stocks board  strong and ready
	sql = "delete from stocks_board_sr"
	cursor = cnx.cursor()
	cursor.execute(sql)
	sql = "delete from stocks_boardlist_sr"
	cursor.execute(sql)
	
	from WindPy import w
	w.start()
	print "Start WindPy--------------"
	stocksBoardStrong = Stock.StocksBoardStrong(8,1,w,cnx)	
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
		stocksBoardStrong[i]['stocklist'] = Stock.StocksStrong(stocksBoardStrong[i],8,1,w,cnx)
		for j in range(0,len(stocksBoardStrong[i]['stocklist'])):
			stock = stocksBoardStrong[i]['stocklist'][j]
			sql = "Insert into stocks_boardlist_sr(id,sectorid,windcode,name,isStrong,isReady) Values(%(id)s,%(sectorid)s,%(windcode)s,%(name)s,%(isStrong)s,%(isReady)s)"
			data = {
				'id': j,
				'sectorid':stocksBoardStrong[i]['sectorid'],
				'windcode':stock['windcode'],
				'name': stock['name'],
				'isStrong': True,
				'isReady': False ,
			}
			cursor.execute(sql , data)

	# stocksBoardReady  = Stock.StocksBoardReady (8,1,w,cnx)
	# for i in range(0,len(stocksBoardReady)):
	#     stocksBoardReady[i]['stockslist'] = Stock.StocksReady (stocksBoardReady[i] ,8,1,w,cnx)
	w.stop()
	cursor.clsoe()
	cnx.commit()
	cnx.close()

def Init():
	cnx = mysql.connector.connect(**Config.config)
	Stock.StocksBoardImport('E:/Outsourcing/LieFeng/requirement/board.txt',cnx)#"E:/Python_LieFeng/board.txt"
	cnx.close()

def RunOnce():
	cnx = mysql.connector.connect(**Config.config)
	cnx.set_converter_class(Slice.NumpyMySQLConverter)
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
	
	#update 
	# Stock.StocksHistoryUpdate(windcode_set ,0,base_days=365*3,minflag=False)
	Stock.StocksHistoryUpdate(windcode_set ,8,base_days=365*30 )
	Stock.StocksHistoryUpdate(stocks_board_set ,7,base_days=365*30 )
	Stock.StocksHistoryUpdate(stocks_board_set ,8,base_days=365*30 )
	Stock.StocksHistoryUpdate(windcode_set,7,base_days=365*30 )
	#slice and techincal analysis
	# ktype_list = [2,4,5,6]
	# for windcode in windcode_set :
	# 	Slice.SliceMinuteUpdate(windcode,MarketType,ktype_list,cnx,None)
	ktype_list= [7,8]
	for ktype in ktype_list :
		for windcode in stocks_board_set :
			indicator = Stock.StocksOHLC(windcode,MarketType,ktype,cnx)
			Slice.CalcTechicalAnalysis(indicator,windcode,MarketType,ktype,cnx,UpdateFlag=True)
			FourIndicator.GoldenDead(windcode,MarketType,ktype)
			FourIndicator.BullBearLongShort(windcode,MarketType,ktype)
		for windcode in windcode_set :
			indicator = Stock.StocksOHLC(windcode,MarketType,ktype,cnx)
			Slice.CalcTechicalAnalysis(indicator,windcode,MarketType,ktype,cnx,UpdateFlag=True)
			FourIndicator.GoldenDead(windcode,MarketType,ktype)
			FourIndicator.BullBearLongShort(windcode,MarketType,ktype)
	from WindPy import w
	w.start()
	print "Start WindPy--------------"
	stocksBoardStrong = Stock.StocksBoardStrong(8,1,w,cnx)
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
		stocksBoardStrong[i]['stocklist'] = Stock.StocksStrong(stocksBoardStrong[i],8,1,w,cnx)
		for j in range(0,len(stocksBoardStrong[i]['stocklist'])):
			stock = stocksBoardStrong[i]['stocklist'][j]
			sql = "Insert into stocks_boardlist_sr(id,sectorid,windcode,name,isStrong,isReady) Values(%(id)s,%(sectorid)s,%(windcode)s,%(name)s,%(isStrong)s,%(isReady)s)"
			data = {
				'id': j,
				'sectorid':stocksBoardStrong[i]['sectorid'],
				'windcode':stock['windcode'],
				'name': stock['name'],
				'isStrong': True,
				'isReady': False ,
			}
			cursor.execute(sql , data)
	# stocksBoardReady  = Stock.StocksBoardReady (8,1,w,cnx)
	# for i in range(0,len(stocksBoardReady)):
	#     stocksBoardReady[i]['stockslist'] = Stock.StocksReady (stocksBoardReady[i] ,8,1,w,cnx)
	w.stop()
	print stocksBoardStrong
	print "Stop  WindPy--------------"
	#update stocks board 
	#
	


if __name__ == '__main__':
	#Init()
	# Update()
	RunOnce()

