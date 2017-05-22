from LieFeng import Config,Slice,FourIndicator,Stock

import  mysql.connector

def Update():
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
	# Stock.StocksHistoryUpdate(windcode_set ,0,base_days=365*3,minflag=True)
	# Stock.StocksHistoryUpdate(windcode_set ,8,base_days=365*30,minflag=True)
	# Stock.StocksHistoryUpdate(stocks_board_set ,8,base_days=365*30,minflag=True)
	# Stock.StocksHistoryUpdate(stocks_board_set ,7,base_days=365*30,minflag=True)
	# Stock.StocksHistoryUpdate(windcode_set,7,base_days=365*30,minflag=True)
	#slice and techincal analysis
	# ktype_list = [2,4,5,6]
	# for windcode in windcode_set :
	# 	Slice.SliceMinuteUpdate(windcode,MarketType,ktype_list,cnx,None)
	ktype_list= [7,8]
	for ktype in ktype_list :
		sql = "select diff from stocks_history where ktype=%(ktype)s and prod_code=%(windcode)s order by date desc limit 1"
		for windcode in stocks_board_set :			
			cursor.execute(sql,{'windcode':windcode,'ktype':ktype,})
			if cursor.rowcount!=0 and cursor.fetchone()[0] is None :				
				indicator = Stock.StocksOHLC(windcode,MarketType,ktype,cnx)
				if ktype ==8 and len(indicator)<52 :
					continue
				print windcode
				Slice.CalcTechicalAnalysis(indicator,windcode,MarketType,ktype,cnx,UpdateFlag=True)
			# FourIndicator.GoldenDead(windcode,MarketType,ktype)
			# FourIndicator.BullBearLongShort(windcode,MarketType,ktype)
		for windcode in windcode_set :
			cursor.execute(sql,{'windcode':windcode,'ktype':ktype,})
			if cursor.rowcount!=0 and cursor.fetchone()[0] is None :				
				indicator = Stock.StocksOHLC(windcode,MarketType,ktype,cnx)
				if ktype ==8 and len(indicator)<52 :
					continue
				print windcode
				Slice.CalcTechicalAnalysis(indicator,windcode,MarketType,ktype,cnx,UpdateFlag=True)
			# FourIndicator.GoldenDead(windcode,MarketType,ktype)
			# FourIndicator.BullBearLongShort(windcode,MarketType,ktype)
	cursor.close()
	# update stocks board 
	#
	cnx.commit()
	cnx.close()

def Init():
	cnx = mysql.connector.connect(**Config.config)
	Stock.StocksBoardImport('E:/Outsourcing/LieFeng/requirement/board.txt',cnx)#"E:/Python_LieFeng/board.txt"
	cnx.close()
if __name__ == '__main__':
	#Init()
	Update()