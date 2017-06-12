#!/usr/bin/env python
# -*- coding: utf8 -*-
from LieFeng import Config,Slice,FourIndicator,Stock
from WindPy import w
from datetime import time,datetime
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

def Init():
	cnx = mysql.connector.connect(**Config.config)
	Stock.StocksBoardImport('E:/Outsourcing/LieFeng/requirement/board.txt',cnx)#"E:/Python_LieFeng/board.txt"
	cnx.close()

def BoardSave(StocksBoard,Connect,WindPy,suspend5,stock8,isStrong,isReady,FileName):
	cursor = Connect.cursor()
	f = open(FileName,'w')
	for i in range(0,len(StocksBoard)):
		sql = " insert into stocks_board_test(id,sectorid,name,type,windcode,update_date,isStrong,isReady,pct_chg,cap) values(%(id)s,%(sectorid)s,%(name)s,%(type)s,%(windcode)s,%(update_date)s,%(isStrong)s,%(isReady)s,%(pct_chg)s,%(cap)s)"
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
			 if wd['windcode']	in stocklist:
			 	if  'sectorid' in wd :
			 		wd['sectorid'] = "%s;%s" % (wd['sectorid'],data['sectorid'])
			 	else :
			 		wd['sectorid'] = data['sectorid']
			 	sl.append(wd)	
		print len(sl),len(stocklist)
		stocklist =Stock.StocksBoardStockCalc(sl)
		print len(sl),len(stocklist)
		if len(stocklist) == 0 :
			Connect.commit()
			continue
		StocksBoard[i]['stocklist'] = Stock.StocksSortPctchg(stocklist,-5,0.2,0.1,WindPy)
		for j in range(0,len(StocksBoard[i]['stocklist'])):			
			stock = StocksBoard[i]['stocklist'][j]
			
			sql = "Insert into stocks_boardlist_test(id,sectorid,windcode,name,isStrong,isReady,pct_chg,cap) Values(%(id)s,%(sectorid)s,%(windcode)s,%(name)s,%(isStrong)s,%(isReady)s,%(pct_chg)s,%(cap)s)"
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

def UpdateData_All(Board_Dict,Windcode_Dict,Connect,WindPy):
	zs_dict = {}
	zs = ["000001.SH",'399300.SZ',"399006.SZ","150051.SZ","399005.SZ","399905.SZ"]
	for code in zs:
		zs_dict [code] =  code

	cursor = Connect.cursor()
	wd = datetime.now().weekday()
	for ktype in [7,8]:		
		# if wd <5 and ktype == 8 :
		# 	continue
		# if wd >4 and ktype == 7 :
		# 	continue
		print 'ktype ',ktype
		Stock.StocksDaysHistoryUpdate(Board_Dict    ,ktype,Connect,WindPy)
		Stock.StocksDaysHistoryUpdate(Windcode_Dict ,ktype,Connect,WindPy)
		Stock.StocksDaysHistoryUpdate(zs_dict ,ktype,Connect,WindPy)		
	# if wd < 6:
	Stock.StocksMinutesHistoryUpdate(Windcode_Dict ,0,Connect,WindPy)
	Stock.StocksMinutesHistoryUpdate(zs_dict ,0,Connect,WindPy)
	MarketType = 1

	#slice and techincal analysis
	ktype_list = [2,4,5,6]
	for windcode in Windcode_Dict :
		Slice.SliceMinuteUpdate_New(windcode,ktype_list,Connect)
	for windcode in zs_dict :
		Slice.SliceMinuteUpdate_New(windcode,ktype_list,Connect)
	# cursor.execute("delete from stocks_basic")
	# Stock.StocksBasicUpdate(Windcode_Dict,Connect,WindPy)

def Update(Connect,WindPy):

	current_date = datetime.now()	
	datestr = current_date.strftime("%Y-%m-%d")
	
	boardcode_dict = {}	
	windcode_dict  = {}

	
	MarketType = 1
	  
	suspend0 = Stock.GetSuspendStockSet(current_date,0,WindPy)
	cursor = Connect.cursor()
	# Windcode_Calc={}
	# sql = "select  name, windcode,stype from stocks_sr" 
	# cursor.execute(sql)
	# for bname,windcode,stype in cursor:
	#     Windcode_Calc [windcode] = {'name':bname,'type':3}
	# sql = "select windcode,date from stocks_mainforce order by date "
	# cursor.execute(sql)
	# for windcode,sdate in cursor:
	#     Windcode_Calc [windcode] = {'type':4,}
	# cursor_stars = Connect.cursor()

	# sql_stars = "select content from stocks_basic where windcode=%(code)s and btype=9 order by quarter desc limit 1"
	sql = "select  name, windcode,sectorid from stocks_board"	
	
	# cursor = Connect.cursor()
	
	cursor.execute(sql)
	for bname,windcode,sectorid in cursor:		
		boardcode_dict [windcode] = {'name':bname,}
		
	wsdata=WindPy.wset("sectorconstituent","date="+datestr+";sectorid=a001010100000000")
	if not Config.CheckWindData(wsdata):
		return
	cursor.execute("delete from stocks_code")
	sql = "Insert into stocks_code(windcode,name,date) values(%(code)s,%(name)s,%(date)s)"
	for index in range(0,len(wsdata.Data[0])):		
		sdate = wsdata.Data[0][index]
		da = {
				'code' : wsdata.Data[1][index],
				'name' : wsdata.Data[2][index],
				'date' : sdate,
			}
		# cursor_stars.execute(sql_stars,{'code':windcode,})
		# if cursor_stars.rowcount > 0 and cursor_stars.fetchone()[0] >5 :
		#     Windcode_Calc [windcode] = {'name':bname,'type':5} 
		cursor.execute(sql,da)	
		if da['code'] in suspend0:
			continue	
		windcode_dict  [da['code'] ] = { 'name':da['name'],}
	Connect.commit()
	
	Stock.StocksBasicUpdate(windcode_dict,Connect,WindPy)

	for ktype in [7,8]:		
		# if wd <5 and ktype == 8 :
		# 	continue
		# if wd >4 and ktype == 7 :
		# 	continue
		print 'ktype ',ktype
		Stock.StocksDaysHistoryUpdate(boardcode_dict    ,ktype,Connect,WindPy)
		Stock.StocksDaysHistoryUpdate(windcode_dict ,ktype,Connect,WindPy)
		# Stock.StocksDaysHistoryUpdate(Windcode_Calc    ,ktype,Connect,WindPy)
		
	# Stock.StocksMinutesHistoryUpdate(Windcode_Calc ,0,Connect,WindPy)
	# #slice and techincal analysis
	# ktype_list = [2,4,5,6]
	# for windcode in Windcode_Calc :
	# 	Slice.SliceMinuteUpdate_New(windcode,ktype_list,Connect)

	UpdateData_All(boardcode_dict,windcode_dict,Connect,WindPy)
	
	print "stocks basic filter and limit to free-------------------------------"
	wl = Stock.StocksBasicCalc(windcode_dict,Connect,WindPy)
	
	limittofree = Stock.GetLimitToFreeStockSet(current_date,3,WindPy)
	wd_temp = []
	b4 = 0
	b5 = 0
	b6 = 0
	f= open("temp.txt",'w')
	for windcode in windcode_dict:
		wd = windcode_dict[windcode]
		stars = wl[windcode]			
		if windcode in limittofree:
			continue
		if windcode in wl :
			# print data
			
			if stars>3:
				wd.update({'stars':wl[windcode]})
				wd_temp.append(wd)
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
	# ktype_list= [2,4,5,6,7,8]
		
	Connect.commit()
	cursor.close()

def DataCheck(Connect,WindPy):
	current_date = datetime.now()
	
	tdo = WindPy.tdaysoffset(0, current_date, "")
	if Config.CheckWindData(tdo) :
		current_date = tdo.Data[0][0]

	datestr = current_date.strftime("%Y-%m-%d")
	
	boardcode_dict = {}	
	windcode_dict  = {}

	
	MarketType = 1
	  
	suspend0 = Stock.GetSuspendStockSet(current_date,0,WindPy)
	cursor = Connect.cursor()

	sql = "select  name, windcode from stocks_board"	
	
	# cursor = Connect.cursor()
	
	cursor.execute(sql)
	for bname,windcode in cursor:		
		boardcode_dict [windcode] = {'name':bname,}
		
	
	cursor.execute("select name,windcode,date from stocks_code")
	num = 0
	for bname,windcode,bdate in cursor:			
		if windcode in suspend0:
			continue
		if bdate.date()	== datetime.now().date() :
			num +=1
		windcode_dict  [ windcode ] = { 'name': bname,}
	if num != len(windcode_dict) :
		message = str(num)+"/"+str(len(windcode_dict))
		Config.SendErrorMessage(current_date,0,u'股票代码获取不全! '+message)
	
	
	sql ="select windcode,date from stocks_history_days where windcode=%(code)s and ktype=%(ktype)s order by date desc limit 1"
	for ktype in [7,8]:
		num = 0 	
		for windcode in boardcode_dict:	
			cursor.execute(sql,{'code':windcode,'ktype':ktype})
			if cursor.rowcount>0:
				wdate = cursor.fetchone()[1]
				if wdate.date() == current_date.date() :
					num += 1
			else:
				num +=1
		if num != len(boardcode_dict):
			message ='ktype='+str(ktype)+' '+ str(num)+"/"+str(len(boardcode_dict))
			Config.SendErrorMessage(current_date,0,u'Board '+message)
		num = 0 	
		for windcode in windcode_dict:	
			cursor.execute(sql,{'code':windcode,'ktype':ktype})
			if cursor.rowcount>0:
				wdate = cursor.fetchone()[1]
				if wdate.date() == current_date.date() :
					num += 1
			else:
				num +=1
		if num != len(windcode_dict):
			message ='ktype='+str(ktype)+' '+ str(num)+"/"+str(len(windcode_dict))
			Config.SendErrorMessage(current_date,0,u'wincode '+message)
		
	sql ="select windcode,date from stocks_history_minutes where windcode=%(code)s and ktype=%(ktype)s order by date desc limit 1"
	for ktype in [0,2,4,5,6]:		
		num = 0 	
		for windcode in windcode_dict:	
			cursor.execute(sql,{'code':windcode,'ktype':ktype})
			if cursor.rowcount>0:
				wdate = cursor.fetchone()[1]
				# print wdate ,current_date
				if wdate.date() == current_date.date() :
					num += 1
			else:
				num +=1
		if num != len(windcode_dict):
			message ='ktype='+str(ktype)+' '+ str(num)+"/"+str(len(windcode_dict))
			Config.SendErrorMessage(current_date,0,u'wincode '+message)
		else:
			print 'ktype ',ktype,num, len(windcode_dict)


	sql ="select windcode,quarter from stocks_basic where btype=9 and windcode=%(code)s order by quarter desc limit 1"
		
	num = 0 	
	for windcode in windcode_dict:	
		cursor.execute(sql,{'code':windcode })
		if cursor.rowcount>0:
			wdate = cursor.fetchone()[1]
			if wdate.date() == current_date.date() :
				num += 1
		else:
			num +=1
	if num != len(windcode_dict):
		message ='stars '+ str(num)+"/"+str(len(windcode_dict))
		Config.SendErrorMessage(current_date,0,u'wincode '+message)		
	else:
		print 'stars ',num ,len(windcode_dict)

			
	
		
	Connect.commit()
	cursor.close()	

# import cProfile
if __name__ == '__main__':
	cnx = mysql.connector.connect(**Config.config)
	cnx.set_converter_class(Slice.NumpyMySQLConverter)
	
	w.start()

	# BoardListUpdate('E:/Python_LieFeng/board.txt',cnx,w)
	# BoardListUpdate('E:/Python_LieFeng/board1.txt',cnx,w)	
	
	print "Start WindPy--------------"
	# prof = cProfile.Profile()
	# prof.enable()
	Update(cnx,w)
	DataCheck(cnx,w)
	# UpdateData_All([],[],cnx,w)
	# prof.create_stats()
	# prof.print_stats()
	print "Stop  WindPy---------------"
	w.stop()
	cnx.close()
