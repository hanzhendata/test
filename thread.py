import time, threading
from LieFeng import Config,Slice,FourIndicator,Stock
from WindPy import w
import  mysql.connector

lock = threading.Lock()
stocksBoardStrong = []
def loop(board,i,connect):	
	print 'thread %s is running...' % threading.current_thread().name
	cursor = connect.cursor()
	sql = " insert into stocks_board_sr(id,stype,sectorid,name,type,windcode,update_date,isStrong,isReady) values(%(id)s,%(stype)s,%(sectorid)s,%(name)s,%(type)s,%(windcode)s,%(update_date)s,%(isStrong)s,%(isReady)s)"
	data ={
		'id'	:  i ,
		'stype' :  1,
		'sectorid':board['sectorid'] ,
		'name':board['name'] ,
		'windcode':board['windcode'],
		'type': board ['type'],
		'update_date' : board ['update_date'],
		'isStrong': True,
		'isReady': False 
	}
	cursor.execute(sql , data)
	board['stocklist'] = Stock.StocksStrong(board,8,1,w,cnx)
	for j in range(0,len(board['stocklist'])):
		stock = board['stocklist'][j]
		sql = "Insert into stocks_boardlist_sr(id,stype,sectorid,windcode,name,isStrong,isReady) Values(%(id)s,%(stype)s,%(sectorid)s,%(windcode)s,%(name)s,%(isStrong)s,%(isReady)s)"
		data = {
			'id': j,
			'stype' :  1,
			'sectorid':board['sectorid'],
			'windcode':stock['windcode'],
			'name': stock['name'],
			'isStrong': True,
			'isReady': False ,
		}
		cursor.execute(sql , data)
	connect.commit()
	cursor.close()
	lock.acquire()
	try:
		stocksBoardStrong[i]['stocklist'] = board['stocklist']
	finally:
		lock.release()
	print 'thread %s ended.' % threading.current_thread().name


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
	cursor.close()


	print 'thread %s is running...' % threading.current_thread().name


	#update 
	start = time.time()
	pool = []
	for code in windcode_set:
		t = threading.Thread(target=Stock.StocksHistoryUpdate,name='UpdateThread',args=([code],0),kwargs={'base_days':365*3,'minflag':False,})
		t.setDaemon(True)
		t.start()
		pool.append(t)
	for t in pool:
		t.join()
	end = time.time()
	print 'Thread %s runs %0.2f seconds.' % (threading.current_thread().name, (end - start))
	print 'Update 0 thread %s ended.' % threading.current_thread().name
	pool = []
	for code in windcode_set:
		t = threading.Thread(target=Stock.StocksHistoryUpdate,name='UpdateThread',args=([code],7),kwargs={'base_days':365*30,})
		t.setDaemon(True)
		t.start()
		pool.append(t)
	for t in pool:
		t.join()
	print 'Update 7 thread %s ended.' % threading.current_thread().name
	pool = []
	for code in windcode_set:
		t = threading.Thread(target=Stock.StocksHistoryUpdate,name='UpdateThread',args=([code],8),kwargs={'base_days':365*30,})
		t.setDaemon(True)
		t.start()
		pool.append(t)
	for t in pool:
		t.join()
	print 'Update 8 thread %s ended.' % threading.current_thread().name
	pool = []
	for code in stocks_board_set:
		t = threading.Thread(target=Stock.StocksHistoryUpdate,name='UpdateThread',args=([code],7),kwargs={'base_days':365*30,})
		t.setDaemon(True)
		t.start()
		pool.append(t)
	for t in pool:
		t.join()
	print 'Update 7 thread %s ended.' % threading.current_thread().name
	pool = []
	for code in stocks_board_set:
		t = threading.Thread(target=Stock.StocksHistoryUpdate,name='UpdateThread',args=([code],8),kwargs={'base_days':365*30,})
		t.setDaemon(True)
		t.start()
		pool.append(t)
	for t in pool:
		t.join()
	print 'Update 8 thread %s ended.' % threading.current_thread().name

	w.start()
	print "Start WindPy--------------"
	stocksBoardStrong = Stock.StocksBoardStrong1(8,1,w,cnx)
	pool =[]
	start = time.time()
	for i in range(len(stocksBoardStrong)):
		t = threading.Thread(target=loop, name='LoopThread',args=(stocksBoardStrong[i],i,cnx))
		t.setDaemon(True)
		t.start()
	for t in pool:
		t.join()
	end = time.time()
	print 'Thread %s runs %0.2f seconds.' % (threading.current_thread().name, (end - start))
	print 'thread %s ended.' % threading.current_thread().name

	# stocksBoardReady  = Stock.StocksBoardReady (8,1,w,cnx)
	# for i in range(0,len(stocksBoardReady)):
	#     stocksBoardReady[i]['stockslist'] = Stock.StocksReady (stocksBoardReady[i] ,8,1,w,cnx)
	w.stop()
	#print stocksBoardStrong
	print "Stop  WindPy--------------"
	#update stocks board 
	#
	f = open("E:/stocksBoardStrong.txt",'w')
	for i in range(len(stocksBoardStrong)):
		board = stocksBoardStrong[i]
		sql = '%s %s %d \r\n' % (board['name'],board['windcode'],i)
		f.write(sql)
		for j in range(len(board['stocklist'])):
			sql = '%s %s %s %d \r\n' % (board['windcode'],board['stocklist']['name'],board['stocklist']['windcode'],j)
			f.write(sql)
	f.close()
if __name__ == '__main__':
	#Init()
	# Update()
	RunOnce()

