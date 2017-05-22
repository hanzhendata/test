# _*_ coding: utf-8 -*-
from WindPy import *
from datetime import date, datetime, timedelta
import mysql.connector
from Config import *
"""
SHFE 上期所 上海期货 
DCE  大商所 大连商品
CZCE 郑商所 郑州商品
SGE  上金所
CFFEX  中金所
*FI.WI 为指数
*.SHF

RB 螺纹钢 SHFE 
AG 白银   SHFE
AU 黄金   SHFE
CU 沪铜   SHFE  
AL 沪铝   SHFE
ZN 沪锌   SHFE
PB 沪铅     SHFE
RU 橡胶   SHFE
FU 燃油   SHFE
WR 线材    SHFE
BU 石油沥青  SHFE
HC 热轧卷板  SHFE
NI 沪镍     SHFE
SN 沪锡     SHFE
A 豆一 DCE
B 豆二
BB 胶合板
C 玉米
CS 玉米淀粉
FB 纤维板
I 铁矿石
J 焦炭
JD 鸡蛋
JM 焦煤
L 乙烯（塑料）
V  PVC
M 豆粕
Y 豆油
P 棕榈油
PP 聚丙烯

CF 棉花  CZC
FG 玻璃
JR 粳稻
LR 晚籼稻
MA 甲醇
RS 菜籽  
RM 菜粕
OI 菜油
PM 普麦
RI 早籼稻
SF 硅铁
SM 锰硅
SR 白糖
TA PTA
WH 强麦
ZC 动力煤
"""
def  FuturesHistoryUpdate(ktype,base_days= 365*2):		
		""" Function doc """
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
                windcode = []
	
		
		starttime = endtime - td
	       	
	       	
	       	insert_sql = ("INSERT INTO futures_history "
			      "(prod_code, ktype, date,"
			      " open_price,high_price,low_price,close_price,"
			      "fast_k,fast_d,fast_j,"
			      "diff,dea,macd,ema12,ma5,slow_k,slow_d ,slow_j ,b2, b1,b3 ,ema26,ma60,"
			      "ema4,ema6,ema9,ema13,ema18,ema24)  "
			      " VALUES ( %(code)s, "+str(ktype)+", %(time)s, "
			      "%(open_price)s,%(high_price)s,%(low_price)s,%(close_price)s,"
			      "%(fast_k)s,%(fast_d)s,%(fast_j)s,"
			      "%(diff)s,%(dea)s,%(macd)s,%(ema12)s,%(ma5)s, %(slow_k)s,%(slow_d)s,%(slow_j)s,%(b2)s, %(b1)s, %(b3)s, "
			      "%(ema26)s,%(ma60)s,%(ema4)s,%(ema6)s,%(ema9)s,%(ema13)s,%(ema18)s,%(ema24)s )")
		kl = ['open_price' , 'high_price', 'low_price', 'close_price', 'fast_k', 'fast_d', 'fast_j',
			 'diff',  'dea', 'macd' ,'ema12',  'ma5', 'slow_k', 'slow_d', 'slow_j', 'b2', 'b1', 'b3',
			  'ema26','ma60','ema4','ema6','ema9','ema13','ema18','ema24']
		cursor.execute("select finance_mic,tradecode from futures")
		btime = datetime.now() 
		for (mic,tradecode) in cursor:
			if ktype<7:
				windcode = "{}.{}".format(tradecode,mic)
			else:
				windcode = "{}FI.WI".format(tradecode)
			print windcode
			#get starttime
		        select_sql = "select date from futures_history where prod_code=%(code)s and ktype=%(ktype)s order by date desc limit 1"
		        select_data = {
			   'code':windcode,
			   'ktype':ktype,
		        }		        
		        cursor_maxdate.execute(select_sql,select_data)
		        if (cursor_maxdate.rowcount==0):
				starttime = endtime - td
		        else:
				starttime = cursor_maxdate.fetchone()[0] + SingleTime
		        print starttime
			#get data from WindPy
			if ktype<7:
				wsdata =w.wsi(windcode, "open,high,low,close,KDJ,MACD,EXPMA,MA",
					starttime, 		endtime,
				    "KDJ_N=9;KDJ_M1=3; KDJ_M2=3;KDJ_IO=0;"
					  "MACD_L=26;MACD_S=12;MACD_N=9;MACD_IO=0;EXPMA_N=12;MA_N=5;BarSize="+str(Barsize)+";Fill=Previous;PriceAdj=F")
				wsi =w.wsi(windcode, "KDJ,BOLL,EXPMA,MA",
					starttime, 		endtime,
					"KDJ_N=21;KDJ_M1=9; KDJ_M2=9;KDJ_IO=0;BOLL_N=26;BOLL_Width=2;BOLL_IO=0;EXPMA_N=26;MA_N=60;BarSize="+str(Barsize)+";Fill=Previous;PriceAdj=F")
			else:
				wsdata = w.wsd(windcode, "open,high,low,close,KDJ,MACD,EXPMA,MA",
					starttime,endtime,
					"KDJ_N=9;KDJ_M1=3; KDJ_M2=3;KDJ_IO=0;MACD_L=26;MACD_S=12;MACD_N=9;MACD_IO=0;EXPMA_N=12;MA_N=5;Period="+str(Barsize)+";Fill=Previous;PriceAdj=F")
				wsi = w.wsd(windcode, "KDJ,BOLL,EXPMA,MA",
					starttime, endtime,
					"KDJ_N=21;KDJ_M1=9; KDJ_M2=9;KDJ_IO=0;BOLL_N=26;BOLL_Width=2;BOLL_IO=0;EXPMA_N=26;MA_N=60;Period="+str(Barsize)+";Fill=Previous;PriceAdj=F")
			if wsdata.ErrorCode !=0:
				continue
			for i in range(0, len(wsi.Data) ):
				wsdata.Data.append(wsi.Data[i])
			for i in [4,6,9,13,18,24]:
				if ktype<7:
					wsi =w.wsi(windcode, "EXPMA", starttime, endtime,
					    "EXPMA_N="+str(i)+";BarSize="+str(Barsize)+";Fill=Previous;PriceAdj=F")
				else:
					wsi = w.wsd(windcode, "EXPMA",    starttime,    endtime,
					"EXPMA_N="+str(i)+";Period="+str(Barsize)+";Fill=Previous;PriceAdj=F")
				wsdata.Data.append(wsi.Data[0])
			#data to liefeng database
			insert_num = 0
			for wtime in wsdata.Times: 
				index  = wsdata.Times.index(wtime)
				history_data = {
					'code'		:windcode,
					'time'		:wtime - timedelta(microseconds=wtime.microsecond),					
				}
				
				for i in range(0,len(kl)):
					history_data[ kl[i] ] = NanToZero(wsdata.Data[i][index])				
				cursor_history.execute(insert_sql,history_data)
				insert_num = insert_num +1
			print insert_num
			cnx.commit()
	        print  str(Barsize) 
	        print (datetime.now()-btime).seconds
	        cursor_history.close() 
	        cursor.close()
	        cnx.close()
		w.stop()
	
for i in [2,5,6,8]:
	FuturesHistoryUpdate(i)







