import decimal
import  mysql.connector
from Config import *
#时间,开盘,最高,最低,收盘,成交量,EMA12,EMA26,DIF,DEA,MACD

cnx = mysql.connector.connect(**config)

def (prod_code,connect,connect):
	cursor = connect.cursor()
	ml = prod_code.split('.',1)
	print ml
	filename = ml[0]+'.'+'txt'

cursor = cnx.cursor()    
sql = " insert into  stocks_history where prod_code='000001.SH' and  ktype=0 and date='"
f = open('000001.txt','r')
fw = open('compare.txt','w')

for line in f.readlines():
         sl = line.split(',')
         cursor.execute(sql +sl[0] + "'")
	 if (cursor.rowcount!=0):
		  wind = cursor.fetchone()[0] 
		  fw.write(sl[0] + ' ' + str(decimal.Decimal(sl[4])-wind) + '\n' )
cursor.close()
cnx.close()
f.close()
fw.close()
		
