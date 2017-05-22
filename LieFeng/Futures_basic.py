from WindPy import *
import mysql.connector
cnx = mysql.connector.connect(user='root',password='toortoor',host='127.0.0.1',database='LieFeng',buffered=True)
cursor = cnx.cursor()
up = cnx.cursor()
cursor.execute("select finance_mic,tradecode from futures")
w.start()
for (mic,code) in cursor:
	windcode = "{}.{}".format(code,mic)
	M= w.wss(windcode,"sccode,punit,tunit,mfprice,cdmonths")
	sql = ("UPDATE futures  SET sec_name = %s, punit = %s, tunit = %s, mfprice =%s, cdmonths =%s WHERE  finance_mic='"+mic+"' and tradecode='"+code+"' ")
	mt =  (M.Data[0][0],M.Data[1][0],M.Data[2][0],M.Data[3][0],M.Data[4][0])
	#print sql
	up.execute(sql , mt)

up.close()
cursor.close()
cnx.commit()
w.stop()
print "Futures basic Infomation End Success!!"

