import  numpy 
import  talib  
import  mysql.connector
import  pandas as pd
import  math
from Config import *
def myMACD(price, fastperiod=12, slowperiod=26, signalperiod=9):
    ewma12 = pd.ewma(price,span=fastperiod)
    ewma60 = pd.ewma(price,span=slowperiod)
    dif = ewma12-ewma60
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

#prepare database environment variable
cnx = mysql.connector.connect(**config)
cursor = cnx.cursor()
sql = "select open_price,high_price,low_price,close_price from stocks_history where prod_code='600000.SH' and ktype=0 order by date"
cursor.execute(sql)
openl = highl = lowl = closel = []
for open_price,high_price,low_price,close_price in cursor:
        openl.append(open_price)
        highl.append(high_price)
        lowl.append(low_price)
        closel.append(close_price)
        
#macd,macdsignal, macdhist = myMACD(pd.Series(closel)) #ema12 =
talib.EMA(numpy.array(closel,dtype= numpy.float64),12) #print ema12
myema(closel,12) 
#myema(closel,26) 
Limit = int(math.ceil(4*(12+1) ))

myema(closel[len(closel)-Limit:len(closel)],12)
macd,macdsignal, macdhist = talib.MACDEXT(numpy.array(closel,dtype= numpy.float),fastperiod = 12,fastmatype=1,slowperiod=26,slowmatype=1, signalperiod=9, signalmatype=1) 
print 'dif',macd 
print  'dea',macdsignal 
print  'macd',macdhist*2
