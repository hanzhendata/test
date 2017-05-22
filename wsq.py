from WindPy import w
from datetime import time,datetime
import  mysql.connector

def WSQCallback(data):
	print data

w.start()
w.wsq("600000.SH,600007.SH", "rt_date,rt_time,rt_high,rt_low,rt_last,rt_last_amt,rt_last_vol,rt_latest,rt_vol,rt_amt,rt_chg,rt_pct_chg", func=WSQCallback)
w.stop()