from LieFeng import Slice
import FourIndicator
import Stocks_Update

import  mysql.connector

def main():
	cnx = mysql.connector.connect(**config)
	cnx.set_converter_class(NumpyMySQLConverter)
	#set windcode
	windcode_set = ["000001.SH","512500.SH","000905.SH","399006.SZ","399005.SZ","399905.SZ"]
	MarketType = 1
	#update only open,high,low,close
	StocksHistoryUpdate(windcode_set ,0,base_days=365*3)

	#slice and techincal analysis
	ktype_list = [2,4,5,6]
	for ktype in ktype_list :
		for windcode in windcode_set :
			SliceMinuteUpdate(windcode,ktype_list,cnx,MarketType,CalcTechicalAnalysis)
	
	#
	cnx.close()

if __name__ == '__main__':
	main()