db_config = {
  'user': 'root',
  'password': 'ypt@66601622',
  'host': '10.15.6.108',
  # 'host': '119.164.253.141',
  'database': 'liefeng',
  'port' : 3306,
  #'raise_on_warnings': True,
  'buffered':True,
}

BreakVolumePermitDays = 10
BreakCentralPermitDays = 10

SellingEnable = False

Release_Port = 3307
Develop_Port = 3309

Warning_Release_TableName='stocks_warning'
Warning_Develop_TableName='stocks_warning_develop'

Stocks_WorkingTime=[{'begin':'0930','end':'1130'},{'begin':'1300','end':'1500'}]
