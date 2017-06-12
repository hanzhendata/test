#!/usr/bin/env python
# -*- coding: gb2312 -*-

import msvcrt
import sys
import TradeX

print "\t*******************************************************************"
print "\t*                        TradeX.dll v1.3.0                        *"
print "\t*                                                                 *"
print "\t*  TradeX.dll 股票程序化交易接口全新发布！                        *"
print "\t*                                                                 *"
print "\t*  版本描述：                                                     *"
print "\t*  1）支持普通账户和融资融券信用账户业务，包括下单、撤单、查询，  *"
print "\t*  融资融券等业务；                                               *"
print "\t*  2）批量多连接下单和多账户同时下单，每秒批量下单可达数百单；    *"
print "\t*  3）支持股票五档、Level 2十档实时行情以及期货等扩展行情，允     *"
print "\t*  许同时批量多连接取行情；                                       *"
print "\t*  4）直连交易服务器和行情服务器，无中转，安全、稳定，实盘运行中；*"
print "\t*  5）全新C++，C#，Python，Delphi，Java，易语言等语言接口；       *"
print "\t*  6）完美兼容trade.dll，彻底解决华泰服务器的连接问题。           *"
print "\t*                                                                 *"
print "\t*  技术QQ群：318139137  QQ：3048747297                            *"
print "\t*  技术首页：https://tradexdll.com/                               *"
print "\t*  http://pan.baidu.com/s/1jIjYq1K                                *"
print "\t*                                                                 *"
print "\t*******************************************************************"

print "\n\t按任意键继续...\n"
msvcrt.getch()

#
#
#
print "\t1、初始化...\n"
TradeX.OpenTdx()

#
#
#
print "\t2、登录交易服务器...\n"

# 交易服务器主机
sHost = "mock.tdx.com.cn"
nPort = 7708
sVersion = "6.40"
sBranchID = 9000
sAccountNo = "net828@163.com"
sTradeAccountNo = "001001001005792"
sPassword = "123123"
sTxPassword = ""

try:
	client = TradeX.Logon(sHost, nPort, sVersion, sBranchID, sAccountNo, sTradeAccountNo, sPassword, sTxPassword)
except TradeX.error, e:
	print "error: " + e.message
	sys.exit(-1)

print "\n\t成功登录\n"

print "\n\t按任意键继续...\n"
msvcrt.getch()

#
#
#
print "\t3、查询资金...\n"

nCategory = 0

status, content = client.QueryData(nCategory)
if status < 0:
    print "error: " + content
else:
	print content

print "\n\t按任意键继续...\n"
msvcrt.getch()

#
#
#
print "\t4、查询股份...\n"

nCategory = 1

status, content = client.QueryData(nCategory)
if status < 0:
    print "error: " + content
else:
	print content

print "\n\t按任意键继续...\n"
msvcrt.getch()

#
#
#
print "\t5、查询可交易股票数量...\n"

nCategory = 0
nPriceType = 0
sAccount = ""
sStockCode = "000002"
fPrice = 3.11

status, content = client.GetTradableQuantity(nCategory, nPriceType, sAccount, sStockCode, fPrice)
if status < 0:
    print "error: " + content
else:
	print content

print "\n\t按任意键继续...\n"
msvcrt.getch()

#
#
#
print "\t6、一键新股申购...\n"

status = client.QuickIPO()
if status < 0:
    print "error: " + str(status)
else:
	print "ok: " + str(status)

#
#
#
print "\t7、新股申购明细...\n"

status, content = client.QuickIPODetail()
if status < 0:
    print "error: " + content
elif status == 0:
	print content
else:
	for elem in content:
		errinfo, result = elem
		if errinfo != "":
			print errinfo
		else:
			print result

print "\n\t按任意键继续...\n"
msvcrt.getch()

#
#
#
print "\t8、委托...\n"

status, content = client.SendOrder(0, 4, "p001001001005793", "601988", 0, 100)
if status < 0:
    print "error: " + content
else:
	print content

print "\n\t按任意键继续...\n"
msvcrt.getch()

#
#
#
print "\t9、批量委托...\n"

status, content = client.SendOrders(((0, 0, "p001001001005793", "601988", 3.11, 100), (0, 0, "p001001001005793", "601988", 3.11, 200)))
if status < 0:
	print content
else:
	for elem in content:
		errinfo, result = elem
		if errinfo != "":
			print errinfo
		else:
			print result

print "\n\t按任意键继续...\n"
msvcrt.getch()

#
#
#
print "\t10、查询五档行情...\n"

status, content = client.GetQuotes(('000001', '600600'))
if status < 0:
	print content
else:
	for elem in content:
		errinfo, result = elem
		if errinfo != "":
			print errinfo
		else:
			print result

print "\n\t按任意键继续...\n"
msvcrt.getch()

#
#
#
print "\t11、查询资金、持仓...\n"

Category = (0, 1, 3)

status, content = client.QueryDatas(Category)
if status < 0:
	print content
else:
	for elem in content:
		errinfo, result = elem
		if errinfo != "":
			print errinfo
		else:
			print result

print "\n\t按任意键继续...\n"
msvcrt.getch()

#
#
#
print "\t12、关闭服务器连接...\n"

del client
TradeX.CloseTdx()

#
#
#
print "\t按任意键退出...\n"

msvcrt.getch()

