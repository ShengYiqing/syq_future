from tqsdk import TqApi
import datetime
import numpy as np
import pandas as pd
import os
import sys
import time
minute = int(sys.argv[1])
api = TqApi()
#获取合约列表
CONTRACT_LIST = [
				'CFFEX.IC', 'CFFEX.IF', 'CFFEX.IH',
				'SHFE.cu', 'SHFE.al', 'SHFE.zn', 'SHFE.pb', 'SHFE.ni', 'SHFE.sn',
				'SHFE.au', 'SHFE.ag',
				'SHFE.rb', 'SHFE.hc', 'SHFE.ss',
				'SHFE.fu', 'SHFE.bu', 'SHFE.ru', 'SHFE.sp', 
				'DCE.m', 'DCE.y', 'DCE.a', 'DCE.b', 'DCE.p', 'DCE.c', 'DCE.cs', 'DCE.jd',
				'DCE.l', 'DCE.v', 'DCE.pp', 'DCE.eg', 'DCE.eb',
				'DCE.j', 'DCE.jm', 'DCE.i',
				'CZCE.SR', 'CZCE.CF', 'CZCE.OI', 'CZCE.RM', 'CZCE.AP', 'CZCE.CJ',
				'CZCE.TA', 'CZCE.MA', 'CZCE.FG', 'CZCE.ZC', 'CZCE.SF', 'CZCE.SM', 'CZCE.UR', 'CZCE.SA',
				]
#遍历合约列表
for contract in CONTRACT_LIST:
	#查询本地是否有文件
	if os.path.exists('../MinuteData/%s_%s.csv'%(contract, minute)):
		#有，读取
		data = pd.read_csv('../MinuteData/%s_%s.csv'%(contract, minute), index_col=0, parse_dates = [0])
		n = 100
		last_day = data.index[-1]
	else:
		#没有，生成空表
		n = 8964
		data = pd.DataFrame(columns=['OPEN', 'HIGH', 'LOW', 'CLOSE', 'VOLUME'])
		data.index.name = 'DATETIME'
		last_day = None
	#从tq取数，整理
	k = api.get_kline_serial('KQ.m@%s'%contract, 60*minute, n)
	k = k.loc[k.datetime > 0]
	k.index = [datetime.datetime.fromtimestamp(k.iloc[i]["datetime"] / 1e9) for i in range(len(k))]
	k.index.name = 'DATETIME'
	k = k.loc[:, ['open', 'high', 'low', 'close', 'volume']]
	k.columns = ['OPEN', 'HIGH', 'LOW', 'CLOSE', 'VOLUME']
	if last_day != None:
		k = k.loc[k.index>last_day, :]
	#两表合并
	ret = pd.concat([data, k], axis=0)
	
	#写入
	ret.to_csv('../MinuteData/%s_%s.csv'%(contract, minute))
api.close()