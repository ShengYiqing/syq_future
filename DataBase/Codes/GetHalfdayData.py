from tqsdk import TqApi
import datetime
import numpy as np
import pandas as pd
import os
import sys
import time
minute = 30
api = TqApi()
#获取合约列表
CONTRACT_LIST = [
				#'CFFEX.IC', 'CFFEX.IF', 'CFFEX.IH',
				#'SHFE.cu', 'SHFE.al', 'SHFE.zn', 'SHFE.pb', 'SHFE.ni', 'SHFE.sn',
				#'SHFE.au', 'SHFE.ag',
				'SHFE.rb', 'SHFE.hc', 'SHFE.ss',
				'SHFE.fu', 'SHFE.bu', 'SHFE.ru', 'SHFE.sp', 
				'DCE.m', 'DCE.y', 'DCE.a', 'DCE.b', 'DCE.p',
				#'DCE.c', 'DCE.cs', 'DCE.jd',
				'DCE.l', 'DCE.v', 'DCE.pp', 'DCE.eg', 'DCE.eb',
				'DCE.j', 'DCE.jm', 'DCE.i',
				'CZCE.SR', 'CZCE.CF', 'CZCE.OI', 'CZCE.RM', 'CZCE.AP', 'CZCE.CJ',
				'CZCE.TA', 'CZCE.MA', 'CZCE.FG', 'CZCE.ZC', 'CZCE.SF', 'CZCE.SM', 'CZCE.UR', 'CZCE.SA',
				]
#遍历合约列表
for contract in CONTRACT_LIST:
	#查询本地是否有文件
	if os.path.exists('../HalfdayData/%s_halfday.csv'%contract):
		#有，读取
		data = pd.read_csv('../HalfdayData/%s_halfday.csv'%contract, index_col=0, parse_dates = [0])
		n = 100
		last_stamp = data.index[-1]
	else:
		#没有，生成空表
		n = 8964
		data = pd.DataFrame(columns=['OPEN', 'HIGH', 'LOW', 'CLOSE', 'VOLUME'])
		data.index.name = 'DATETIME'
		last_stamp = None
	#从tq取数，整理
	k = api.get_kline_serial('KQ.m@%s'%contract, 60*minute, n)
	k = k.loc[k.datetime > 0]
	k.index = [datetime.datetime.fromtimestamp(k.iloc[i]["datetime"] / 1e9) for i in range(len(k))]
	k.index.name = 'DATETIME'
	k = k.loc[:, ['open', 'high', 'low', 'close', 'volume']]
	k.columns = ['OPEN', 'HIGH', 'LOW', 'CLOSE', 'VOLUME']

	k.loc[:, 'day'] = k.index
	k.loc[:, 'time'] = k.index
	k.day = [i.strftime('%Y-%m-%d %H:%M:%S').split(' ')[0] for i in k.day]
	k.time = [i.strftime('%Y-%m-%d %H:%M:%S').split(' ')[1] for i in k.time]
	t1 = '09:00:00'
	t2 = '13:30:00'
	t3 = '15:00:00'
	t4 = '21:00:00'
	k.loc[:, 't'] = [t1 if t1 <= k.time[i] < t2 else t2 if t2 <= k.time[i] < t3 else t4 for i in range(len(k))] 
	k.loc[:, 'GROUP'] = [k.day[i] + ' ' + k.t[i] for i in range(len(k))]
	OPEN = k.loc[:, ['OPEN', 'GROUP']].groupby('GROUP').first()
	HIGH = k.loc[:, ['HIGH', 'GROUP']].groupby('GROUP').max()
	LOW = k.loc[:, ['LOW', 'GROUP']].groupby('GROUP').min()
	CLOSE = k.loc[:, ['CLOSE', 'GROUP']].groupby('GROUP').last()
	VOLUME = k.loc[:, ['VOLUME', 'GROUP']].groupby('GROUP').sum()
	k_new = pd.DataFrame(index=OPEN.index)
	k_new.loc[:, 'OPEN'] = OPEN.iloc[:,0]
	k_new.loc[:, 'HIGH'] = HIGH.iloc[:,0]
	k_new.loc[:, 'LOW'] = LOW.iloc[:,0]
	k_new.loc[:, 'CLOSE'] = CLOSE.iloc[:,0]
	k_new.loc[:, 'VOLUME'] = VOLUME.iloc[:,0]
	if last_stamp != None:
		data = data.loc[data.index < k_new.index[0]]
		#k_new = k_new.loc[k_new.index>last_stamp.strftime('%Y-%m-%d %H:%M:%S'), :]
	#两表合并
	ret = pd.concat([data, k_new], axis=0)
	
	#写入
	ret.to_csv('../HalfdayData/%s_halfday.csv'%contract)
api.close()
