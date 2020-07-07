import tushare as ts
pro = ts.pro_api()
exchanges = ['SSE', 'SZSE', 'CFFEX', 'SHFE', 'CZCE', 'DCE', 'INE']
for exchange in exchanges:
    trade_cal = pro.trade_cal(exchange=exchange)
    trade_cal.cal_date = ['%s-%s-%s'%(i[:4], i[4:6], i[6:]) for i in trade_cal.cal_date]
    trade_cal.set_index('cal_date').to_csv('../TradeCal/%s.csv'%trade_cal.iloc[0,0])