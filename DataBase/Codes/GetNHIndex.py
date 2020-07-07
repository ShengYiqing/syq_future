import tushare as ts

pro = ts.pro_api()
instruments = [
               'A', 'AG', 'AL', 'AP', 'AU',
               'BU',
               'C', 'CF', 'CS', 'CU',
               'FG', 'FU',
               'HC',
               'I',
               'J', 'JD', 'JM',
               'L',
               'M', 'ME',
               'NI',
               'P', 'PB', 'PP',
               'RB', 'RM', 'RO', 'RU',
               'SC', 'SN', 'SP', 'SR',
               'TA', 'TC',
               'V',
               'Y',
               'ZN',
               ]

for instrument in instruments:
    pro.index_daily(ts_code='%s.NH'%instrument).set_index('trade_date').sort_index().to_csv('../NHIndex/%s.csv'%instrument)