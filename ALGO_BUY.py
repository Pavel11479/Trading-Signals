import time
import gspread 
from oauth2client.service_account import ServiceAccountCredentials
from alpha_vantage.timeseries import TimeSeries
from pprint import pprint
import pandas as pd
import numpy as np
from ib.opt import Connection, message
from ib.ext.Contract import Contract
from ib.ext.Order import Order


def error_handler(msg):
    print("Server Error:", msg)

def server_handler(msg):
    print("Server Msg:", msg.typeName, "-", msg)

def make_contract(symbol, sec_type, exch, prim_exch, curr):
    Contract.m_symbol = symbol
    Contract.m_secType = sec_type
    Contract.m_exchange = exch
    Contract.m_primaryExch = prim_exch
    Contract.m_currency = curr
    return Contract

def make_order(action, quantity):
        order = Order()
        order.m_orderType = 'MKT'         
        order.m_totalQuantity = quantity
        order.m_action = action        
        return order

while True:
    scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
    credentials = ServiceAccountCredentials.from_json_keyfile_name('C:/Users/pavel/Downloads/TradingSignals-0afb3f18df64.json', scope)
    gc = gspread.authorize(credentials)
    wks = gc.open('Tradingsignals').sheet1   
    n = 1 
    
    while True:
        
        if wks.cell(n,1).value not in(''):
            
            if wks.cell(n,3).value in('BUY'):
                ts = TimeSeries(key='H3YG4J5IUAE6CUSA', output_format='pandas')
                data60, meta_data = ts.get_intraday(symbol=wks.cell(n,2).value, interval='60min', outputsize='compact')
                data60.columns = ['open','high','low','close','vol']
                span = 20
                sma = data60.open.rolling(window=span, min_periods=20).mean()[:span]
                rest = data60.open[span:]
                ema20 = pd.concat([sma,rest]).ewm(span=span, adjust=False).mean()
                span1 = 50
                sma1 = data60.close.rolling(window=span1, min_periods=50).mean()[:span1]
                rest1 = data60.close[span1:]
                ema50 = pd.concat([sma1,rest1]).ewm(span=span1, adjust=False).mean()
            
                if ema20[-1] > ema50[-2]:
                    data15, meta_data = ts.get_intraday(symbol=wks.cell(n,2).value, interval='15min', outputsize='compact')
                    data15.columns = ['open','high','low','close','vol']
            
                    if data15.close[-1] < 1.005*(data15.close[-2]): 
                        data5, meta_data = ts.get_intraday(symbol=wks.cell(n,2).value, interval='5min', outputsize='full')
                        data5.columns = ['open','high','low','close','vol']
                        if data5.vol[-1]+data5.vol[-2] > 3*(np.mean(data5.vol[-224:-3])):
                            if __name__ == "__main__":
                                conn = Connection.create(port=7497, clientId=11479)
                                conn.connect()
                                conn.register(error_handler, 'Error')
                                conn.registerAll(server_handler)
                                oid = int(n)+60
                                cont = make_contract(wks.cell(n,2).value, 'STK', 'SMART', 'SMART', 'USD')
                                offer = make_order(wks.cell(n,5).value, int(wks.cell(n,4).value))
                                conn.placeOrder(oid, cont, offer)
                                conn.disconnect()
                            
            n = n+1
        time.sleep(2)

