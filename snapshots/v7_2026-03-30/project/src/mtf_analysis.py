import yfinance as yf

def get_mtf_data(symbol):

    tf = {}

    tf["5m"] = yf.download(symbol,period="5d",interval="5m")
    tf["15m"] = yf.download(symbol,period="5d",interval="15m")
    tf["1h"] = yf.download(symbol,period="30d",interval="1h")
    tf["1d"] = yf.download(symbol,period="1y",interval="1d")

    return tf