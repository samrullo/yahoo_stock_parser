from header import *

ticker="7259.T"
start = datetime.date(2020, 5, 1)
end = datetime.date(2020, 8, 6)
yahoo_parser = YahooStockParser(ticker, start, end)
eq_px_df = yahoo_parser.get_all_stock_dataframe()
