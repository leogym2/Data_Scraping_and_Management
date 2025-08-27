import pandas as pd

data = pd.read_csv('nasdaq.csv')
data["Market Cap"] = pd.to_numeric(data["Market Cap"])
sorted_data = data.sort_values(by='Market Cap',ascending=False)
first_50_stock = sorted_data.iloc[:51] 
list_stock = first_50_stock["Symbol"].tolist()
list_stock.pop(4)

print(list_stock)