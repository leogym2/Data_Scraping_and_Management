import pymongo
from datetime import datetime
import requests
import json
from pymongo import MongoClient
from connection_to_atlas import getclient
from Stocklist import list_stock
 
client = getclient()
db = client["Raw_data"]
collection = db["Stock"] 

rename_map_meta_data = {
    "1. Information": "Information",
    "2. Symbol": "Symbol",
    "3. Last Refreshed": "Last_Refreshed",
    "4. Output Size": "Output_Size",
    "5. Time Zone": "Time_Zone"
}
rename_map = {
    "1. open": "open",
    "2. high": "high",
    "3. low": "low",
    "4. close": "close",
    "5. volume": "volume"
}

Stock = list_stock[25:]

for S in Stock:
    url = 'https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol='+S+'&apikey=HU9LUETZI0DJ01DG&outputsize=full'
    r = requests.get(url)
    newdata = r.json()

    newdata["Meta Data"] = {rename_map_meta_data.get(k, k): v for k, v in newdata["Meta Data"].items()}

    time_series = []

    for date, values in newdata["Time Series (Daily)"].items():
        # Aggiungi sia la data come stringa che la isodate
        date_iso = datetime.strptime(date, "%Y-%m-%d")
        values['date'] = date  # Aggiungi la data come stringa
        values['isodate'] = date_iso  # Aggiungi la data come isodate (oggetto datetime)
        
        # Rinomina i campi secondo la mappa
        values = {rename_map.get(k, k): v for k, v in values.items()}
        
        # Aggiungi il dizionario della singola data al array
        time_series.append(values)

    # Aggiungi la "Time Series (Daily)" ristrutturata al documento principale
    newdata["Time Series (Daily)"] = time_series

    print(newdata["Meta Data"]["Symbol"]+"uploaded")
    collection.insert_one(newdata)

