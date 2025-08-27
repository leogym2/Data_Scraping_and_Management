import pymongo
import json
from pymongo import MongoClient
from connection_to_atlas import getclient

client = getclient()
db = client["Raw_data"]
collection = db["Stock_InsiderTrading_integrated"]

pipeline = [
    # Unwind per esplodere i dati dell'array 'transactions_from_lookup'
    { "$unwind": "$transactions_from_lookup" },
    { "$unwind": "$transactions_from_lookup.filtered_nonDerivativeTransaction" },

    # Estrai high e low dal livello superiore del documento
    {
        "$project": {
            "symbol":1,
            "high": 1,
            "low": 1,
            "transactions_from_lookup.Date_transactions": 1,
            "transactions_from_lookup.Entity_name_cleaned": 1,
            "transactions_from_lookup.filtered_nonDerivativeTransaction.transactionPricePerShare": 1,
            "_id": 0
        }
    },

    # Filtra i documenti dove 'transactionPricePerShare' è maggiore di 'high' o minore di 'low'
    {
        "$match": {
            "$expr": {
                "$or": [
                    { "$gte": ["$transactions_from_lookup.filtered_nonDerivativeTransaction.transactionPricePerShare", "$high"] },
                    { "$lte": ["$transactions_from_lookup.filtered_nonDerivativeTransaction.transactionPricePerShare", "$low"] }
                ]
            }
        }
    },

    # Filtra per eliminare i valori di 'transactionPricePerShare' che sono 0 o None
    { 
        "$match": {  
            "$and": [
                { "transactions_from_lookup.filtered_nonDerivativeTransaction.transactionPricePerShare": { "$ne": 0.0 } },
                { "transactions_from_lookup.filtered_nonDerivativeTransaction.transactionPricePerShare": { "$ne": None } }
            ]
        }
    },

    # Proietta i campi richiesti
    { 
        "$project": {
            "Stock": "$symbol",
            "Date": "$transactions_from_lookup.Date_transactions",
            "Entity": "$transactions_from_lookup.Entity_name_cleaned",
            "transactionPricePerShare": "$transactions_from_lookup.filtered_nonDerivativeTransaction.transactionPricePerShare",
            "low": "$low",
            "high": "$high"
        }
    }
]

# Esegui la query
result = list(collection.aggregate(pipeline))

for i in result:
    print(i) 

print(len(result))