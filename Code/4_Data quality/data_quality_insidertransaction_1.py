import pymongo
import json
from pymongo import MongoClient
from connection_to_atlas import getclient

client = getclient()
db = client["Raw_data"]
collection = db["Insider_Trading"]

pipeline = [
    # Salver solo le date di interesse
    {
        "$project": {
            "_id": 1,
            "Date_transactions": 1,
            "Date_publication": 1
        }
    },
    # Aggiungere i controlli per le date 'NA' e il confronto tra Date_transactions e Date_publication
    {
        "$addFields": {
            "is_transaction_na": { "$eq": ["$Date_transactions", "NA"] },
            "is_publication_na": { "$eq": ["$Date_publication", "NA"] },
            "is_transaction_before_publication": {
                "$cond": {
                    "if": {
                        "$and": [
                            { "$ne": ["$Date_transactions", "NA"] },
                            { "$ne": ["$Date_publication", "NA"] }
                        ]
                    },
                    "then": { "$lt": [
                        { "$dateFromString": { "dateString": "$Date_transactions" } },
                        { "$dateFromString": { "dateString": "$Date_publication" } }
                    ] },
                    "else": False
                }
            }
        }
    },
    # Sommare i risultati: totale, validi, falliti e con NA
    {
        "$group": {
            "_id": None,  
            "total_checks": { "$sum": 1 },
            "valid_checks": {
                "$sum": {
                    "$cond": [
                        { "$and": [
                            { "$ne": ["$Date_transactions", "NA"] },
                            { "$ne": ["$Date_publication", "NA"] },
                            "$is_transaction_before_publication"
                        ] },
                        1, 0
                    ]
                }
            },
            "failed_checks": {
                "$sum": {
                    "$cond": [
                        { "$or": [
                            { "$eq": ["$Date_transactions", "NA"] },
                            { "$eq": ["$Date_publication", "NA"] },
                            { "$not": "$is_transaction_before_publication" }
                        ] },
                        1, 0
                    ]
                }
            },
            "na_checks": {
                "$sum": {
                    "$cond": [
                        { "$or": [
                            { "$eq": ["$Date_transactions", "NA"] },
                            { "$eq": ["$Date_publication", "NA"] }
                        ] },
                        1, 0
                    ]
                }
            }
        }
    }
]

# Esegui la pipeline
result = list(collection.aggregate(pipeline))

# Stampa il riassunto delle metriche
if result:
    summary = result[0]  # Primo (e unico) risultato della pipeline
    print(f"Totale controlli effettuati: {summary['total_checks']}")
    print(f"Controlli validi: {summary['valid_checks']}")
    print(f"Controlli falliti: {summary['failed_checks']}")
    print(f"Controlli con 'NA' nelle date: {summary['na_checks']}")
else:
    print("Nessun documento trovato.")
