import pymongo
import json
from pymongo import MongoClient
from connection_to_atlas import getclient

client = getclient()
db = client["Raw_data"]
collection = db["Stock_InsiderTrading_integrated"]

# Pipeline per contare documenti con array vuoto 'transactions_from_lookup' e campi nulli dentro l'array
pipeline = [
    # 1. Proiezione per verificare l'esistenza di un array vuoto in 'transactions_from_lookup'
    {
        "$project": {
            "transactions_from_lookup_empty": {
                "$cond": [
                    {"$eq": [{"$type": "$transactions_from_lookup"}, "array"]},  # Verifica se è un array
                    {"$eq": [{"$size": "$transactions_from_lookup"}, 0]},  # Verifica se l'array è vuoto
                    False  # Se non è un array, considera come non vuoto
                ]
            },
            # Conta i campi nulli in ogni documento (incluso il controllo per i valori nulli)
            "null_fields_count": {
                "$sum": [
                    {"$cond": [{"$eq": [{"$type": "$symbol"}, "missing"]}, 1, 0]},
                    {"$cond": [{"$eq": [{"$type": "$date"}, "missing"]}, 1, 0]},
                    {"$cond": [{"$eq": [{"$type": "$close"}, "missing"]}, 1, 0]},
                    {"$cond": [{"$eq": [{"$type": "$open"}, "missing"]}, 1, 0]},
                    {"$cond": [{"$eq": [{"$type": "$high"}, "missing"]}, 1, 0]},
                    {"$cond": [{"$eq": [{"$type": "$low"}, "missing"]}, 1, 0]},
                    {"$cond": [{"$eq": [{"$type": "$volume"}, "missing"]}, 1, 0]},
                    {"$cond": [{"$eq": [{"$type": "$change_close_next_1_day"}, "missing"]}, 1, 0]},
                    {"$cond": [{"$eq": [{"$type": "$change_close_next_1_day"}, "null"]}, 1, 0]},  
                    {"$cond": [{"$eq": [{"$type": "$change_close_next_5_days"}, "missing"]}, 1, 0]},
                    {"$cond": [{"$eq": [{"$type": "$change_close_next_5_days"}, "null"]}, 1, 0]}, 
                    {"$cond": [{"$eq": [{"$type": "$change_close_next_15_days"}, "missing"]}, 1, 0]},
                    {"$cond": [{"$eq": [{"$type": "$change_close_next_15_days"}, "null"]}, 1, 0]},
                    {"$cond": [{"$eq": [{"$type": "$change_volume_next_1_day"}, "missing"]}, 1, 0]},
                    {"$cond": [{"$eq": [{"$type": "$change_volume_next_1_day"}, "null"]}, 1, 0]}, 
                    {"$cond": [{"$eq": [{"$type": "$change_volume_next_5_days"}, "missing"]}, 1, 0]},
                    {"$cond": [{"$eq": [{"$type": "$change_volume_next_5_days"}, "null"]}, 1, 0]},  
                    {"$cond": [{"$eq": [{"$type": "$change_volume_next_15_days"}, "missing"]}, 1, 0]},
                    {"$cond": [{"$eq": [{"$type": "$change_volume_next_15_days"}, "null"]}, 1, 0]},  
                ]
            },
            # Se l'array 'transactions_from_lookup' non è vuoto, possiamo anche contare i campi al suo interno
            "null_fields_in_array": {
                "$cond": [
                    {"$eq": [{"$type": "$transactions_from_lookup"}, "array"]},
                    {
                        "$sum": [
                            {"$cond": [{"$eq": [{"$type": "$transactions_from_lookup.Date_transactions"}, "missing"]}, 1, 0]},
                            {"$cond": [{"$eq": [{"$type": "$transactions_from_lookup.Date_transactions"}, "null"]}, 1, 0]}  # Aggiunto controllo per null
                        ]
                    },
                    0  # Se non è un array, non conteggiare nulla
                ]
            },
            # Aggiungiamo il totale dei campi per ogni documento
            "total_fields_count": {
                "$sum": [
                    {"$cond": [{"$eq": [{"$type": "$symbol"}, "missing"]}, 0, 1]},
                    {"$cond": [{"$eq": [{"$type": "$date"}, "missing"]}, 0, 1]},
                    {"$cond": [{"$eq": [{"$type": "$close"}, "missing"]}, 0, 1]},
                    {"$cond": [{"$eq": [{"$type": "$open"}, "missing"]}, 0, 1]},
                    {"$cond": [{"$eq": [{"$type": "$high"}, "missing"]}, 0, 1]},
                    {"$cond": [{"$eq": [{"$type": "$low"}, "missing"]}, 0, 1]},
                    {"$cond": [{"$eq": [{"$type": "$volume"}, "missing"]}, 0, 1]},
                    {"$cond": [{"$eq": [{"$type": "$change_close_next_1_day"}, "missing"]}, 0, 1]},
                    {"$cond": [{"$eq": [{"$type": "$change_close_next_1_day"}, "null"]}, 0, 1]},  # Aggiunto controllo per null
                    {"$cond": [{"$eq": [{"$type": "$change_close_next_5_days"}, "missing"]}, 0, 1]},
                    {"$cond": [{"$eq": [{"$type": "$change_close_next_5_days"}, "null"]}, 0, 1]},  # Aggiunto controllo per null
                    {"$cond": [{"$eq": [{"$type": "$change_close_next_15_days"}, "missing"]}, 0, 1]},
                    {"$cond": [{"$eq": [{"$type": "$change_close_next_15_days"}, "null"]}, 0, 1]},  # Aggiunto controllo per null
                    {"$cond": [{"$eq": [{"$type": "$change_volume_next_1_day"}, "missing"]}, 0, 1]},
                    {"$cond": [{"$eq": [{"$type": "$change_volume_next_1_day"}, "null"]}, 0, 1]},  # Aggiunto controllo per null
                    {"$cond": [{"$eq": [{"$type": "$change_volume_next_5_days"}, "missing"]}, 0, 1]},
                    {"$cond": [{"$eq": [{"$type": "$change_volume_next_5_days"}, "null"]}, 0, 1]},  # Aggiunto controllo per null
                    {"$cond": [{"$eq": [{"$type": "$change_volume_next_15_days"}, "missing"]}, 0, 1]},
                    {"$cond": [{"$eq": [{"$type": "$change_volume_next_15_days"}, "null"]}, 0, 1]},  # Aggiunto controllo per null
                    {"$cond": [{"$eq": [{"$type": "$transactions_from_lookup"}, "missing"]}, 0, 1]},  # Include anche l'array
                    {"$cond": [{"$eq": [{"$type": "$total_transactions"}, "missing"]}, 0, 1]}  # Include anche l'altro campo
                ]
            }
        }
    },
    # 2. Raggruppamento per ottenere il totale dei documenti e delle metriche
    {
        "$group": {
            "_id": None,
            "total_documents": {"$sum": 1},
            "documents_with_empty_array": {"$sum": {"$cond": [{"$eq": ["$transactions_from_lookup_empty", True]}, 1, 0]}},
            "total_null_fields": {"$sum": "$null_fields_count"},
            "total_null_fields_in_array": {"$sum": "$null_fields_in_array"},
            "total_fields": {"$sum": "$total_fields_count"}  # Aggiunto totale campi
        }
    }
]

# Esegui la pipeline
result = list(collection.aggregate(pipeline))

# Stampa i risultati
if result:
    result = result[0]
    print(f"Totale documenti: {result['total_documents']}")
    print(f"Documenti con array vuoto 'transactions_from_lookup': {result['documents_with_empty_array']}")
    print(f"Totale campi nulli (escluso array): {result['total_null_fields']}")
    print(f"Totale campi nulli dentro l'array 'transactions_from_lookup': {result['total_null_fields_in_array']}")
    print(f"Totale campi in tutti i documenti: {result['total_fields']}")
else:
    print("Nessun documento trovato.")
