import pymongo
from pymongo import MongoClient
from connection_to_atlas import getclient
from datetime import datetime, timedelta

# Connessione a MongoDB Atlas
client = getclient()
db = client["Raw_data"]
collection = db["Stock"]

# Inizializza il contatore degli errori
error_count = 0

# Inizializza il contatore degli errori
error_count = 0

# Esegui la pipeline direttamente su tutti i documenti nella collection
pipeline = [
    {"$unwind": "$Time Series (Daily)"},  # Estrai i dati dall'array
    {"$project": {
        "high": {"$toDouble": "$Time Series (Daily).high"},
        "low": {"$toDouble": "$Time Series (Daily).low"},
        "date": "$Time Series (Daily).date"
    }},
    {"$match": {
        "$expr": {
            "$lte": ["$high", "$low"]  # Verifica se high è minore o uguale a low
        }
    }},
]

# Esegui la pipeline con aggregazione
result = list(collection.aggregate(pipeline, allowDiskUse=True))

# Conta il numero di errori
error_count = len(result)

# Stampa il numero di errori trovati e le informazioni sugli errori
print(f"Numero di errori trovati: {error_count}")

# Stampa i dettagli degli errori
for item in result:
    print(f"Errore trovato nella data {item['date']}: high ({item['high']}) non e' maggiore di low ({item['low']})")
