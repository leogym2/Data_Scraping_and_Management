import pymongo
from pymongo import MongoClient
from connection_to_atlas import getclient
from datetime import datetime, timedelta

# Connessione a MongoDB Atlas
client = getclient()
db = client["Raw_data"]
collection = db["Stock"]

# La pipeline per estrarre e ordinare le date, con la data direttamente nel filtro
pipeline = [
    {"$unwind": "$Time Series (Daily)"},  # Estrai ogni documento dall'array
    {"$match": {"Time Series (Daily).date": {"$gte": "2020-01-01"}}},  # Filtro per prendere solo dal 01/01/2020 in poi
    {"$project": {"date": "$Time Series (Daily).date"}},  # Estrai il campo 'date' come stringa
    {"$sort": {"date": 1}}  # Ordina per 'date' in ordine crescente
]

date_mancanti_tot = 0
conta_date_tot = 0

# Ciclare su ogni documento (ogni stock) usando _id per filtrare
for document in collection.find():  # Itera su ogni documento
    # Applica la pipeline solo su questo documento corrente, utilizzando il suo _id
    result = list(collection.aggregate([
        {"$match": {"_id": document["_id"]}},  # Filtra per il documento corrente
        *pipeline  # Aggiungi la pipeline per l'aggregazione
    ], allowDiskUse=True))

    # Estrai tutte le date dal risultato
    dates_in_document = [item["date"] for item in result]
    total_dates = len(dates_in_document)
    conta_date_tot += total_dates  # Incrementa il totale delle date

    missing_dates = []  # Lista per le date mancanti

    # Itera attraverso le date per verificare se ci sono date mancanti
    for i in range(len(dates_in_document) - 1):
        current_date = datetime.strptime(dates_in_document[i], "%Y-%m-%d")
        next_date = datetime.strptime(dates_in_document[i + 1], "%Y-%m-%d")
        
        # Calcola la data successiva prevista
        expected_next_date = current_date + timedelta(days=1)
        
        # Se la data successiva non è quella prevista, aggiungi la data mancante
        if expected_next_date != next_date:
            missing_dates.append(expected_next_date.strftime("%Y-%m-%d"))
    
    # Incrementa il totale delle date mancanti
    date_mancanti_tot += len(missing_dates)

# Stampa i totali
print(f"Numero totale di date: {conta_date_tot}")
print(f"Numero totale di date mancanti: {date_mancanti_tot}")