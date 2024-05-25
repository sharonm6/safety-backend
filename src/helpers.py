import json

import firebase_admin
from firebase_admin import credentials, firestore

cred = credentials.Certificate("../service-account.json")
app = firebase_admin.initialize_app(cred, name="helpers")
db = firestore.client(app=firebase_admin.get_app(name='helpers'))

def map_score(score):
    if score < 0.0006: 
        return 1
    elif score < 0.0012:
        return 2
    elif score < 0.0018:
        return 3
    elif score < 0.0024:
        return 4
    else:
        return 5

def insert_data():
    print('Loading data.json')
    records = None
    with open('../data/db_data.json', encoding="utf-8") as json_file:  
        records = json.load(json_file)
    print('%i records read from file' % len(records))

    i = 0
    batch = db.batch()
    print('Writing records to Firestore')
    for record_id in records.keys():
        doc = db.collection('districts').document(record_id)
        batch.set(doc, records[record_id])
        i += 1
        if (i % 500 == 0):
            batch.commit()
            batch = db.batch()
            print(i)
    batch.commit()
    print(i)
