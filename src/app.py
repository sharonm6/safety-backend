from flask import Flask, jsonify, request
from flask_cors import CORS

import firebase_admin
from firebase_admin import credentials, firestore

import pandas as pd
import json

cred = credentials.Certificate("../service-account.json")
firebase_app = firebase_admin.initialize_app(cred)
db = firestore.client()

# flask setup
app = Flask(__name__)
CORS(app)

#routes
@app.route('/signup', methods=['POST'])
def signup():
    data = request.json
    user_id = data['user_id']
    email = data['email']
    name = data['name']
    gender = data['gender']
    age = data['age']
    descent = data['descent']
    
    data = {
        "user_id": user_id,
        "first": name,
        "email": email,
        "gender": gender,
        "age": age,
        "descent": descent
    }
    
    db.collection("users").document(user_id).set(data)
    
    return jsonify({'success': True, 'data':{'user_id': user_id}})

@app.route('/login', methods=['GET'])
def login():
    query = request.args.to_dict()
    email = query['emails']
    
    user_query = db.collection("users").where("email", "==", email).limit(1).get()

    if not user_query:
        return jsonify({'success': False})
    else:
        user_id = user_query[0].id
        return jsonify({'success': True, 'data':{'user_id': user_id}})

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

if __name__ == '__main__':
    insert_data()