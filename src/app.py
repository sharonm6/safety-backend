import helpers

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
        "age": int(age),
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

@app.route('/map', methods=['GET'])
def get_scores():
    query = request.args.to_dict()
    user_id = query['user_id']
    
    fetched_data = db.collection("districts").get()
    score_data = []
    
    if user_id != "N/A":
        user_query = db.collection("users").where("user_id", "==", user_id).limit(1).get()
        for district in fetched_data:
            user_data = user_query[0].to_dict()
            
            district_data = district.to_dict()
            dis_score_data = {}
            
            dis_score_data['lat'] = district_data['lat']
            dis_score_data['lon'] = district_data['lon']
            score = 0

            if user_data["gender"] == "female":
                score += district_data["woman_score"]
            elif user_data["gender"] == "male":
                score += district_data["man_score"]
            else:
                score += district_data["otherSex_score"]
            
            if user_data["age"] < 18:
                score += district_data["minor_score"]
            elif user_data["age"] < 25:
                score += district_data["earlyTwenty_score"]
            elif user_data["age"] < 30:
                score += district_data["lateTwenty_score"]
            elif user_data["age"] < 40:
                score += district_data["thirties_score"]
            elif user_data["age"] < 65:
                score += district_data["midlife_score"]
            elif user_data["age"] < 65:
                score += district_data["elderly_score"]
            
            if user_data["descent"] == "asian":
                score += district_data["asian_score"]
            elif user_data["descent"] == "black":
                score += district_data["black_score"]
            elif user_data["descent"] == "hispanic":
                score += district_data["hispanic_score"]
            elif user_data["descent"] == "white":
                score += district_data["white_score"]
            elif user_data["descent"] == "pacific":
                score += district_data["pacific_score"]
            elif user_data["descent"] == "indian":
                score += district_data["indian_score"]
            else:
                score += district_data["otherDescent_score"]

            dis_score_data['score'] = helpers.map_score(score * district_data['default_score'] * 2)
            
            score_data.append(dis_score_data)
    else:
        for district in fetched_data:
            district_data = district.to_dict()
            dis_score_data = {}
            
            dis_score_data['lat'] = district_data['lat']
            dis_score_data['lon'] = district_data['lon']
            dis_score_data['score'] = helpers.map_score(district_data['default_score'])
            
            score_data.append(dis_score_data)
    
    return jsonify({'success': True, 'data': score_data})

if __name__ == '__main__':
    pass