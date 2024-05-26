from flask import Flask, jsonify, request
from flask_cors import CORS

import psycopg2

import helpers
import uuid
from collections import defaultdict

CONNECTION = psycopg2.connect(
    dbname="haven",
    user="postgres",
    password="postgres",
    host="localhost",
    port="5432"
)

# Create a cursor to interact with the database
cursor = CONNECTION.cursor()

# flask setup
app = Flask(__name__)
CORS(app)

#routes
@app.route('/signup', methods=['POST'])
def signup():
    data = request.json
    user_id = uuid.uuid4().hex
    email = data['email']
    name = data['name']
    gender = data['gender']
    age = data['age']
    descent = data['descent']
    
    cursor.execute("INSERT INTO users(userid, name, email, gender, age, descent) VALUES('%s', '%s', '%s', '%s', '%s', '%s') RETURNING userid;" % (user_id, name, email, gender, age, descent))
    CONNECTION.commit()
    user_id = cursor.fetchone()
    
    return jsonify({'success': True, 'data':{'user_id': user_id}})

@app.route('/login', methods=['GET'])
def login():
    query = request.args.to_dict()
    email = query['email']
    
    cursor.execute("SELECT userid FROM users WHERE email = '%s';" % (email))
    user = cursor.fetchone()

    if not user:
        return jsonify({'success': False})
    else:
        user_id = user[0]
        return jsonify({'success': True, 'data':{'user_id': user_id}})

@app.route('/map', methods=['GET'])
def get_scores():
    query = request.args.to_dict()
    user_id = query['user_id']
    
    cursor.execute("SELECT * FROM crimes;")
    fetched_data = cursor.fetchall()
    score_data = []
    
    if user_id != "N/A":
        cursor.execute("SELECT userid FROM users WHERE userid = '%s';" % (user_id))
        user = cursor.fetchone()
        for district in fetched_data:
            user_data = user[0]
            
            district_data = district.to_dict()
            dis_score_data = {}
            
            dis_score_data['lat'] = district_data[18]
            dis_score_data['lon'] = district_data[19]
            score = 0

            if user_data[3] == "female":
                score += district_data[2]
            elif user_data[3] == "male":
                score += district_data[3]
            else:
                score += district_data[4]
            
            if user_data[4] < 18:
                score += district_data[12]
            elif user_data[4] < 25:
                score += district_data[13]
            elif user_data[4] < 30:
                score += district_data[14]
            elif user_data[4] < 40:
                score += district_data[15]
            elif user_data[4] < 65:
                score += district_data[16]
            elif user_data[4] < 65:
                score += district_data[17]
            
            if user_data[5] == "asian":
                score += district_data[5]
            elif user_data[5] == "black":
                score += district_data[6]
            elif user_data[5] == "hispanic":
                score += district_data[7]
            elif user_data[5] == "white":
                score += district_data[8]
            elif user_data[5] == "pacific":
                score += district_data[9]
            elif user_data[5] == "indian":
                score += district_data[10]
            else:
                score += district_data["otherDescent_score"]

            dis_score_data['score'] = helpers.map_score(score * district_data[1] * 2)
            
            score_data.append(dis_score_data)
    else:
        for district in fetched_data:
            district_data = district.to_dict()
            dis_score_data = {}
            
            dis_score_data['lat'] = district_data[18]
            dis_score_data['lon'] = district_data[19]
            dis_score_data['score'] = helpers.map_score(district_data[1])
            
            score_data.append(dis_score_data)
    
    return jsonify({'success': True, 'data': score_data})

@app.route('/newpost', methods=['POST'])
def make_post():
    data = request.json
    title = data['title']
    location = data['location']
    score = data['score']
    body = data['body']
    tags = data['tags']
    user_id = data['user_id']
    
    verified = 0 if user_id == "N/A" else 1
    upvotes = 0
    
    post_id = uuid.uuid4().hex
    
    cursor.execute("INSERT INTO posts(postid, title, location, score, body, tags, verified, upvotes) VALUES('%s', '%s', '%s', '%s', '%s', ARRAY %s, '%s', '%s') RETURNING postid;" % (post_id, title, location, score, body, tags, verified, upvotes))
    CONNECTION.commit()
    post_id = cursor.fetchone()
    
    return jsonify({'success': True, 'data': {'post_id': post_id}})

@app.route('/posts', methods=['GET'])
def get_posts():
    query = request.args.to_dict()
    location = query['location']

    cursor.execute("SELECT * FROM posts WHERE location = '%s' ORDER BY upvotes;" % (location))
    fetched_data = cursor.fetchall()

    tags_all = defaultdict(int)
    posts = []
    for post in fetched_data:
        post_data = {
            "title": post[6],
            "location": post[1],
            "score": post[2],
            "body": post[3],
            "tags": post[4],
            "verified": post[5],
            "upvotes": post[7]
        }
        posts.append(post_data)
        
        for tag in post[4]:
            tags_all[tag] += 1
    
    return jsonify({'success': True, 'data': {
        "score": 0,
        "tags": dict(tags_all),
        "posts": posts
    }})

if __name__ == '__main__':
    pass