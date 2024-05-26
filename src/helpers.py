import json
import psycopg2

from dotenv import load_dotenv
import os
load_dotenv()

CONNECTION = psycopg2.connect(
    dbname = os.getenv("DB_NAME"),
    user = os.getenv("DB_USER"),
    password = os.getenv("DB_PASSWORD"),
    host = os.getenv("DB_HOST"),
    port = os.getenv("DB_PORT")
)

def map_type(place_type):
    if place_type in ['police', 'hospital', 'fire station', 'clinic', 'townhall']:
        return 5
    elif place_type in ['hotel', 'retail', 'park', 'library', 'school', 'post office', 'place of worship', 'supermarket']:
        return 4
    return 0

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

    # Establish a connection to the PostgreSQL database
    try:
        cursor = CONNECTION.cursor()
        print('Connected to PostgreSQL database')

        # Iterate over the records and insert them into the database
        i = 0
        print('Writing records to PostgreSQL')
        for record_id, record_data in records.items():
            # Convert record_data to a format suitable for insertion
            columns = ("districtid" + ', ') + ', '.join([x.lower() for x in record_data.keys()])
            values = (record_id + ', ') + ', '.join(['%s'] * len(record_data))
            insert_query = f'INSERT INTO crimes ({columns}) VALUES ({values})'
            
            # Execute the insertion query
            cursor.execute(insert_query, list(record_data.values()))
            i += 1
            
            # Commit every 500 records
            if i % 500 == 0:
                CONNECTION.commit()
                print(i)
        
        # Commit any remaining records
        CONNECTION.commit()
        print(i)
        
    except Exception as e:
        print(f'Error: {e}')
    finally:
        if CONNECTION:
            cursor.close()
            CONNECTION.close()
            print('PostgreSQL connection is closed')