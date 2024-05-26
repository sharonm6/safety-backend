import json
import psycopg2

CONNECTION = psycopg2.connect(
    dbname="haven",
    user="postgres",
    password="postgres",
    host="localhost",
    port="5432"
)

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
                connection.commit()
                print(i)
        
        # Commit any remaining records
        connection.commit()
        print(i)
        
    except Exception as e:
        print(f'Error: {e}')
    finally:
        if connection:
            cursor.close()
            connection.close()
            print('PostgreSQL connection is closed')