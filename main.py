from pymongo import MongoClient
import psycopg2

choice = input('Press "m" for setting up mongo db from postgres and "p" to setup postgres db from mongo :: ').strip().lower()

if choice == 'p':
    try:
        # MongoDB connection Setup
        uri = input("Paste URI for your mongo server :: ").strip()
        client = MongoClient(uri)
        db = input("Enter the name of database from which you want to import :: ").strip()
        coll = input("Enter the name of collection :: ").strip()
        database = client[db]
        collection = database[coll]
    
        db_created = input(f"Enter y after creating the db name {db} in your sql server : ").strip().lower()

        if db_created == 'y': 
            conn = psycopg2.connect(
            dbname=db,
            user= input("Enter the username for postgres :: ").strip(),
            password=input(f"Enter the password for this user :: ").strip(),
            host=input("Enter the host name (localhost if using the local server) :: ").strip(),
            port=input("Enter the post of your server (5432 is using local server) :: ").strip()
            )
            cur = conn.cursor()
            response = collection.find()
            keys = []
            values = []
            for docs in response :
                keys.append(docs.keys())
                value = list(docs.values())
                value[0] = str(value[0])
                values.append(tuple(value))
            if all(x == keys[0] for x in keys):
                print(f"In your sql db {db} create a table name {coll} with columns (select appropriate datatypes): ")
                for i in keys[0]:
                    print(i)
                ans = input('Press y if you have made that table with proper datatype :: ')
                if ans.strip().lower() == 'y':
                    print(f"Inserted to the DB={db} and table={coll}")
                    val = ""
                    for i in (values):
                        val += f"{i},"
                    query = f'insert into {coll} values{val[0:-1]};'
                    # print(query)
                    cur.execute(query)
                    conn.commit()
                else:
                    print("Ohk exiting the program .....")
            else:
                print("Schema of the documents is not consistent so cant be put into the sql table")
            cur.close()
            conn.close()
        client.close()

    except Exception as e:
        raise Exception(
            "The following error occurred: ", e)
elif choice == 'm':
    try:
        db = input("Enter the name of database from which you want to import :: ").strip()
        table = input("Enter the name of table :: ").strip()
        conn = psycopg2.connect(
            dbname=db,
            user= input("Enter the username for postgres :: ").strip(),
            password=input(f"Enter the password for this user :: ").strip(),
            host=input("Enter the host name (localhost if using the local server) :: ").strip(),
            port=input("Enter the post of your server (5432 is using local server) :: ").strip()
        )
        cur = conn.cursor()
        cur.execute(f'SELECT * FROM {table};')
        result = cur.fetchall()
        column_names = [desc[0] for desc in cur.description]
        db_created = input(f"Enter y to start inserting data into {db} db in your mongo server : ").strip().lower()
        if db_created == 'y':
            uri = input("Paste URI for your mongo server :: ").strip()
            client = MongoClient(uri)
            database = client[db]
            collection = database[table]
            insert_arr = []
            for i in result:
                insert_dict = {}
                for j in range(len(column_names)):
                    insert_dict[column_names[j]] = i[j]
                insert_arr.append(insert_dict)
            collection.insert_many(insert_arr)
            print(f"Data inserted into collection {table} in DB {db} ... ")
            client.close()
        cur.close()
        conn.close()
    except Exception as e:
        raise Exception(
            "The following error occurred: ", e)
