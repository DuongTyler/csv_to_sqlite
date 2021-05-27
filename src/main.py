import csv
import sqlite3
import argparse

def csv_getData(path: str):
    with open (path) as csv_f:
        csv_r = csv.reader(csv_f, delimiter=',')
        csv_data = [row for row in csv_r]
        print(csv_data[0])
    return csv_data

def create_table(cur: sqlite3.Cursor, table: str, cols):
    # sqlite doesn't enforce varchar lengths
    # Also, if someone knows a way to use prepared statements in a create table statement, we should implement that here.
    query = "CREATE TABLE IF NOT EXISTS %s (id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL" % table
    for arg in cols:
        query += ", " + str(arg).replace("'", "\\'") + " VARCHAR(64)"
    query += ")"
    print(query)
    cur.execute(query)

def insert_table(cur: sqlite3.Cursor, table: str, data):
    query = ("INSERT INTO %s VALUES (NULL" % table) + ", ?"*len(data[0]) + ")" #generates a prepared insert statement. only sql-injectable thing here is table name, which we get as a command-line arg
    print(query)
    cur.executemany(query, data)

def main(args):
    print("connecting to: " + args.database_path)
    db = sqlite3.connect(args.database_path)
    if not db:
        print("Could not connect to database")
        exit
    cur = db.cursor()
    if not cur:
        print("Could not create cursor")
        exit
    csv_data = csv_getData(args.csv_path)   # csv needs to be loaded into an array before we can do anything with it
    create_table(cur, args.table_name, csv_data[0]) #pass the first row as the table entry names
    insert_table(cur, args.table_name, csv_data[1:])
    db.commit()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
            description="update a database from a CSV")
    parser.add_argument(
            "--database", "-d", dest="database_path", required=True, 
            help="The database uri")
    parser.add_argument(
            "--csv", "-c", dest="csv_path", required=True, 
            help="The csv uri")
    parser.add_argument(
            "--table", "-t", dest="table_name", required=True, 
            help="The table name")
    args = parser.parse_args()
    main(args)
