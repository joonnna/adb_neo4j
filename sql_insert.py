import MySQLdb
import sys
import argparse
from config import *

def parse_args():
    parser = argparse.ArgumentParser(description="Inserts the provided friendship datasets into a given database")
    parser.add_argument("--sql_pw", help="Password to the sql database")
    parser.add_argument("--sql_name", help="Name of the sql database")
    parser.add_argument("--table_name", help="Name of the table to create(will contain userId and friendId)")
    parser.add_argument("--data", default="facebook_combined.txt", help="Path to dataset, should contain friendship relations")

    return parser.parse_args()

def main():
    local_args = parse_args()

    args = init(local_args)
    if args == None:
        exit_str =  "Non-existing or insufficent config, execute program with -h to view parameters required for config creation\n"
        sys.exit(exit_str)

    db = MySQLdb.connect(host="localhost", user="root", passwd=args.sql_pw, db=args.sql_name)

    cur = db.cursor()

    f = open(args.data, "r")
    data = f.read()
    f.close()

    split = data.split("\n")

    q = "CREATE TABLE %s (userId INT, friendId INT)" % (args.table_name)
    cur.execute(q)

    for line in split:
        if len(line) < 1:
            continue

        vals = line.split(" ")
        query = "INSERT INTO %s (userId, friendId) VALUES (%d, %d)" % (args.table_name, int(vals[0]), int(vals[1]))
        cur.execute(query)


        query2 = "INSERT INTO %s (userId, friendId) VALUES (%d, %d)" % (args.table_name, int(vals[1]), int(vals[0]))
        cur.execute(query2)

    db.commit()
    db.close()

if __name__ == "__main__":
    main()
