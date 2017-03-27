import MySQLdb
import time
import sys
from config import *
import argparse
from neo4j.v1 import GraphDatabase, basic_auth

def parse_args():
    parser = argparse.ArgumentParser(description="Inserts the provided friendship datasets into a given database")
    parser.add_argument("--sql_pw", help="Password to the sql database")
    parser.add_argument("--sql_db", help="Name of the sql database")
    parser.add_argument("--neo_pw", help="Password to neo4j database")
    parser.add_argument("--neo_db", help="Name of neo4j databse")
    parser.add_argument("--depth", type=int, help="Specifices which depth to test, defaults to full test from depth 1 to 5")
    parser.add_argument("--result_file", default="results", help="Path to result file, creates the file \"results\" by default")

    return parser.parse_args()

def neo_test(args):
    driver = GraphDatabase.driver("bolt://localhost:7687", auth=basic_auth(args.neo_db, args.neo_pw))

    query = "MATCH (n:Person {name: \"0\"})-[:KNOWS]-(f1) WITH DISTINCT f1"

    for i in range (1, args.depth):
        if i == args.depth - 1:
            query += " MATCH (f%d)-[:KNOWS]-(f%d) RETURN DISTINCT f%d.name as res" % (i, i+1, args.depth)
        else:
            query += " MATCH (f%d)-[:KNOWS]-(f%d) WITH DISTINCT f%d" % (i, i+1, i+1)

    if args.depth == 1:
        query = "MATCH (n:Person {name: \"0\"})-[:KNOWS]-(f1) WITH DISTINCT f1 RETURN DISTINCT f1.name as res"

    with driver.session() as session:
        start = time.time()
        res = session.run(query)
        session.sync()
        end = time.time() - start
        print "NEO4J TEST : %f" % (end)

    records = res.records()

    out = [int(r["res"]) for r in records]

    return (out, end)


def sql_test(args):

    db = MySQLdb.connect(host="localhost", user="root", passwd=args.sql_pw, db=args.sql_db)

    cur = db.cursor()

    query = ""
    for i in range (1, args.depth+1):
        if i == args.depth:
            query += "select distinct friendId from fb_dup where userId = 0" + ")" * (args.depth-1)
        else:
            query += "select distinct friendId from fb_dup where userId in("

    if args.depth == 1:
        query = "select distinct friendId from fb_dup where userId = 0"

    start = time.time()
    n_res = cur.execute(query)
    end = time.time() - start
    print "SQL TEST : %f" % (end)
    data = cur.fetchall()

    out = [d[0] for d in data if d != ""]

    return (out, end)

def do_test(args, res_file):

    res1 = neo_test(args)
    res2 = sql_test(args)

    data1 = set(res1[0])
    data2 = set(res2[0])

    diff = data1.symmetric_difference(data2)
    if len(diff) != 0:
        print "SHIT WENT WRONG FUCKERS"
        print len(data1)
        print len(data2)
        print len(diff)
    else:
        print "FUCK YE"
        print len(data1)
        print len(data2)

    result_str = "%d\t%f\t%f\t%d\n" % (args.depth, res1[1], res2[1], len(data1))
    res_file.write(result_str)
    res_file.flush()

def main():

    local_args = parse_args()

    config = init(local_args)

    if config == None:
        exit_str =  "\nNon-existing or insufficent config, execute program with -h to view parameters required for config creation\n"
        sys.exit(exit_str)

    f = open(config.result_file, "w")

    if local_args.depth is None:
        for i in range(1, 5):
            for j in range(1, 30):
                config.depth = i
                do_test(config, f)
    else:
        config.depth = local_args.depth
        do_test(config, f)

    f.close()


if __name__== "__main__":
    main()
