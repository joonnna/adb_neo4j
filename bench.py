import MySQLdb
import time
import sys
from neo4j.v1 import GraphDatabase, basic_auth


def neo_test(depth):
    driver = GraphDatabase.driver("bolt://localhost:7687", auth=basic_auth("neo4j", "feeder123"))

    query = "MATCH (n:Person {name: \"0\"})-[:KNOWS]-(f1) WITH DISTINCT f1"

    for i in range (1, depth):
        if i == depth - 1:
            query += " MATCH (f%d)-[:KNOWS]-(f%d) RETURN DISTINCT f%d.name as res" % (i, i+1, depth)
        else:
            query += " MATCH (f%d)-[:KNOWS]-(f%d) WITH DISTINCT f%d" % (i, i+1, i+1)

    if depth == 1:
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


def sql_test(db, depth):
    cur = db.cursor()

    query = ""
    for i in range (1, depth+1):
        if i == depth:
            query += "select distinct friendId from fb_dup where userId = 0" + ")" * (depth-1)
        else:
            query += "select distinct friendId from fb_dup where userId in("

    if depth == 1:
        query = "select distinct friendId from fb_dup where userId = 0"

    start = time.time()
    n_res = cur.execute(query)
    end = time.time() - start
    print "SQL TEST : %f" % (end)
    data = cur.fetchall()

    out = [d[0] for d in data if d != ""]

    return (out, end)

def do_test(db, depth, res_file):

    res1 = neo_test(depth)
    res2 = sql_test(db, depth)

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

    result_str = "%d\t%f\t%f\t%d" % (depth, res1[1], res2[1], len(data1))
    res_file.write(result_str)
    res_file.flush()

def main():
    db = MySQLdb.connect(host="localhost", user="root", passwd="feeder123", db="shitballs")

    f = open("results", "w")

    if len(sys.argv) > 1:
        depth = int(sys.argv[1])
        do_test(db, depth, f)
    else:
        for i in range(1, 4):
            for j in range(1, 30):
                print i
                do_test(db, i, f)


    f.close()


if __name__== "__main__":
    main()
