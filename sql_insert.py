import MySQLdb
import sys

db = MySQLdb.connect(host="localhost", user="root", passwd="feeder123", db="shitballs")

cur = db.cursor()

path = sys.argv[1]

f = open(path, "r")
data = f.read()
f.close()

split = data.split("\n")

for line in split:
    if len(line) < 1:
        continue

    vals = line.split(" ")
    query = "INSERT INTO fb_dup (userId, friendId) VALUES (%d, %d)" % (int(vals[0]), int(vals[1]))
    cur.execute(query)


    query2 = "INSERT INTO fb_dup (userId, friendId) VALUES (%d, %d)" % (int(vals[1]), int(vals[0]))
    cur.execute(query2)

db.commit()
db.close()
