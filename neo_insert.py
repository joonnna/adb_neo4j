from neo4j.v1 import GraphDatabase, basic_auth
driver = GraphDatabase.driver("bolt://localhost:7687", auth=basic_auth("neo4j", "feeder123"))

data = []

path = sys.argv[1]

with open(path, 'r') as f:
    for line in f:
        line = line.strip('\n')
        subset = line.split(' ')
        data.append(subset)

# Insert data
insert_query = '''
UNWIND {pairs} as pair
MERGE (p1:Person {name:pair[0]})
MERGE (p2:Person {name:pair[1]})
MERGE (p1)-[:KNOWS]-(p2);
'''

with driver.session() as session:
    session.run(insert_query, parameters={"pairs": data})
