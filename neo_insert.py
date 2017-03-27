import argparse
import sys
from config import *
from neo4j.v1 import GraphDatabase, basic_auth

def parse_args():
    parser = argparse.ArgumentParser(description="Inserts the provided dataset of friendship relations into a neo4j database")
    parser.add_argument("--neo_pw", help="password to the neo4j database")
    parser.add_argument("--neo_db", help="name of the neo4j database")
    parser.add_argument("--data", help="path to dataset to be inserted into the neo4j database, should contain friendship relations", default="facebook_combined.txt")

    return parser.parse_args()

def main():
    local_args = parse_args()

    args = init(local_args)
    if args == None:
        exit_str =  "\nNon-existing or insufficent config, execute program with -h to view parameters required for config creation\n"
        sys.exit(exit_str)

    driver = GraphDatabase.driver("bolt://localhost:7687", auth=basic_auth(args.neo_db, args.neo_pw))

    data = []

    with open(args.data, 'r') as f:
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


if __name__ == "__main__":
    main()
