package main

import (
	"database/sql"

	_ "gopkg.in/cq.v1"
)

type Neo4j struct {
	*sql.DB
}

func InitNeo4j() (*Neo4j, error) {
	db, err := sql.Open("neo4j-cypher", "http::/localhost:7474")
	if err != nil {
		return nil, err
	}

	return &Neo4j{
		DB: db,
	}, nil
}

func (n Neo4j) insertFriends() {

}
