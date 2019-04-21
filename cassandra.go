package db

import (
	"log"

	"github.com/gocql/gocql"
)

// CassandraSession exported
var CassandraSession *gocql.Session

var keyspaceCreationQuery = `
CREATE KEYSPACE IF NOT EXISTS malengopay
WITH replication={'class': 'SimpleStrategy', 'replication_factor': 3}
`

var transactionBaseQuery = `
CREATE TABLE IF NOT EXISTS malengopay.transactions (
    id int,
    accountid text,
    createdat timestamp,
    description text,
    amount int,
    currency text,
    notes text,
    PRIMARY KEY(id)
)
`
var balanceBaseQuery = `
CREATE TABLE IF NOT EXISTS malengopay.balances (
    accountid text,
    amount int,
    currency text,
    PRIMARY KEY(accountid)
)
`

// Initialize : Initialize database
func init() {

	cluster := gocql.NewCluster("127.0.0.1")
	cluster.Keyspace = "system"
	session, err := cluster.CreateSession()
	if err != nil {
		log.Fatal(err)
	}
	session.Query(keyspaceCreationQuery).Iter()
	session.Query(transactionBaseQuery).Iter()
	session.Query(balanceBaseQuery).Iter()

	// Close session when done initializing it
	defer CassandraSession.Close()
}
