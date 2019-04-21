package db

import (
	"fmt"
	"log"
	"time"

	"github.com/gocql/gocql"
)

// CassandraChan exported
var CassandraChan chan *gocql.Session

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

func getSession() (*gocql.Session, error) {
	select {

	case CassandraSession := <-CassandraChan:
		return CassandraSession, nil
	case <-time.After(100 * time.Millisecond):
		cluster := gocql.NewCluster("127.0.0.1")
		cluster.Keyspace = "system"

		session, err := cluster.CreateSession()

		if err != nil {
			panic(err)
		}

		return session, nil
	}
}

// Initialize : Initialize database
func Initialize() {

	cluster := gocql.NewCluster("127.0.0.1")
	cluster.Keyspace = "system"
	session, err := cluster.CreateSession()
	if err != nil {
		log.Fatal(err)
	}
	session.Query(keyspaceCreationQuery).Iter()
	session.Query(transactionBaseQuery).Iter()
	session.Query(balanceBaseQuery).Iter()

	CassandraChan = make(chan *gocql.Session, 50)
	queueSession(session)
}

func queueSession(session *gocql.Session) {

	select {
	case CassandraChan <- session:
		// session enqueued
	default:
		fmt.Println("Blocked")
		defer session.Close()
	}

}
