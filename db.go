package db

import (
	"math/rand"
	"time"

	pb "github.com/ubunifupay/transaction/pb"

	"github.com/scylladb/gocqlx"
	"github.com/scylladb/gocqlx/qb"
)

// transactionDB definitions
type transactionDB struct {
	ID          int64     `db:"id"`
	AccountID   string    `db:"accountid"`
	CreatedAt   time.Time `db:"createdat"`
	Description string    `db:"description"`
	Amount      int64     `db:"amount"`
	Currency    string    `db:"currency"`
	Notes       string    `db:"notes"`
}

// StoreTransaction : This stores the stransaction
func StoreTransaction(req *pb.TransactionRequest) (*pb.TransactionRequest, error) {
	if req.ID == 0 {
		req.ID = int64(rand.Intn(150000)) // Should be gocql.RandomUUID() but since i didnt set up any pb with strings as ids..
	}

	session, _ := getSession()

	stmt, names := qb.Insert("malengopay.transactions").
		Columns("id", "accountid", "createdat", "description", "amount", "currency", "notes").
		ToCql()

	obj := transactionDB{
		req.ID,
		req.AccountID,
		time.Now(),
		req.Description,
		req.Amount,
		req.Currency,
		req.Notes,
	}

	// Create a query which uses the built query and populates it with the
	// values in the new item
	query := gocqlx.Query(session.Query(stmt), names).BindStruct(obj)

	// Run that query and release it when done
	err := query.ExecRelease()

	go queueSession(session)

	return req, err
}
