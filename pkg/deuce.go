package pkg

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/catalystsquad/app-utils-go/errorutils"
	"github.com/catalystsquad/app-utils-go/logging"
	"github.com/dgraph-io/dgo/v200"
	"github.com/dgraph-io/dgo/v200/protos/api"
	"github.com/sirupsen/logrus"
	"time"
)

type Revisions struct {
	Revisions []Revision
}
type Revision struct {
	DType     []string  `json:"dgraph.type,omitempty"`
	Uid       string    `json:"uid,omitempty"`
	Id        int       `json:"revision.id,omitempty"`
	Version   int       `json:"revision.version"`
	AppliedAt time.Time `json:"revision.appliedAt,omitempty"`
	Locked    bool      `json:"revision.locked"`
}

type Migration struct {
	Operation     *api.Operation
	MigrationFunc func(txn *dgo.Txn) error
}

type Deuce struct {
	ClientId   string
	Migrations []Migration
	Client     *dgo.Dgraph
}

var revisionId = 1

func (d Deuce) Up() error {
	currentRevision, err := d.initialize()
	if err != nil {
		return err
	}
	numBehind := len(d.Migrations) - currentRevision.Version - 1
	if numBehind == 0 {
		logging.Log.Info("database is up to date, there are no migrations to run")
		return nil
	}
	logging.Log.WithFields(logrus.Fields{"current_revision": currentRevision.Version}).Info("current dgraph revision")
	for i, migration := range d.Migrations {
		//skip migrations that are lower than the current revision
		if currentRevision.Version >= i {
			continue
		}
		logging.Log.WithFields(logrus.Fields{"revision": i}).Info("running dgraph migration")
		err := d.runMigration(migration)
		if err != nil {
			return err
		}
		logging.Log.WithFields(logrus.Fields{"revision": i}).Info("completed dgraph migration")
		currentRevision.Version = i
		currentRevision.AppliedAt = time.Now()
		err = d.updateCurrentRevision(*currentRevision)
		if err != nil {
			return err
		}
	}
	logging.Log.WithFields(logrus.Fields{"current_revision": currentRevision.Version}).Info("Completed dgraph migrations")
	return nil
}

func (d Deuce) runMigration(migration Migration) error {
	// run the api operation first
	err := d.maybeExecuteApiOperation(migration)
	if err != nil {
		return err
	}
	// run the migration function
	return d.maybeExecuteMigrationFunction(migration)
}

func (d Deuce) maybeExecuteApiOperation(migration Migration) error {
	if migration.Operation != nil {
		err := d.Client.Alter(context.Background(), migration.Operation)
		errorutils.LogOnErr(nil, "error altering dgraph schema", err)
		if err != nil {
			return err
		}
	}
	return nil
}

func (d Deuce) maybeExecuteMigrationFunction(migration Migration) error {
	if migration.MigrationFunc != nil {
		txn := d.Client.NewTxn()
		defer txn.Discard(context.Background())
		err := migration.MigrationFunc(txn)
		errorutils.LogOnErr(nil, "error running migration function", err)
		if err != nil {
			return err
		}
		return err
	}
	return nil
}

func (d Deuce) initialize() (*Revision, error) {
	op := &api.Operation{
		Schema: `
		revision.id: int @index(int) @upsert .
		revision.version: int .
		revision.appliedAt: datetime .
		revision.locked: bool .
		revision.lockedAt: datetime .
		type Revision {
			revision.id: int!
			revision.version: int!
			revision.appliedAt: datetime!
			revision.locked: bool!
			revision.lockedAt: datetime!
		}
	`}
	err := d.Client.Alter(context.Background(), op)
	errorutils.LogOnErr(nil, "error creating deuce schema", err)
	if err != nil {
		return nil, err
	}
	return d.getCurrentRevision()
}

func (d Deuce) getCurrentRevision() (*Revision, error) {
	query := fmt.Sprintf(`
		{
			revisions(func: eq(revision.id, %d)) {
				uid
				revision.id
				revision.version
				revision.appliedAt
				revision.locked
			}
		}`, revisionId)
	txn := d.Client.NewReadOnlyTxn()
	resp, err := txn.Query(context.Background(), query)
	errorutils.LogOnErr(nil, "error getting current revision", err)
	if err != nil {
		return nil, err
	}
	revisions := Revisions{}
	err = json.Unmarshal(resp.GetJson(), &revisions)
	errorutils.LogOnErr(nil, "error marshalling revisions to struct", err)
	if err != nil {
		return nil, err
	}
	if len(revisions.Revisions) == 0 {
		return &Revision{
			DType:     []string{"Revision"},
			Id:        revisionId,
			Version:   -1,
			AppliedAt: time.Time{},
			Locked:    false,
		}, nil
	}
	return &revisions.Revisions[0], nil
}

func (d Deuce) updateCurrentRevision(currentRevision Revision) error {
	bytes, err := json.Marshal(currentRevision)
	errorutils.LogOnErr(nil, "error marshalling currentRevision to json", err)
	if err != nil {
		return err
	}
	mutation := &api.Mutation{
		CommitNow: true,
		SetJson:   bytes,
	}
	txn := d.Client.NewTxn()
	defer txn.Discard(context.Background())
	response, err := txn.Mutate(context.Background(), mutation)
	errorutils.LogOnErr(nil, "error updating current revision", err)
	if err != nil {
		return err
	}
	logging.Log.WithFields(logrus.Fields{"response": string(response.Json)}).Info("updated current revision")
	return nil
}
