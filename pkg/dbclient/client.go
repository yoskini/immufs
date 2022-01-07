package dbclient

import (
	"context"

	"immufs/pkg/config"

	"github.com/codenotary/immudb/pkg/client"
	"github.com/sirupsen/logrus"
)

type ImmuDbClient struct {
	cl  client.ImmuClient
	log *logrus.Entry
}

// Instantiate and connect the Immudb client
func NewImmuDbClient(ctx context.Context, cfg *config.Config, log *logrus.Logger) (*ImmuDbClient, error) {
	cl := client.NewClient()
	err := cl.OpenSession(ctx, []byte(cfg.User), []byte(cfg.Password), cfg.Immudb)
	if err != nil {
		return nil, err
	}
	return &ImmuDbClient{
		cl:  cl,
		log: log.WithFields(logrus.Fields{"component": "immudb client"}),
	}, nil
}

func (idb *ImmuDbClient) Destroy(ctx context.Context) error {
	err := idb.cl.CloseSession(ctx)
	if err != nil {
		idb.log.Errorf("could not close session: %s", err)

		return err
	}

	return nil
}
