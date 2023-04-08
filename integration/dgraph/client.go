package dgraph

import (
	"context"
	"encoding/json"
	"github.com/dgraph-io/dgo/v210"
	"github.com/dgraph-io/dgo/v210/protos/api"
	"google.golang.org/grpc"
)

type DGraphClient struct {
	conn   *grpc.ClientConn
	client *dgo.Dgraph
	txn    *dgo.Txn
}

func (d *DGraphClient) New(target string) error {
	err := d.CreateConn(target)
	if err != nil {
		return err
	}

	d.CreateTxn()
	return nil
}

func (d *DGraphClient) CreateConn(target string) error {
	conn, err := grpc.Dial(target, grpc.WithInsecure())
	if err != nil {
		return err
	}

	d.conn = conn
	d.client = dgo.NewDgraphClient(api.NewDgraphClient(conn))
	return nil
}

func (d *DGraphClient) Close() {
	d.conn.Close()
	d.txn = d.client.NewTxn()
}

func (d *DGraphClient) CreateTxn() {
	d.txn = d.client.NewTxn()
}

func (d *DGraphClient) CloseTxn(ctx context.Context) {
	d.txn.Discard(ctx)
}

func (d *DGraphClient) Mutate(ctx context.Context, p any) (*api.Response, error) {
	pb, err := json.Marshal(p)
	// Check error

	mu := &api.Mutation{
		SetJson: pb,
	}

	req := &api.Request{CommitNow: true, Mutations: []*api.Mutation{mu}}
	res, err := d.txn.Do(ctx, req)
	if err != nil {
		return nil, err
	}

	return res, nil
}

func (d *DGraphClient) QueryWithVars(ctx context.Context, q string, vars ...map[string]string) (*api.Response, error) {
	res, err := d.txn.QueryWithVars(ctx, q, vars[0])
	if err != nil {
		return nil, err
	}

	return res, nil
}
