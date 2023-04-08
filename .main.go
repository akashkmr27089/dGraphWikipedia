package main

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/dgraph-io/dgo/v210"
	"github.com/dgraph-io/dgo/v210/protos/api"
	"google.golang.org/grpc"
)

func createConnection() (*grpc.ClientConn, error) {
	return grpc.Dial("localhost:9080", grpc.WithInsecure())
}

type Person struct {
	Uid   string   `json:"uid,omitempty"`
	Name  string   `json:"name,omitempty"`
	DType []string `json:"dgraph.type,omitempty"`
}

func main() {
	ctx := context.Background()
	conn, err := createConnection()
	if err != nil {
		panic(err)
	}
	// Check error
	defer conn.Close()

	dgraphClient := dgo.NewDgraphClient(api.NewDgraphClient(conn))
	//err = dgraphClient.Login(ctx, "groot", "password")
	//if err != nil {
	//	panic(err)
	//}

	txn := dgraphClient.NewTxn()
	defer txn.Discard(ctx)

	p := Person{
		Uid:   "_:alice",
		Name:  "Alice2",
		DType: []string{"Person"},
	}

	pb, err := json.Marshal(p)
	// Check error

	mu := &api.Mutation{
		SetJson: pb,
	}

	req := &api.Request{CommitNow: true, Mutations: []*api.Mutation{mu}}
	res, err := txn.Do(ctx, req)
	if err != nil {
		panic(err)
	}
	fmt.Println(res)

	txn = dgraphClient.NewTxn()

	q := `query all($a: string) {
    all(func: eq(name, $a)) {
      name
    }
  }`

	res, err = txn.QueryWithVars(ctx, q, map[string]string{"$a": "Alice"})
	fmt.Printf("%s\n", res.Json)
}
