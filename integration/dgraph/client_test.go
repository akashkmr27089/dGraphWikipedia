package dgraph

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/dgraph-io/dgo/v210"
	"github.com/dgraph-io/dgo/v210/protos/api"
	"google.golang.org/grpc"
	"testing"
)

type Person struct {
	Uid    string   `json:"uid,omitempty"`
	Name   string   `json:"name,omitempty"`
	DType  []string `json:"dgraph.type,omitempty"`
	Parent *Person  `json:"parent"`
}

func Test_client(t *testing.T) {
	// Create Clinet
	d := DGraphClient{}
	err := d.New("localhost:9080")
	if err != nil {
		panic(err)
	}

	ctx := context.Background()
	type Person struct {
		Uid    string   `json:"uid,omitempty"`
		Name   string   `json:"name,omitempty"`
		DType  []string `json:"dgraph.type,omitempty"`
		Parent *Person  `json:"parent"`
	}

	p := Person{
		Name:  "Alice24",
		DType: []string{"Person"},
		Parent: &Person{
			Uid:  "0x2720",
			Name: "Bob",
		},
	}

	pb, err := json.Marshal(p)
	// Check error

	mu := &api.Mutation{
		SetJson: pb,
	}

	req := &api.Request{CommitNow: true, Mutations: []*api.Mutation{mu}}
	res, err := d.txn.Do(ctx, req)
	if err != nil {
		panic(err)
	}

	d.Close()

	fmt.Println(res)
}

func Test_query(t *testing.T) {
	ctx := context.Background()
	d := DGraphClient{}
	err := d.New("localhost:9080")
	if err != nil {
		panic(err)
	}

	q := `query all($a: string) {
    all(func: eq(name, $a)) {
      name
      uid
    }
  }`

	resp, err := d.QueryWithVars(ctx, q, map[string]string{"$a": "Alice"})
	if err != nil {
		panic(err)
	}

	data := resp.Json
	//p := Person{}
	type Data struct {
		All []Person `json:"all"`
	}
	dataPipe := Data{}

	err = json.Unmarshal(data, &dataPipe)
	if err != nil {
		fmt.Println(err)
	}
	//fmt.Printf(p)

	defer d.Close()
	parentKey := dataPipe.All[0].Uid
	fmt.Println(parentKey)
	fmt.Println("This is Just a test")
}

func Test_clientTest(t *testing.T) {
	ctx := context.Background()
	conn, err := grpc.Dial("localhost:9080", grpc.WithInsecure())
	if err != nil {
		panic(err)
	}
	// Check error
	defer conn.Close()
	dgraphClient := dgo.NewDgraphClient(api.NewDgraphClient(conn))

	txn := dgraphClient.NewTxn()
	defer txn.Discard(ctx)

	type Person struct {
		Uid   string   `json:"uid,omitempty"`
		Name  string   `json:"name,omitempty"`
		DType []string `json:"dgraph.type,omitempty"`
	}

	p := Person{
		Uid:   "_:alice",
		Name:  "Alice234",
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

}
