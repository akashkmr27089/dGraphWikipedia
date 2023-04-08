package dgraph

import (
	"context"
	"dgraphProject/integration/dgraph"
	"dgraphProject/models"
	"encoding/json"
	"errors"
)

//1. Get the Parent UUID
//2. Enter its details along with parent Details Uid
//3. Keep Recursing

type DGraphSvc struct {
	client *dgraph.DGraphClient
	cache  map[string]string
}

func New(client *dgraph.DGraphClient) *DGraphSvc {
	mapSet := make(map[string]string)
	return &DGraphSvc{
		client: client,
		cache:  mapSet,
	}
}

func (d *DGraphSvc) GetParentInfo(ctx context.Context, name string) (*string, error) {
	a, ok := d.cache[name]
	if ok {
		return &a, nil
	}

	q := `query all($a: string) {
    all(func: eq(name, $a)) {
      name
      uid
    }
  }`

	d.client.CreateTxn()
	resp, err := d.client.QueryWithVars(ctx, q, map[string]string{"$a": name})
	if err != nil {
		return nil, err
	}

	data := resp.Json
	type Data struct {
		All []models.Node `json:"all"`
	}
	dataPipe := Data{}

	err = json.Unmarshal(data, &dataPipe)
	if err != nil {
		return nil, err
	}

	if len(dataPipe.All) == 0 {
		return nil, errors.New("No Data Found")
	}

	//defer d.client.Close()
	parentKey := dataPipe.All[0].Uid
	d.cache[name] = parentKey

	return &parentKey, nil
}

func (d *DGraphSvc) CreateParentNode(ctx context.Context, parentNodeNames string) error {
	parentnode := models.Node{
		Name: parentNodeNames,
	}

	_, err := d.client.Mutate(ctx, parentnode)
	if err != nil {
		return err
	}

	return nil
}

func (d *DGraphSvc) CreateNodeWithParentConnection(ctx context.Context, childNode *models.Node, parentNodeName string) error {
	var parentUidInfo *string
	parentUidInfo, err := d.GetParentInfo(ctx, parentNodeName)
	if err != nil && parentUidInfo == nil {
		err := d.CreateParentNode(ctx, parentNodeName)
		if err != nil {
			return err
		}
		parentUidInfo, err = d.GetParentInfo(ctx, parentNodeName)
	}
	if err != nil {
		return err
	}

	childNode.Parent = &models.Node{
		Uid: *parentUidInfo,
	}

	d.client.CreateTxn()
	_, err = d.client.Mutate(ctx, childNode)
	if err != nil {
		return err
	}

	return nil
}
