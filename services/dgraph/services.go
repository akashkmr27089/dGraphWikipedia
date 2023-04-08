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
	client     *dgraph.DGraphClient
	cache      map[string]string
	countCache map[string]int
}

func New(client *dgraph.DGraphClient) *DGraphSvc {
	mapSet := make(map[string]string)
	mapSet2 := make(map[string]int)

	return &DGraphSvc{
		client:     client,
		cache:      mapSet,
		countCache: mapSet2,
	}
}

func (d *DGraphSvc) GetParentInfo(ctx context.Context, name string) (*int, *string, error) {
	a, ok := d.cache[name]
	if ok {
		d.countCache[name] += 1
		b, _ := d.countCache[name]
		return &b, &a, nil
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
		return nil, nil, err
	}

	data := resp.Json
	type Data struct {
		All []models.Node `json:"all"`
	}
	dataPipe := Data{}

	err = json.Unmarshal(data, &dataPipe)
	if err != nil {
		return nil, nil, err
	}

	if len(dataPipe.All) == 0 {
		return nil, nil, errors.New("No Data Found")
	}

	//defer d.client.Close()
	parentKey := dataPipe.All[0].Uid
	parentRefCount := dataPipe.All[0].ReferenceCount
	d.cache[name] = parentKey
	d.countCache[name] = parentRefCount

	return &parentRefCount, &parentKey, nil
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

func (d *DGraphSvc) UpdateReferenceCount(ctx context.Context, parentUid *string, parentCount *int) error {
	data := models.Node{
		Uid:            *parentUid,
		ReferenceCount: *parentCount + 1,
	}

	d.client.CreateTxn()
	_, err := d.client.Mutate(ctx, data)
	if err != nil {
		return err
	}

	return nil
}

func (d *DGraphSvc) CreateNodeWithParentConnection(ctx context.Context, childNode *models.Node, parentNodeName string) error {
	var (
		parentUidInfo *string
		parentCount   *int
	)

	parentCount, parentUidInfo, err := d.GetParentInfo(ctx, parentNodeName)
	if err != nil && parentUidInfo == nil {
		err := d.CreateParentNode(ctx, parentNodeName)
		if err != nil {
			return err
		}
		parentCount, parentUidInfo, err = d.GetParentInfo(ctx, parentNodeName)
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

	// Increase Parent Count
	err = d.UpdateReferenceCount(ctx, parentUidInfo, parentCount)
	if err != nil {
		return err
	}

	return nil
}
