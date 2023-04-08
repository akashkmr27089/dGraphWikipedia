package main

import (
	"context"
	"dgraphProject/models"
	"fmt"
	"strings"
	"sync"

	"dgraphProject/integration/Storage"
	"dgraphProject/integration/dgraph"
	dgraphSvc "dgraphProject/services/dgraph"
	"github.com/gocolly/colly"
)

//var wg sync.WaitGroup

// Implement Logic For Implementing Wikipedia Graph
// Make the Scrapper run in go-routing
// Publish it to kafka Topics
// Logic for Calculating the Score for each node in Cron based on incoming connection and external Connection
// Worker for Subscribing the topic,

type Wrapper struct {
	wg             sync.WaitGroup
	queue          Storage.Queue
	processedQueue int
}

func (w *Wrapper) generateLinkage(ctx context.Context, searchParam string, allowedDomains string, client *dgraphSvc.DGraphSvc) {
	w.processedQueue += 1
	//fileName := strings.Split(searchParam, "wiki/")[1]
	//fName := fmt.Sprintf("%s.csv", fileName)

	//defer w.wg.Done()
	//file, err := os.Create("test/" + fName)
	//if err != nil {
	//	log.Fatalf("Could not create file, err: %q", err)
	//	return
	//}
	//defer file.Close()
	//
	//writer := csv.NewWriter(file)
	//defer writer.Flush()

	c := colly.NewCollector(
		colly.AllowedDomains(allowedDomains),
	)

	// Find and print all links
	c.OnHTML(".mw-parser-output", func(e *colly.HTMLElement) {
		links := e.ChildAttrs("a", "href")
		fmt.Println(links)
		for _, link := range links {
			if len(strings.Split(link, "wiki/")) > 1 {
				//writer.Write([]string{searchParam, link})

				childNode := models.Node{
					Name: strings.Split(link, "wiki/")[1],
				}

				err := client.CreateNodeWithParentConnection(ctx, &childNode, strings.Split(searchParam, "wiki/")[1])
				if err != nil {
					return
				}

				w.queue.Push(link)
			}
		}
	})

	err := c.Visit("https://en.wikipedia.org/" + searchParam)
	if err != nil {
		panic(err)
		return
	}

	return
}

//func (w *Wrapper) run() {
//	w.wg.Add(2)
//	allowedDomains := "en.wikipedia.org"
//	searchParam := "wiki/India"
//	go w.generateLinkage(searchParam, allowedDomains)
//	go w.generateLinkage("wiki/China", allowedDomains)
//	w.wg.Wait()
//	fmt.Println("Terminating Program")
//	pop, b := w.queue.Pop()
//	if b {
//		fmt.Println(fmt.Sprintf("This is Special %s", pop))
//	}
//}

func (w *Wrapper) run2(ctx context.Context, client *dgraphSvc.DGraphSvc) {

	allowedDomains := "en.wikipedia.org"
	searchParam := "wiki/India"
	w.wg.Add(1)
	w.generateLinkage(ctx, searchParam, allowedDomains, client)
	//w.wg.Wait()

	var (
		b   = true
		pop = ""
	)

	for b && w.processedQueue < 100 {
		pop, b = w.queue.Pop()
		if b {
			//w.wg.Add(1)
			w.generateLinkage(ctx, pop, allowedDomains, client)
			//w.wg.Wait()
		}
	}
	//w.wg.Wait()
}

func main() {
	w := Wrapper{}
	ctx := context.Background()
	dg := dgraph.DGraphClient{}
	err := dg.New("localhost:9080")
	if err != nil {
		panic(err)
	}

	dgraphSvc := dgraphSvc.New(&dg)

	w.run2(ctx, dgraphSvc)
}
