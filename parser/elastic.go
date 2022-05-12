package parser

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/dogukanuhn/es-golang/models"
	"github.com/dustin/go-humanize"
	"github.com/elastic/go-elasticsearch/v8"
	"github.com/elastic/go-elasticsearch/v8/esapi"
)

func IndexData(es *elasticsearch.Client, movieList []models.Movie) {

	count := len(movieList)
	batch := 255

	var (
		buf bytes.Buffer
		res *esapi.Response
		err error
		raw map[string]interface{}

		indexName = "movies"

		numItems   int
		numErrors  int
		numIndexed int
		numBatches int
		currBatch  int
	)

	// Re-create the index
	//
	if res, err = es.Indices.Delete([]string{indexName}); err != nil {
		log.Fatalf("Cannot delete index: %s", err)
	}
	res, err = es.Indices.Create(indexName)
	if err != nil {
		log.Fatalf("Cannot create index: %s", err)
	}
	if res.IsError() {
		log.Fatalf("Cannot create index: %s", res)
	}

	if count%batch == 0 {
		numBatches = (count / batch)
	} else {
		numBatches = (count / batch) + 1
	}

	start := time.Now().UTC()

	for i, item := range movieList {

		numItems++

		currBatch = i / batch
		if i == count-1 {
			currBatch++
		}

		// Prepare the metadata payload
		//
		meta := []byte(fmt.Sprintf(`{ "index" : { "_id" : "%d" } }%s`, item.ID, "\n"))

		// Prepare the data payload: encode article to JSON
		//
		data, err := json.Marshal(item)
		if err != nil {
			log.Fatal("Cannot encode movie")
		}

		// Append newline to the data payload
		//
		data = append(data, "\n"...)

		// Append payloads to the buffer (ignoring write errors)
		//
		buf.Grow(len(meta) + len(data))
		buf.Write(meta)
		buf.Write(data)

		if i > 0 && i%batch == 0 || i == count-1 {

			fmt.Printf("[%d/%d] ", currBatch, numBatches)

			res, err = es.Bulk(bytes.NewReader(buf.Bytes()), es.Bulk.WithIndex(indexName))

			if err != nil {
				log.Fatalf("Failure indexing batch %d: %s", currBatch, err)
			}
			// If the whole request failed, print error and mark all documents as failed
			//
			if res.IsError() {
				numErrors += numItems
				if err := json.NewDecoder(res.Body).Decode(&raw); err != nil {
					log.Fatalf("Failure to to parse response body: %s", err)
				} else {
					log.Printf("  Error: [%d] %s: %s",
						res.StatusCode,
						raw["error"].(map[string]interface{})["type"],
						raw["error"].(map[string]interface{})["reason"],
					)
				}
				// A successful response might still contain errors for particular documents...
				//
			}

			// Close the response body, to prevent reaching the limit for goroutines or file handles
			//
			res.Body.Close()

			// Reset the buffer and items counter
			//
			buf.Reset()
			numItems = 0

		}

	}

	// Report the results: number of indexed docs, number of errors, duration, indexing rate
	//
	fmt.Print("\n")
	log.Println(strings.Repeat("▔", 65))

	dur := time.Since(start)

	if numErrors > 0 {
		log.Fatalf(
			"Indexed [%s] documents with [%s] errors in %s (%s docs/sec)",
			humanize.Comma(int64(numIndexed)),
			humanize.Comma(int64(numErrors)),
			dur.Truncate(time.Millisecond),
			humanize.Comma(int64(1000.0/float64(dur/time.Millisecond)*float64(numIndexed))),
		)
	} else {
		log.Printf(
			"Sucessfuly indexed [%s] documents in %s (%s docs/sec)",
			humanize.Comma(int64(numIndexed)),
			dur.Truncate(time.Millisecond),
			humanize.Comma(int64(1000.0/float64(dur/time.Millisecond)*float64(numIndexed))),
		)
	}
}

func Match(es *elasticsearch.Client, title string) {
	var mapResp map[string]interface{}
	var buf bytes.Buffer

	query := `{"query": {"match_phrase_prefix" : {"OriginalTitle":"inters"}},"size": 2}`

	// Concatenate a string from query for reading
	var b strings.Builder
	b.WriteString(query)
	read := strings.NewReader(b.String())

	// Attempt to encode the JSON query and look for errors
	if err := json.NewEncoder(&buf).Encode(read); err != nil {
		log.Fatalf("Error encoding query: %s", err)
	}

	res, err := es.Search(
		es.Search.WithContext(context.Background()),
		es.Search.WithIndex("movies"),
		es.Search.WithBody(read),
		es.Search.WithTrackTotalHits(true),
		es.Search.WithPretty(),
	)

	// Check for any errors returned by API call to Elasticsearch
	if err != nil {
		log.Fatalf("Elasticsearch Search() API ERROR:", err)
	} else {

		// Close the result body when the function call is complete
		defer res.Body.Close()

		// Decode the JSON response and using a pointer
		if err := json.NewDecoder(res.Body).Decode(&mapResp); err != nil {
			log.Fatalf("Error parsing the response body: %s", err)

			// If no error, then convert response to a map[string]interface
		} else {
			// Iterate the document "hits" returned by API call
			for _, hit := range mapResp["hits"].(map[string]interface{})["hits"].([]interface{}) {

				doc := hit.(map[string]interface{})
				source := doc["_source"]
				fmt.Println("_source:", source, "\n")
			}
		}
	}
}
