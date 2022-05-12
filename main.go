package main

import (
	"encoding/csv"
	"fmt"
	"io/ioutil"
	"log"
	"os"

	"github.com/dogukanuhn/es-golang/parser"
	"github.com/elastic/go-elasticsearch/v8"
	"github.com/joho/godotenv"
)

func initIndex(es *elasticsearch.Client) {
	file, err := os.Open("movie.csv")

	if err != nil {
		log.Fatalln("Error Opening: %s", err.Error())
	}

	defer file.Close()

	csvReader := csv.NewReader(file)
	csvReader.FieldsPerRecord = -1
	data, err := csvReader.ReadAll()

	fmt.Println("total no of rows:", len(data))

	if err != nil {
		log.Fatal(err)
	}

	movieList := parser.MovieParse(data)

	parser.IndexData(es, movieList)
}
func main() {
	log.SetFlags(0)
	godotenv.Load()

	cert, err := ioutil.ReadFile("http_ca.crt")

	if err != nil {
		log.Fatalf(err.Error())
	}

	es, err := elasticsearch.NewClient(elasticsearch.Config{
		Addresses: []string{
			"https://localhost:9200",
		},
		CACert:   cert,
		Username: os.Getenv("user"),
		Password: os.Getenv("pass"),
	})

	// initIndex(es)

	parser.Match(es, "inters")
}
