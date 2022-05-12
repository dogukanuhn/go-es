package parser

import (
	"encoding/json"
	"log"
	"strings"
	"time"

	"github.com/dogukanuhn/es-golang/models"
)

func MovieParse(csvData [][]string) []models.Movie {

	movieList := make([]models.Movie, 0, len(csvData))
	start := time.Now().UTC()
	for i, line := range csvData {

		if i > 0 {

			var movie models.Movie

			var value []models.ValueType
			movie.ID = i - 1
			movie.OriginalTitle = line[8]

			formatedLine := strings.Replace(line[3], `'`, `"`, -1)

			json.Unmarshal([]byte(formatedLine), &value)

			movie.Genres = value

			movieList = append(movieList, movie)
		}
	}

	dur := time.Since(start)

	log.Print(dur.Truncate(time.Millisecond))

	return movieList
}
