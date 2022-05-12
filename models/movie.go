package models

type Movie struct {
	ID            int
	OriginalTitle string
	Genres        []ValueType
}

type ValueType struct {
	ID   int    `json:"id"`
	Name string `json:"name"`
}
