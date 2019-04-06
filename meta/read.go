package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
)

type GameObject struct {
	UUID string `json:"uuid"`
	Name string `json:"name"`
}

type File struct {
	GameObjects []GameObject `json:"game_objects"`
}

func main() {
	file, _ := ioutil.ReadFile("/tmp/game_objects.json")
	data := File{}
	err := json.Unmarshal([]byte(file), &data)
	if err != nil {
		fmt.Printf("error: %v\n", err)
		return
	}

	for _, obj := range data.GameObjects {
		fmt.Printf("%s %s\n", obj.UUID, obj.Name)
	}
}
