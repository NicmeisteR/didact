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

type LeaderPower struct {
	UUID   string `json:"uuid"`
	TypeID string `json:"type_id"`
}

type File struct {
	GameObjects  []GameObject  `json:"game_objects"`
	LeaderPowers []LeaderPower `json:"leader_powers"`
}

func main() {
	file, _ := ioutil.ReadFile("/tmp/leader_powers.json")
	data := File{}
	err := json.Unmarshal([]byte(file), &data)
	if err != nil {
		fmt.Printf("error: %v\n", err)
		return
	}

	for _, obj := range data.GameObjects {
		fmt.Printf("INSERT INTO meta_object(mo_id, mo_uuid) VALUES ('%s', '%s'::UUID) ON CONFLICT(mo_id) DO UPDATE SET mo_uuid='%s'::UUID;\n", obj.Name, obj.UUID, obj.UUID)
	}

	for _, lp := range data.LeaderPowers {
		fmt.Printf("INSERT INTO meta_leader_power(mlp_id, mlp_uuid) VALUES ('%s', '%s'::UUID) ON CONFLICT(mlp_id) DO UPDATE SET mlp_uuid='%s'::UUID;\n", lp.TypeID, lp.UUID, lp.UUID)
	}
}
