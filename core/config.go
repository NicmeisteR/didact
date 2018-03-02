package core

import (
	"bytes"
	"encoding/json"
	"io/ioutil"
	"os"
	"strconv"
	"strings"
)

type DBConfig struct {
	Host     *string `json:"Host"`
	Port     *int    `json:"Port"`
	User     *string `json:"User"`
	Password *string `json:"Password"`
	DBName   *string `json:"DBName"`
}

type APIKeyConfig struct {
	Key  string `json:"Key"`
	Rate int    `json:"Rate"`
}

type CrawlerConfig struct {
	Keys          []APIKeyConfig `json:"Keys"`
	BlockDuration int            `json:"BlockDuration"`
	TaskWorkers   int            `json:"TaskWorkers"`
}

type BotConfig struct {
	Key string `json:"Key"`
}

type Config struct {
	Crawler  CrawlerConfig `json:"Crawler"`
	Database DBConfig      `json:"Database"`
	Bot      BotConfig     `json:"Bot"`
}

func (dbc *DBConfig) GetConnectionString(obfuscate bool) string {
	var buffer bytes.Buffer
	if dbc.Host != nil {
		buffer.WriteString(" host=")
		buffer.WriteString(*dbc.Host)
	}
	if dbc.Port != nil {
		buffer.WriteString(" port=")
		buffer.WriteString(strconv.Itoa(*dbc.Port))
	}
	if dbc.User != nil {
		buffer.WriteString(" user='")
		buffer.WriteString(*dbc.User)
		buffer.WriteString("'")
	}
	if dbc.Password != nil {
		buffer.WriteString(" password='")
		if obfuscate {
			buffer.WriteString(strings.Repeat("*", len(*dbc.Password)))
		} else {
			buffer.WriteString(*dbc.Password)
		}
		buffer.WriteString("'")
	}
	if dbc.DBName != nil {
		buffer.WriteString(" dbname='")
		buffer.WriteString(*dbc.DBName)
		buffer.WriteString("'")
	}
	return buffer.String()
}

func ParseConfigFile(path string) (*Config, error) {
	configFile, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer configFile.Close()
	configBuffer, err := ioutil.ReadAll(configFile)
	if err != nil {
		return nil, err
	}

	config := new(Config)
	err = json.Unmarshal(configBuffer, config)
	if err != nil {
		return nil, err
	}
	return config, nil
}
