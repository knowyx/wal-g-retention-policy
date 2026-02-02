package main

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
)

type retentionPart struct {
	Name        string
	Value       int
	Description string
}

func getPaths() map[string]string {
	output := make(map[string]string)
	for i := 1; i < len(os.Args); i++ {
		if strings.Split(os.Args[i], "=")[0] == "--walg-path" {
			output["wal-g"] = strings.Split(os.Args[i], "=")[1]
		} else {
			if strings.Split(os.Args[i], "=")[0] == "--config-path" {
				output["config"] = strings.Split(os.Args[i], "=")[1]
			}
		}
	}
	_, existWalg := output["wal-g"]
	_, existConfig := output["config-path"]
	if !existWalg {
		output["wal-g"] = "wal-g"
	}
	if !existConfig {
		output["config-path"] = "./walg_policy.json"
	}
	return output
}

func readRetentionSettings(path string) []retentionPart {
	dataStr, err := os.ReadFile(path)
	if err != nil {
		log.Fatalf("Error while opening settings file at %s. Error: %s", path, err)
	}
	var dataFormated []retentionPart
	err = json.Unmarshal(dataStr, &dataFormated)
	if err != nil {
		log.Fatalf("Failed to unmarshal data from json %s. Error: %s", path, err)
	}
	return dataFormated
}

func updateDataWithArguments(data []retentionPart) []retentionPart {
	for i := 1; i < len(os.Args); i++ {
		for j := 0; j < len(data); j++ {
			if strings.Split(os.Args[i], "=")[0] == "--"+data[j].Name {
				value := strings.Split(os.Args[i], "=")[1]
				intVal, err := strconv.Atoi(value)
				if err != nil {
					log.Fatalf("Wrong data at the left side of string %s <-- here. Should be int, not %T."+
						" Error: %s", value, value, err)
				}
				data[j].Value = intVal
			}
		}
	}
	return data
}

func main() {
	logFile, err := os.OpenFile("wal-g-policy.log", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
	if err != nil {
		log.Fatalf("Failed to open log file: %s", err)
	}
	defer logFile.Close()
	log.SetOutput(logFile)
	fmt.Print("Welcome to wal-g-policy utility! It will help to to observe copyies of your db with policies\n")
	paths := getPaths()
	fileData := readRetentionSettings(paths["config-path"])
	retentionData := updateDataWithArguments(fileData)

	fmt.Printf("\nUsing this polycies: %v\n", retentionData)
}
