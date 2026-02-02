package main

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"time"
)

type retentionPart struct {
	Name        string
	Value       int
	Description string
}

func retentionPolicy(data []retentionPart, paths map[string]string, checked map[string]bool) {
	fmt.Printf("time is: %s, %v, %v, %v", time.Now(), data, paths, checked)
}

func checker(data []retentionPart, paths map[string]string) map[string]bool {
	var hasInterval, hasCapacity, hasWindow bool
	output := make(map[string]bool)
	for i := 0; i < len(data); i++ {
		if data[i].Name == "check-interval" {
			hasInterval = true
		}
		if data[i].Name == "retention-capacity" {
			hasCapacity = true
		}
		if data[i].Name == "retention-window" {
			hasWindow = true
		}
	}
	if !hasInterval {
		log.Fatalf("For using this utility you need to add a set to %s with parameters: "+
			"\"Name\" — \"check-interval\", \"Description\" — something as you "+
			"want and \"Value\" — time in hours to check backups", paths["config-path"])
	}
	if hasCapacity {
		output["hasCapacity"] = true
	} else {
		output["hasCapacity"] = false
	}
	if hasWindow {
		output["hasWindow"] = true
	} else {
		output["hasWindow"] = false
	}
	return output
}

func printHelp(data []retentionPart, paths map[string]string) {
	fmt.Print("This utility will check the correction of your PostgreSQL backups and keep it \"in the same view\"\n")
	fmt.Printf("Program will use this policyes from file: %s and program arguments:\n", paths["config-path"])
	for i := 0; i < len(data); i++ {
		fmt.Printf("#%d. %s, value: %d, description: %s\n", i+1, data[i].Name, data[i].Value, data[i].Description)
	}
	splitedArgs := strings.Split(os.Args[0], "/")
	fmt.Printf("To run program use:\n%s.go --help (to open this menu) --walg-path (set path to your wal-g) "+
		"(default: 'wal-g') --config-path (set path to your file with retention settings) "+
		"(default: '/walg_policy.json') --retention-capacity=1 "+
		"(or similar from json, if you need to setup specific value)", splitedArgs[len(splitedArgs)-1])
}

func getPaths() map[string]string {
	output := make(map[string]string)
	for i := 1; i < len(os.Args); i++ {
		if strings.Split(os.Args[i], "=")[0] == "--help" {
			output["helpmode"] = "help"
		}
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
	//setup log file
	_, err := os.Create("wal-g-policy.log")
	if err != nil {
		log.Fatalf("Failed to create log file: %s", err)
	}
	logFile, err := os.OpenFile("wal-g-policy.log", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
	if err != nil {
		log.Fatalf("Failed to open log file: %s", err)
	}
	defer logFile.Close()
	log.SetOutput(logFile)

	//main executing
	log.Print("================ Start of executing here ================")
	dir, err := os.Getwd()
	if err != nil {
		log.Fatalf("Failed to get working directory. Error %s", err)
	}
	log.Printf("Working directory: %s", dir)
	fmt.Print("Welcome to wal-g-policy utility! It will help to to observe copyies of your db with policies\n")
	paths := getPaths()
	fileData := readRetentionSettings(paths["config-path"])
	retentionData := updateDataWithArguments(fileData)
	_, existHelp := paths["helpmode"]
	if existHelp {
		log.Print("Enter help mode")
		printHelp(retentionData, paths)
	} else {
		log.Printf("Using this values. Path to wal-g: %s, path to settings file: %s", paths["wal-g"], paths["config-path"])
		fmt.Print("Using this polycies:\n")
		for i := 0; i < len(retentionData); i++ {
			log.Printf("Retention policy #%d. With name: %s and value: %d", i+1, retentionData[i].Name, retentionData[i].Value)
			fmt.Printf("#%d. %s, value: %d\n", i+1, retentionData[i].Name, retentionData[i].Value)
		}
		cheked := checker(retentionData, paths)
		var interval int
		for j := 0; j < len(retentionData); j++ {
			if retentionData[j].Name == "check-interval" {
				interval = retentionData[j].Value
			}
		}
		for {
			go retentionPolicy(retentionData, paths, cheked)
			time.Sleep(time.Duration(interval) * time.Second) //TIME IN SECONDS, NOT IN HOURS
		}
	}
	log.Print("================ End of executing here ================")
}
