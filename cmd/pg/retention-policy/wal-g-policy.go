package main

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
)

type retention_part struct {
	Name          string
	Default_value int
	Description   string
}

func get_paths() map[string]string {
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
	_, exist_walg := output["wal-g"]
	_, exist_config := output["config-path"]
	if exist_walg == false {
		output["wal-g"] = "wal-g"
	}
	if exist_config == false {
		output["config-path"] = "./walg_policy.json"
	}
	return output
}

func read_retention_settings(path string) []retention_part {
	data_str, err := os.ReadFile(path)
	if err != nil {
		log.Fatalf("Error while opening settings file: %s", err)
	}
	var data_formated []retention_part
	json.Unmarshal(data_str, &data_formated)
	return data_formated
}

func update_data_with_arguments(data []retention_part) []retention_part {
	for i := 1; i < len(os.Args); i++ {
		for j := 0; j < len(data); j++ {
			if strings.Split(os.Args[i], "=")[0] == "--"+data[j].Name {
				value := strings.Split(os.Args[i], "=")[1]
				int_val, err := strconv.Atoi(value)
				if err != nil {
					log.Fatal(err)
				}
				data[j].Default_value = int_val
			}
		}
	}
	return data
}

func main() {
	//read file
	//file, err := os.Open(os.Args[2])
	// if err != nil {
	// 	fmt.Println(err)
	// 	os.Exit(1)
	// }
	// defer file.Close()

	// data := make([]byte, 64)

	// var fileData string
	// for {
	// 	_, err := file.Read(data)
	// 	if err == io.EOF {
	// 		break
	// 	}
	// 	// fileData += string(data[:n])
	// }
	logFile, err := os.OpenFile("wal-g-policy.log", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
	if err != nil {
		log.Fatal("Failed to open log file:", err)
	}
	defer logFile.Close()
	log.SetOutput(logFile)
	fmt.Print("Welcome to wal-g-policy utility! It will help to to observe copyies of your db with policies\n")
	paths := get_paths()
	file_data := read_retention_settings(paths["config-path"])
	retention_data := update_data_with_arguments(file_data)

	fmt.Printf("\nUsing this polycies: %v\n", retention_data)

	// // if json -> work with json unmarshal
	// arg1 := os.Args[1]
	// if arg1 == "json=1" {

	// } else {
	// 	//else work with text
	// 	os.Stdout.Write(data)
	// }

	// //print args
	// fmt.Println(os.Args)
}
