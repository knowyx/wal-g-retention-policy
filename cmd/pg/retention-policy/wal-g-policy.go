package main

// importing depencies
import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"time"
)

// creating a stuct to unmarshal data from file
type retentionPart struct {
	Name        string
	Value       int
	Description string
}

type retentionSettings struct {
	RetentionCapacity int
	RetentionWindow   int
	CheckInterval     int
}

// main func to delete backups
func retentionPolicy(data []retentionPart, paths map[string]string, checked map[string]bool) {
	// fmt.Printf("time is: %s, %v, %v, %v", time.Now(), data, paths, checked)
	// cmdo := "wal-g backup-list"
	// fmt.Print(cmdo)
	days := 0
	capacity := 0
	for i := 0; i < len(data); i++ {
		if data[i].Name == "retention-capacity" {
			capacity = data[i].Value
		}
		if data[i].Name == "retention-window" {
			days = data[i].Value
		}
	}
	saveAfter := time.Now().Add(-24 * time.Duration(days) * time.Hour)
	// args := " delete retain " + strconv.Itoa(capacity) + " --after " + saveAfter.Format(time.RFC3339) + " --confirm"
	args := []string{
		"delete",
		"retain", "FIND_FULL", strconv.Itoa(capacity),
		"--after", saveAfter.Format(time.RFC3339),
		"--confirm",
	}
	fmt.Printf("%s %s\n", paths["wal-g"], args)
	cmd := exec.Command(paths["wal-g"], args...)
	var out bytes.Buffer
	var stderr bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = &stderr
	err := cmd.Run()
	fmt.Print(out)
	if err != nil {
		fmt.Println(fmt.Sprint(err) + ": " + stderr.String())
		return
	}

	args = []string{
		"delete",
		"retain", "FIND_FULL", strconv.Itoa(capacity),
		"--confirm",
	}

	fmt.Printf("%s %s\n", paths["wal-g"], args)
	cmd = exec.Command(paths["wal-g"], args...)

	cmd.Stdout = &out
	cmd.Stderr = &stderr
	err = cmd.Run()
	fmt.Print(out)
	if err != nil {
		fmt.Println(fmt.Sprint(err) + ": " + stderr.String())
		return
	}
	// fmt.Print(string(out))
	// if checked["hasCapacity"] && checked["hasWindow"] {
	// 	cmdo = "delete retain " + strconv.Itoa(data[0].Value) +
	// 		" --after " + strconv.Itoa() + " --confirm"
	// }
	// fmt.Printf("wal-g %s", cmdo)
	// output, err := exec.Command("wal-g", cmdo).Output()
	// if err != nil {
	// 	log.Fatalf("There is error while executing command. Error: %s", err)
	// }
	// fmt.Print(string(output))

}

func arguments_getting() map[string]string {
	output := make(map[string]string)
	walgPath := flag.String("walg-path", "wal-g", "a name to your wal-g executable or symlink")
	cfgPath := flag.String("config-path", "walg_policy.json", "a path to your config file")
	retCapacity := flag.Int("RetentionCapacity", -1, "amount of copies stored at the same time. "+
		"Will be used instead of value in file")
	retWindow := flag.Int("RetentionWindow", -1, "time (in days) to store a backup. "+
		"Will be used instead of value in file")
	checkInterval := flag.Int("CheckInterval", -1, "delay (in hours) between checking. "+
		"Will be used instead of value in file")
	helpMode := flag.Bool("help", false, "get help with using a program")
	flag.Parse()
	output["walg-path"] = *walgPath
	output["cfg-path"] = *cfgPath
	output["RetentionCapacity"] = strconv.Itoa(*retCapacity)
	output["RetentionWindow"] = strconv.Itoa(*retWindow)
	output["CheckInterval"] = strconv.Itoa(*checkInterval)
	output["help"] = strconv.FormatBool(*helpMode)
	return output
}

// checking of data corretion in file and arguments
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
	// without a interal user cant run program
	if !hasInterval {
		log.Fatalf("For using this utility you need to add a set to %s with parameters: "+
			"\"Name\" — \"check-interval\", \"Description\" — something as you "+
			"want and \"Value\" — time in hours to check backups", paths["config-path"])
	}
	// without at least 1 setting user cant run program
	if !hasCapacity && !hasWindow {
		log.Fatalf("For using this utility you need to add a least 1 set: retention-capacity"+
			" or retention-window to %s with parameters: "+
			"\"Name\" — \"retention-capacity\" or \"retention-window\", \"Description\" — something as you "+
			"want and \"Value\" — Amount of full copies stored at same time for retention-capacity or "+
			"amount of days to store a one copy for retention-window", paths["config-path"])
	}
	// without capacity or window user can run program (1 setting wil work)
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

// function to print a help
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

// getting path from args or set standart
// func getPaths() map[string]string {
// 	output := make(map[string]string)
// 	for i := 1; i < len(os.Args); i++ {
// 		if strings.Split(os.Args[i], "=")[0] == "--help" {
// 			output["helpmode"] = "help"
// 		}
// 		if strings.Split(os.Args[i], "=")[0] == "--walg-path" {
// 			output["wal-g"] = strings.Split(os.Args[i], "=")[1]
// 		} else {
// 			if strings.Split(os.Args[i], "=")[0] == "--config-path" {
// 				output["config"] = strings.Split(os.Args[i], "=")[1]
// 			}
// 		}
// 	}
// 	_, existWalg := output["wal-g"]
// 	_, existConfig := output["config-path"]
// 	if !existWalg {
// 		output["wal-g"] = "wal-g"
// 	}
// 	if !existConfig {
// 		output["config-path"] = "./walg_policy.json"
// 	}
// 	return output
// }

// reading a settings file, update data with file values
func readRetentionSettings(dataFromArgs map[string]string) map[string]string {
	//reading file
	fileData, err := os.ReadFile(dataFromArgs["cfg-path"])
	if err != nil {
		log.Fatalf("Error while opening settings file at %s. Error: %s", dataFromArgs["cfg-path"], err)
	}
	//unmarshal file from json into struct retentionSettings
	var fileDataFormated []retentionSettings
	err = json.Unmarshal(fileData, &fileDataFormated)
	if err != nil {
		log.Fatalf("Failed to unmarshal data from json %s. Error: %s", dataFromArgs["cfg-path"], err)
	}
	log.Printf("Unmarshaled json successfuly, data: %v", fileDataFormated)
	//getting a values from dataFromArgs and converting it to a int
	retCapacityFromArgs, err := strconv.Atoi(dataFromArgs["RetentionCapacity"])
	if err != nil {
		log.Fatalf("Failed to convert RetentionCapacity in args to int. Error: %s", err)
	}
	retWindowFromArgs, err := strconv.Atoi(dataFromArgs["RetentionWindow"])
	if err != nil {
		log.Fatalf("Failed to convert RetentionWindow in args to int. Error: %s", err)
	}
	checkIntervalFromArgs, err := strconv.Atoi(dataFromArgs["CheckInterval"])
	if err != nil {
		log.Fatalf("Failed to convert CheckInterval in args to int. Error: %s", err)
	}
	//check if args values contain a valid value (-1 be default), if yes, pass, if no, write value from file
	if retCapacityFromArgs <= 0 {
		dataFromArgs["RetentionCapacity"] = strconv.Itoa(fileDataFormated[0].RetentionCapacity)
		log.Printf("Using RetentionCapacity value from file. Value: %s", dataFromArgs["RetentionCapacity"])
	} else {
		log.Printf("Using RetentionCapacity value from args. Value: %s", dataFromArgs["RetentionCapacity"])
	}
	if retWindowFromArgs <= 0 {
		dataFromArgs["RetentionWindow"] = strconv.Itoa(fileDataFormated[0].RetentionWindow)
		log.Printf("Using RetentionWindow value from file. Value: %s", dataFromArgs["RetentionWindow"])
	} else {
		log.Printf("Using RetentionWindow value from args. Value: %s", dataFromArgs["RetentionWindow"])
	}
	if checkIntervalFromArgs <= 0 {
		dataFromArgs["CheckInterval"] = strconv.Itoa(fileDataFormated[0].CheckInterval)
		log.Printf("Using CheckInterval value from file. Value: %s", dataFromArgs["CheckInterval"])
	} else {
		log.Printf("Using CheckInterval value from args. Value: %s", dataFromArgs["CheckInterval"])
	}
	return dataFromArgs
}

// if arguments have additional data, update the file data with it
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
	dataFromArgsAndFile := arguments_getting()
	if dataFromArgsAndFile["help"] == "true" {
		log.Print("Enter help mode")
		// printHelp(argsData)
	} else {
		asd := readRetentionSettings(dataFromArgsAndFile)
		fmt.Print("\n")
		fmt.Print(asd)
	}

	// paths := getPaths()
	//
	// retentionData := updateDataWithArguments(fileData)

	// _, existHelp := paths["helpmode"]
	// if existHelp {
	// 	log.Print("Enter help mode")
	// 	printHelp(retentionData, paths)
	// } else {
	// 	log.Printf("Using this values. Path to wal-g: %s, path to settings file: %s", paths["wal-g"], paths["config-path"])
	// 	fmt.Print("Using this polycies:\n")
	// 	for i := 0; i < len(retentionData); i++ {
	// 		log.Printf("Retention policy #%d. With name: %s and value: %d", i+1, retentionData[i].Name, retentionData[i].Value)
	// 		fmt.Printf("#%d. %s, value: %d\n", i+1, retentionData[i].Name, retentionData[i].Value)
	// 	}
	// 	cheked := checker(retentionData, paths)
	// 	var interval int
	// 	for j := 0; j < len(retentionData); j++ {
	// 		if retentionData[j].Name == "check-interval" {
	// 			interval = retentionData[j].Value
	// 		}
	// 	}
	// 	for {
	// 		go retentionPolicy(retentionData, paths, cheked)
	// 		time.Sleep(time.Duration(interval) * time.Second) //TIME IN SECONDS, NOT IN HOURS
	// 	}
	// }
	// log.Print("================ End of executing here ================")
}
