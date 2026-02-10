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
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"
)

// creating a stuct to unmarshal data from file
// type retentionPart struct {
// 	Name        string
// 	Value       int
// 	Description string
// }

type retentionSettings struct {
	RetentionCapacity int
	RetentionWindow   int
	CheckInterval     int
}

// // main func to delete backups
// func retentionPolicy1(data []retentionPart, paths map[string]string, checked map[string]bool) {
// 	// fmt.Printf("time is: %s, %v, %v, %v", time.Now(), data, paths, checked)
// 	// cmdo := "wal-g backup-list"
// 	// fmt.Print(cmdo)
// 	days := 0
// 	capacity := 0
// 	for i := 0; i < len(data); i++ {
// 		if data[i].Name == "retention-capacity" {
// 			capacity = data[i].Value
// 		}
// 		if data[i].Name == "retention-window" {
// 			days = data[i].Value
// 		}
// 	}
// 	saveAfter := time.Now().Add(-24 * time.Duration(days) * time.Hour)
// 	// args := " delete retain " + strconv.Itoa(capacity) + " --after " + saveAfter.Format(time.RFC3339) + " --confirm"
// 	args := []string{
// 		"delete",
// 		"retain", "FIND_FULL", strconv.Itoa(capacity),
// 		"--after", saveAfter.Format(time.RFC3339),
// 		"--confirm",
// 	}
// 	fmt.Printf("%s %s\n", paths["wal-g"], args)
// 	cmd := exec.Command(paths["wal-g"], args...)
// 	var out bytes.Buffer
// 	var stderr bytes.Buffer
// 	cmd.Stdout = &out
// 	cmd.Stderr = &stderr
// 	err := cmd.Run()
// 	fmt.Print(out)
// 	if err != nil {
// 		fmt.Println(fmt.Sprint(err) + ": " + stderr.String())
// 		return
// 	}

// 	args = []string{
// 		"delete",
// 		"retain", "FIND_FULL", strconv.Itoa(capacity),
// 		"--confirm",
// 	}

// 	fmt.Printf("%s %s\n", paths["wal-g"], args)
// 	cmd = exec.Command(paths["wal-g"], args...)

// 	cmd.Stdout = &out
// 	cmd.Stderr = &stderr
// 	err = cmd.Run()
// 	fmt.Print(out)
// 	if err != nil {
// 		fmt.Println(fmt.Sprint(err) + ": " + stderr.String())
// 		return
// 	}
// 	// fmt.Print(string(out))
// 	// if checked["hasCapacity"] && checked["hasWindow"] {
// 	// 	cmdo = "delete retain " + strconv.Itoa(data[0].Value) +
// 	// 		" --after " + strconv.Itoa() + " --confirm"
// 	// }
// 	// fmt.Printf("wal-g %s", cmdo)
// 	// output, err := exec.Command("wal-g", cmdo).Output()
// 	// if err != nil {
// 	// 	log.Fatalf("There is error while executing command. Error: %s", err)
// 	// }
// 	// fmt.Print(string(output))

// }

func settingsStrToIntConvertor(dataFromArgsAndFile map[string]string) (int, int, int) {
	retCapacity, err := strconv.Atoi(dataFromArgsAndFile["RetentionCapacity"])
	if err != nil {
		log.Fatalf("Failed to convert RetentionCapacity in args to int. Error: %s", err)
	}
	retWindow, err := strconv.Atoi(dataFromArgsAndFile["RetentionWindow"])
	if err != nil {
		log.Fatalf("Failed to convert RetentionWindow in args to int. Error: %s", err)
	}
	checkInterval, err := strconv.Atoi(dataFromArgsAndFile["CheckInterval"])
	if err != nil {
		log.Fatalf("Failed to convert CheckInterval in args to int. Error: %s", err)
	}
	return retCapacity, retWindow, checkInterval

}

func getBackupCnt(data map[string]string) int {
	// commandList := exec.Command(data["walg-path"], "backup-list")
	// commandTail := exec.Command("tail", "-n", "+2")
	// commandWc := exec.Command("wc", "-l")
	// listOut, err := commandList.StdoutPipe()
	// fmt.Print(1111)
	// if err != nil {
	// 	log.Fatalf("Error while getting backups list. "+
	// 		"Error: %s", err)
	// }
	// if err != nil {
	// 	log.Fatalf("Failed to start  getting backups list. "+
	// 		"Error: %s", err)
	// }
	// defer listOut.Close()
	// commandTail.Stdin = listOut
	// tailOut, err := commandTail.StdoutPipe()
	// if err != nil {
	// 	log.Fatalf("Error while tailing backups list. "+
	// 		"Error: %s", err)
	// }
	// defer tailOut.Close()
	// commandWc.Stdin = tailOut
	// out, err := commandWc.CombinedOutput()
	// if err != nil {
	// 	log.Fatalf("Error while getting word count of backups. "+
	// 		"Error: %s", err)
	// }
	// fmt.Print(out)
	cmd := exec.Command("/bin/sh", "-c", data["walg-path"]+" backup-list | tail -n +2 | wc -l")
	var out bytes.Buffer
	var stderr bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = &stderr
	err := cmd.Run()
	if err != nil {
		log.Fatalf("Failed to get amount of backups. "+
			"Error #1: %s, Error #2: %s)", err, stderr.String())
	}
	outString := strings.ReplaceAll(out.String(), "\n", "")
	outInt, err := strconv.Atoi(outString)
	if err != nil {
		log.Fatalf("Failed to convert amount of backups to the int class. Error: %s", err)
	}
	return outInt
}

func retentionPolicyChecker(data map[string]string, _ time.Time) {
	startBackupsAmount := getBackupCnt(data)
	capacity, window, _ := settingsStrToIntConvertor(data)
	if window > 0 {
		saveAfter := time.Now().Add(-24 * time.Duration(window) * time.Hour)
		args := []string{
			"delete",
			"retain", "FIND_FULL", strconv.Itoa(capacity),
			"--after", saveAfter.Format(time.RFC3339),
			"--confirm",
		}
		cmd := exec.Command(data["walg-path"], args...)
		var out bytes.Buffer
		var stderr bytes.Buffer
		cmd.Stdout = &out
		cmd.Stderr = &stderr
		err := cmd.Run()
		if err != nil {
			log.Fatalf("Error while checking retention poclicy (step 1: with window). "+
				"Error #1: %s, Error #2: %s)", err, stderr.String())
		}
	}
	args := []string{
		"delete",
		"retain", "FIND_FULL", strconv.Itoa(capacity),
		"--confirm",
	}
	cmd := exec.Command(data["walg-path"], args...)
	var out bytes.Buffer
	var stderr bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = &stderr
	err := cmd.Run()
	if err != nil {
		log.Fatalf("Error while checking retention poclicy (step 2: without window). "+
			"Error #1: %s, Error #2: %s)", err, stderr.String())
	}
	fmt.Printf("Check successfuly completed!\n")
	endBackupsAmount := getBackupCnt(data)
	if endBackupsAmount != startBackupsAmount {
		fmt.Printf("Deleted %d backups\n", startBackupsAmount-endBackupsAmount)
	} else {
		fmt.Print("No backups have been deleted\n")
	}
	log.Printf("Check completed. Deleted %d backups", startBackupsAmount-endBackupsAmount)
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

// // checking of data corretion in file and arguments
// func checker1(data []retentionPart, paths map[string]string) map[string]bool {
// 	var hasInterval, hasCapacity, hasWindow bool
// 	output := make(map[string]bool)
// 	for i := 0; i < len(data); i++ {
// 		if data[i].Name == "check-interval" {
// 			hasInterval = true
// 		}
// 		if data[i].Name == "retention-capacity" {
// 			hasCapacity = true
// 		}
// 		if data[i].Name == "retention-window" {
// 			hasWindow = true
// 		}
// 	}
// 	// without a interal user cant run program
// 	if !hasInterval {
// 		log.Fatalf("For using this utility you need to add a set to %s with parameters: "+
// 			"\"Name\" — \"check-interval\", \"Description\" — something as you "+
// 			"want and \"Value\" — time in hours to check backups", paths["config-path"])
// 	}
// 	// without at least 1 setting user cant run program
// 	if !hasCapacity && !hasWindow {
// 		log.Fatalf("For using this utility you need to add a least 1 set: retention-capacity"+
// 			" or retention-window to %s with parameters: "+
// 			"\"Name\" — \"retention-capacity\" or \"retention-window\", \"Description\" — something as you "+
// 			"want and \"Value\" — Amount of full copies stored at same time for retention-capacity or "+
// 			"amount of days to store a one copy for retention-window", paths["config-path"])
// 	}
// 	// without capacity or window user can run program (1 setting wil work)
// 	if hasCapacity {
// 		output["hasCapacity"] = true
// 	} else {
// 		output["hasCapacity"] = false
// 	}
// 	if hasWindow {
// 		output["hasWindow"] = true
// 	} else {
// 		output["hasWindow"] = false
// 	}
// 	return output
// }

func getWorkDir() string {
	dir, err := os.Getwd()
	if err != nil {
		log.Fatalf("Failed to get working directory. Error %s", err)
	}
	return dir
}

func checker(dataFromArgsAndFile map[string]string) {
	fmt.Print("Started checking... View logs for additional information\n")
	log.Print("Started checking")
	retCapacity, retWindow, checkInterval := settingsStrToIntConvertor(dataFromArgsAndFile)
	dir := getWorkDir()
	allName := strings.Split(os.Args[0], "/")
	filename := allName[len(allName)-1]
	if checkInterval <= 0 {
		log.Fatalf("For using this utility you need to add an interval between checks. "+
			"You can add it to file, located here: %s (like \"CheckInterval\": int > 0). "+
			"Or add it to the arguments: %s.go -CheckInterval=int > 0",
			dir+"/"+dataFromArgsAndFile["cfg-path"], filename)
	}
	log.Print("Check checkInterval > 0 passed!")
	if retCapacity <= 0 {
		log.Fatalf("For using this utility you need to add an amount of backups stored at "+
			"the same time (RetentionCapacity). "+
			"You can add it to file, located here: %s (like \"RetentionCapacity\": "+
			"int (piece for RetentionCapacity) > 0). "+
			"Or add it to the arguments, like this: %s.go -RetentionCapacity=int > 0",
			dir+"/"+dataFromArgsAndFile["cfg-path"], filename)
	}
	log.Print("Check retCapacity > 0 passed!")
	if retWindow <= 0 {
		log.Print("Check retWindow > 0 not passed!")
		fmt.Print("You may add a RetentionWindow setting (it will delete backups that created " +
			"before date) in file (\"RetentionWindow\": int > 0) or program argumets " +
			"(-RetentionWindow=int > 0).")
	} else {
		log.Print("Check retWindow > 0 passed!")
	}
	fmt.Print("The checks were completed successfully!\n")
}

// function to print a help
func printHelp(dataFromArgsAndFile map[string]string) {
	fmt.Print("This utility will check the correction of your PostgreSQL backups and keep it \"in the same view\".\n")
	dir := getWorkDir()
	fmt.Printf("Program will use this policyes from file: %s and program arguments.\n", dir+"/"+dataFromArgsAndFile["cfg-path"])
	allName := strings.Split(os.Args[0], "/")
	filename := allName[len(allName)-1]
	fmt.Printf("To run the program use:\n%s.go with args:\n-help (to open this menu)\n-walg-path (set path to your wal-g) "+
		"(default: 'wal-g')\n-config-path (set path to your file with retention settings) "+
		"(default: 'walg_policy.json')\n-retention-capacity=1 "+
		"(or similar from json, if you need to set a specific value)\n", filename)
	log.Print("================ End of executing here ================")
}

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
	retCapacityFromArgs, retWindowFromArgs, checkIntervalFromArgs := settingsStrToIntConvertor(dataFromArgs)
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

func tickerSetup(intervalStr string) time.Ticker {
	interval, err := strconv.Atoi(intervalStr)
	if err != nil {
		log.Fatalf("Failed to convert CheckInterval to int. Error: %s", err)
	}
	ticker := time.NewTicker(time.Duration(interval) * time.Second) // SECONDS, CHANGE TO HOURS
	log.Printf("Ticker setup is successful")
	return *ticker
}

func main() {
	//setup log file
	logFile, err := os.OpenFile("wal-g-policy.log", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
	if err != nil {
		log.Fatalf("Failed to open log file: %s", err)
	}
	defer logFile.Close()
	log.SetOutput(logFile)
	// interrupt signal setup
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	//main executing
	log.Print("================ Start of executing here ================")
	dir := getWorkDir()
	log.Printf("Working directory: %s", dir)
	fmt.Print("Welcome to wal-g-policy utility! It will help to to observe copyies of your db with policies.\n")
	dataFromArgs := arguments_getting()
	//if help is requested, print it, if no, running
	if dataFromArgs["help"] == "true" {
		log.Print("Enter help mode")
		printHelp(dataFromArgs)
	} else {
		dataFromArgsAndFile := readRetentionSettings(dataFromArgs)
		checker(dataFromArgsAndFile)
		log.Printf("All is OK. Ready to run program")
		ticker := tickerSetup(dataFromArgsAndFile["CheckInterval"])

		go func() {
			for t := range ticker.C {
				retentionPolicyChecker(dataFromArgsAndFile, t)
			}
		}()
	}
	<-sigChan
	log.Printf("Recived shutdown signal")
	fmt.Print("\nShutting down gracefully...\n")
	log.Print("================ End of executing here ================")
}
