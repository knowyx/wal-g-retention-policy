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

// struct to store a retention settings
type retentionSettings struct {
	RetentionCapacity int
	RetentionWindow   int
	CheckInterval     int
}

// get settings from map and convert it to the int claass
func settingsStrToIntConvertor(dataFromArgsAndFile map[string]string) (int, int, int) {
	// get capacity
	retCapacity, err := strconv.Atoi(dataFromArgsAndFile["RetentionCapacity"])
	if err != nil {
		log.Fatalf("Failed to convert RetentionCapacity in args to int. Error: %s", err)
	}
	// get window
	retWindow, err := strconv.Atoi(dataFromArgsAndFile["RetentionWindow"])
	if err != nil {
		log.Fatalf("Failed to convert RetentionWindow in args to int. Error: %s", err)
	}
	// get interval
	checkInterval, err := strconv.Atoi(dataFromArgsAndFile["CheckInterval"])
	if err != nil {
		log.Fatalf("Failed to convert CheckInterval in args to int. Error: %s", err)
	}
	return retCapacity, retWindow, checkInterval
}

// run shell command to get the list of backups, then count it
func getBackupCnt(data map[string]string) int {
	// get shell path
	shell := os.Getenv("SHELL")
	if shell == "" {
		shell = "/bin/sh"
	}
	// executing of command
	cmd := exec.Command(shell, "-c", data["walg-path"]+" backup-list | tail -n +2 | wc -l")
	var out bytes.Buffer
	var stderr bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = &stderr
	err := cmd.Run()
	if err != nil {
		log.Fatalf("Failed to get amount of backups. "+
			"Error #1: %s, Error #2: %s)", err, stderr.String())
	}
	//remove new line at the end and convert to the int
	outString := strings.ReplaceAll(out.String(), "\n", "")
	outInt, err := strconv.Atoi(outString)
	if err != nil {
		log.Fatalf("Failed to convert amount of backups to the int class. Error: %s", err)
	}
	return outInt
}

// main retention checker loop
func retentionPolicyChecker(data map[string]string, _ time.Time) {
	// get additional data
	startBackupsAmount := getBackupCnt(data)
	capacity, window, _ := settingsStrToIntConvertor(data)
	// run command two times, because walg ignores capacity value
	if window > 0 {
		// setup time min creation date and time for saved backups
		saveAfter := time.Now().Add(-24 * time.Duration(window) * time.Hour)
		// executuing command
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
	// executing command #2
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
	// print info and log
	fmt.Printf("Check successfully completed!\n")
	endBackupsAmount := getBackupCnt(data)
	if endBackupsAmount != startBackupsAmount {
		fmt.Printf("Deleted %d backup(s)\n", startBackupsAmount-endBackupsAmount)
	} else {
		fmt.Print("No backups have been deleted\n")
	}
	log.Printf("Check completed. Deleted %d backup(s)", startBackupsAmount-endBackupsAmount)
}

// getting arguments with flag lib
func argumentsGetting() map[string]string {
	// setupf flags to parse
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
	// parse all and convert to the map
	flag.Parse()
	output["walg-path"] = *walgPath
	output["cfg-path"] = *cfgPath
	output["RetentionCapacity"] = strconv.Itoa(*retCapacity)
	output["RetentionWindow"] = strconv.Itoa(*retWindow)
	output["CheckInterval"] = strconv.Itoa(*checkInterval)
	output["help"] = strconv.FormatBool(*helpMode)
	return output
}

// get dir of executing. used in logging
func getWorkDir() string {
	dir, err := os.Getwd()
	if err != nil {
		log.Fatalf("Failed to get working directory. Error %s", err)
	}
	return dir
}

// check if checkInterval and retentionCapacity settings not empty, print warn if window is empty
func checker(dataFromArgsAndFile map[string]string) {
	// print user info and get data
	fmt.Print("Started checking... View logs for additional information\n")
	log.Print("Started checking")
	retCapacity, retWindow, checkInterval := settingsStrToIntConvertor(dataFromArgsAndFile)
	dir := getWorkDir()
	allName := strings.Split(os.Args[0], "/")
	filename := allName[len(allName)-1]
	// check if interval is empty and end executing
	if checkInterval <= 0 {
		log.Fatalf("For using this utility you need to add an interval between checks. "+
			"You can add it to file, located here: %s (like \"CheckInterval\": int > 0). "+
			"Or add it to the arguments: %s.go -CheckInterval=int > 0",
			dir+"/"+dataFromArgsAndFile["cfg-path"], filename)
	}
	log.Print("Check checkInterval > 0 passed!")
	// check if capacity is empty and end executing
	if retCapacity <= 0 {
		log.Fatalf("For using this utility you need to add an amount of backups stored at "+
			"the same time (RetentionCapacity). "+
			"You can add it to file, located here: %s (like \"RetentionCapacity\": "+
			"int (piece for RetentionCapacity) > 0). "+
			"Or add it to the arguments, like this: %s.go -RetentionCapacity=int > 0",
			dir+"/"+dataFromArgsAndFile["cfg-path"], filename)
	}
	log.Print("Check retCapacity > 0 passed!")
	// check if window is empty and end executing
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
	fmt.Print("This utility will check the correction of your PostgreSQL backups and keep it " +
		"\"in the same view\".\n")
	dir := getWorkDir()
	fmt.Printf("Program will use this policyes from file: %s and program arguments.\n", dir+
		"/"+dataFromArgsAndFile["cfg-path"])
	// get path to the programm
	allName := strings.Split(os.Args[0], "/")
	filename := allName[len(allName)-1]
	fmt.Printf("To run the program use:\n%s.go with args:\n-help (to open this menu)\n-walg-path "+
		"(set path to your wal-g) "+
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
	log.Printf("Unmarshaled json successfully, data: %v", fileDataFormated)
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

// setup ticker with checkInterval value
func tickerSetup(intervalStr string) time.Ticker {
	interval, err := strconv.Atoi(intervalStr)
	if err != nil {
		log.Fatalf("Failed to convert CheckInterval to int. Error: %s", err)
	}
	ticker := time.NewTicker(time.Duration(interval) * time.Hour)
	log.Printf("Ticker setup is successful")
	return *ticker
}

func main() {
	// setup log file
	logFile, err := os.OpenFile("wal-g-policy.log", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
	if err != nil {
		log.Fatalf("Failed to open log file: %s", err)
	}
	defer logFile.Close()
	log.SetOutput(logFile)
	// interrupt signal setup
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	// main executing
	log.Print("================ Start of executing here ================")
	dir := getWorkDir()
	log.Printf("Working directory: %s", dir)
	fmt.Print("Welcome to wal-g-policy utility! It will help to to observe copyies of your db " +
		"with policies.\n")
	dataFromArgs := argumentsGetting()
	// if help is requested, print it, if no, running
	if dataFromArgs["help"] == "true" {
		log.Print("Enter help mode")
		printHelp(dataFromArgs)
	} else {
		dataFromArgsAndFile := readRetentionSettings(dataFromArgs)
		checker(dataFromArgsAndFile)
		log.Printf("All is OK. Ready to run program")
		ticker := tickerSetup(dataFromArgsAndFile["CheckInterval"])
		// run goroutine for main executing
		go func() {
			for t := range ticker.C {
				retentionPolicyChecker(dataFromArgsAndFile, t)
			}
		}()
	}
	// if terminate signal triggered
	<-sigChan
	log.Printf("Received shutdown signal")
	fmt.Print("\nShutting down gracefully...\n")
	log.Print("================ End of executing here ================")
}
