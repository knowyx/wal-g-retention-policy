# WAL-G Retention Policy Utility
![Docker-tests-status](https://github.com/knowyx/wal-g-retention-policy/workflows/Docker%20tests/badge.svg)
![Unit-tests-status](https://github.com/knowyx/wal-g-retention-policy/workflows/Unit%20tests/badge.svg)

![You can also view the oficial docs of wal-g](https://github.com/knowyx/wal-g-retention-policy/blob/master/docs/README.md)
![This is fork of official wal-g repository](https://github.com/wal-g/wal-g)

WAL-G retention policy utility will help you kepp control on your backups.
Key features are:
- Removing old backups
- Setting a cap, after that backups will remove
- Automatic work

**Table of Contents**
- [Installation](#installation)
- [Configuration](#configuration)
- [Usage](#usage)
- [Debug](#debug)


Installation and running
----------
Installation of this utility is not more harder, then installation of wal-g. Just clone this repo, go to:
```
wal-g/cmd/pg/retention-policy
```
and use:
```
go run wal-g-policy.go
```
you can also add some attributes:
- walg-path — path to your wal-g executable or symlink (by defaut: wal-g)
- config-path — path to your json config file (by defaut: walg_policy.json)

Configuration
-------------

There are two ways how you can configure WAL-G Retention Policy:

1. Using a file
This way is good to store constant values, that wont be changed for a log time.
Base config file location is:
```
wal-g/cmd/pg/retention-policy/walg_policy.json
```
Lets take a look at paramets of this file
- CheckInterval — sets a time (in hours) after with program will execute command, that will remove old and excessive backups (by default: 1)
- RetentionWindow — sets a max time (in days) to store a backup (by default: 7)
- RetentionCapacity — sets a max amount of backups, stored at one time (by default: 4)
2. Using an arguments 
This way is good then you need rapidly change the value for a short time. You can use all the parametrs from point 1 simply add:
```
...go -CheckInterval=2
```
This values have more priority, then values in file

Usage
-------------
After running a program (see [Installation](#installation)), it will do all automaticly. You can contol it with reading a terminal and view log file located in the same direcory:
to:
```
wal-g/cmd/pg/retention-policy/wal-g-policy.log
```
You can safly close the program by sending an intterupt signal

Debug
-------------
Log (see [Usage](#usage)) will tell all you want to know. All error and statistics are stored here. For example (in successful running case):
```
2026/02/11 00:09:18 ================ Start of executing here ================
2026/02/11 00:09:18 Working directory: /home/user/wal-g/cmd/pg/retention-policy
2026/02/11 00:09:18 Unmarshaled json successfuly, data: [{3 7 10}]
2026/02/11 00:09:18 Using RetentionCapacity value from file. Value: 3
2026/02/11 00:09:18 Using RetentionWindow value from file. Value: 7
2026/02/11 00:09:18 Using CheckInterval value from file. Value: 10
2026/02/11 00:09:18 Started checking
2026/02/11 00:09:18 Check checkInterval > 0 passed!
2026/02/11 00:09:18 Check retCapacity > 0 passed!
2026/02/11 00:09:18 Check retWindow > 0 passed!
2026/02/11 00:09:18 All is OK. Ready to run program
2026/02/11 00:09:18 Ticker setup is successful
2026/02/11 00:09:28 Check completed. Deleted 10 backups
2026/02/11 00:09:38 Check completed. Deleted 0 backups
2026/02/11 00:09:48 Check completed. Deleted 2 backups
2026/02/11 00:09:58 Check completed. Deleted 0 backups
2026/02/11 00:10:08 Check completed. Deleted 0 backups
2026/02/11 00:10:10 Recived shutdown signal
2026/02/11 00:10:10 ================ End of executing here ================
```