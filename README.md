# SparkGitDownloader

This project aims to facilitate cloning and analyzing git projects process. It makes use of spark and AKKA pipeline to efficiently download and process git projects at a scale with configurable parameters.

## Components

This project has two main components namely GHTorrent and Git. The first one is responsible for getting list of repositories from a [GHTorrent](http://ghtorrent.org). The latter one is responsible to cloning those repositories in batches and storing results to file system.

### 1. GHTorrent

This part uses native shell script to download and extract `projects.csv` from GHTorrent archive. This script is designed to terminate connection as soon as required file(s) are downloaded. It also caches the `projects.csv` file and only downloads a fresh copy in case of a cache miss.

The later part of this script pushed all the processed records from `projects.csv` after applying required filters to the MySQL database table. This table acts as a frontier queue for the projects to be downloaded.

### 2. Git

This part makes use of native JGit api to read directly form the cloned git repository and saves the processed records to the file systems supported by hadoop.

## Usage

### Pre-requisite
1. A MySQL setup with a schema and users
2. These configurations saved in the `src/main/resources/config-defaults.properties`

### Configurations

After putting required values in the configurations.

```
make setup
```

### Execution
```
make all
```
