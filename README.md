<p align="center">
<img src="docs/logo.png" alt="Onedump" title="Onedump" />
</p>

[![Mentioned in Awesome Go](https://awesome.re/mentioned-badge.svg)](https://github.com/avelino/awesome-go)
[![GoDoc](https://godoc.org/github.com/liweiyi88/onedump?status.svg)](https://godoc.org/github.com/liweiyi88/onedump)
![tests](https://github.com/liweiyi88/onedump/actions/workflows/tests.yaml/badge.svg)
[![codecov](https://codecov.io/gh/liweiyi88/onedump/branch/main/graph/badge.svg?token=ROIDLHX41V)](https://codecov.io/gh/liweiyi88/onedump)
[![Go Report Card](https://goreportcard.com/badge/github.com/liweiyi88/onedump)](https://goreportcard.com/report/github.com/liweiyi88/onedump)
[![License](https://img.shields.io/badge/license-MIT-blue.svg)](https://github.com/liweiyi88/onedump/blob/main/LICENSE.md)

Onedump is a database dump and backup tool that can dump different databases to different storages with a simple configuration file or CLI commands.

## Features
* Database backup from different sources to different destinations.
* MySQL dump with zero dependencies (with built-in mysql native dumper). 
* Supports dumpers with dependencies (`mysqldump` and `pg_dump`).
* Loads configuration from S3 bucket.
* Compression (use `job.gzip: true` to enable compression).
* Unique filename (use `job.unique: true` to enable unique filename).
* Slack notification.
* Maintained docker image that contains all dependencies.

### Supported data sources
| Databases | Status |
| --- | --- |
| MySQL | ✅ Suported |
| PostgreSQL | ✅ Suported |

### Supported destinations
| Storage | Status |
| --- | --- |
| Local | ✅ Suported |
| S3 | ✅ Suported |
| Google Drive | ✅ Supported |
| Dropbox | ✅ Supported |


## Installation
Onedump provides both binaries and Docker images via the release process.

### Binary
`onedump` binaries are available at https://github.com/liweiyi88/onedump/releases. Use the latest version of the binary that is suitable for your OS.
After downloading the binary and moving it to a folder that is in your `$PATH` environment variable (e.g., `/usr/local/bin/onedump`), give it executable permissions (e.g., `sudo chmod +x /usr/local/bin/onedump`). Then you should be able to run it:
```
$ onedump
```

### Docker image

Docker images are also available in [Docker Hub](https://hub.docker.com/r/julianli/onedump/tags).

#### When to use the Docker image
1. You want run `onedump` in Kubernetes, ECS, or any other container environment.

2. Currently, `onedump` does not provide a native PostgreSQL dumper out of the box. Therefore, for PostgreSQL, you need to have `pg_dump` installed on the same machine. The Docker image includes `pg_dump`, making it easier to run `onedump` without additional manual installation.

3. When the current native MySQL dumper doesn't fit your needs and you need `mysqldump`.

#### Use a specific pg_dump version
The docker image contains `mysql` client, `postgresql15-client` and `postgresql16-client`. By default it uses `postgresql16-client`. However, you can pass an environemnt `PG_VERSION` to swtich postgresql client version between `15` and `16`. For example, `docker run -e PG_VERSION=15 julianli/onedump:v1.2.0-arm64 -f config.yaml`.

> Although we maintain both `ARM64` and `AMD64` Docker images, usually what you need is the `AMD64` image on your production Linux machine. For example: `julianli/onedump:v1.2.0-amd64`*

## Prerequisites

`onedump` provides a native MySQL dumper, allowing you to use the `onedump` binary to dump your MySQL database contents without needing `mysqldump` be installed on your machine.

If you need more advanced MySQL dump features or for PostgreSQL dump, you may want to use `mysqldump` or `pg_dump` dumper, which you need to install the below dependencies on the machine.

MySQL: `mysql-client`  
PostgreSQL: `postgresql-client`

> If you use the Docker image we maintain, these tools are included by default. Alternatively, you can extend or build the Docker image yourself if you need further customization.

## Run onedump

`onedump` has just one simple command to load a config file and dump DB contents based on the configuration. It has two ways of loading the config file.

### Option 1. Load configuration file from local directory
After installing onedump, you should be able to run it as a simple cli command. For example:

```
$ onedump -f /path/to/config.yaml
```

The config.yaml contains all DB backup jobs in yaml format. For all configurable items. see [configuration](./docs/CONFIG_REF.md)

### Option 2. Load configuration from an S3 bucket
Moreover, instead of loading your config file from a local directory, you can also store it in an AWS S3 bucket. Run the cli command to load the config file from an S3 bucket
```
$ onedump -f backup-config/config.yaml --s3-bucket mybucket
```
In this case, you pass the `--s3-bucket` option to indicate onedump that it should load the configuration content from an s3 bucket called `mybucket`. Then  onedump will treat the file path option `backup-config/config.yaml` as the s3 key. By default, onedump will use any AWS environment variables to interact with S3, if environment variables are not found, then it will use the credentials of the default profile in your `~/.aws/credentials` file. To overwirte these default credentials, you can pass `--aws-key`, `--aws-region` and `--aws-secret` options.

### Configuration examples

For all configurable items and instructions. see [configuration](./docs/CONFIG_REF.md)

#### Dump a local DB to 2 local directories
```
jobs:
- name: local-dump
  dbdriver: mysql
  dbdsn: root@tcp(127.0.0.1)/test_local
  gzip: true
  storage:
    local:
      - path: /Users/jack/Desktop/mydb.sql
      - path: /Users/jack/Desktop/mydb2.sql
```

#### Dump a remote DB via SSH and save to a local directory and a S3 bucket
```
jobs:
- name: ssh-dump
  dbdriver: mysql
  dbdsn: user:password@tcp(127.0.0.1:3306)/mydb
  sshhost: mywebsite.com
  sshuser: root
  sshkey: |-
    -----BEGIN OPENSSH PRIVATE KEY-----
    b3BlbnNzaC1rZXktdjEAAAAABG5vbmUAAAAEbm9uZQAAAAAAAAABAAACFwAAAAdzc2gtcn...
    -----END OPENSSH PRIVATE KEY-----
  storage:
    local:
      - path: /Users/jack/Desktop/db.sql
    s3:
      - bucket: mys3bucket
        key: backup/mydb.sql
        region: ap-southeast-2
        access-key-id: awsaccesskey
        secret-access-key: awssecret
```

#### Multiple dumps to different storage
```
jobs:
- name: local-dump
  dbdriver: mysql
  dbdsn: root@tcp(127.0.0.1)/test_local
  storage:
    local:
      - path: /Users/jack/Desktop/mydb.sql
- name: ssh-dump
  dbdriver: mysql
  dbdsn: user:password@tcp(127.0.0.1:3306)/mydb
  sshhost: mywebsite.com
  sshuser: root
  sshkey: |-
    -----BEGIN OPENSSH PRIVATE KEY-----
    b3BlbnNzaC1rZXktdjEAAAAABG5vbmUAAAAEbm9uZQAAAAAAAAABAAACFwAAAAdzc2gtcn...
    -----END OPENSSH PRIVATE KEY-----
  storage:
    s3:
      - bucket: mys3bucket
        key: backup/mydb.sql
        region: ap-southeast-2
        access-key-id: awsaccesskey
        secret-access-key: awssecret
```

#### Slack notification

```
notifier:
    slack:
      - incomingwebhook: https://hooks.slack.com/services/A0B8A11N4N/...
jobs:
- name: local-dump
  dbdriver: mysql
  dbdsn: root@tcp(127.0.0.1)/test_local
  gzip: true
  storage:
    local:
      - path: /Users/jack/Desktop/mydb.sql
```

### Recommendation

Loading the configuration from a local directory is handy when you have control of a machine and want to run `onedump` as a normal cli command. However, you are responsible to make sure the config file is stored securely in that machine or maybe you are responsible for encryption at rest yourself.

On the other hand, loading the configuration from an S3 bucket is better when security is your concern (encryption, versioning and fine-grain permission control etc) and when it is not convenient to have a persistent volume to store your config file (e.g. run it via a docker container). Instead of downloading the config file from S3 to a local directory. `onedump` will load the config directly to memory from S3 via AWS API.

## How it works
The primary use case of `onedump` is to run one command with a configuration file. It dumps database from different drivers to different destinations.

### Connect to the database
`onedump` connects to your database in two ways: direct network access or SSH.

#### Connect the database via network access
No matter if it is to dump from your local DB or a DB host that the machine can dial directly. You can create a `job` configurable item in the config file

```
jobs:
- name: exec-dump
  dbdriver: mysql
  dbdsn: user:password@tcp(10.10.10.1)/dbname
  # the rest of config...
```

`dbdriver` and `dbdsn` are the required fields to be able to connect to your database. For this case, the DB host is in a private network that has an IP address `10.10.10.1`. It is possible to connect to the DB when the machine that runs `onedump` is in the same private network.

#### Connect the database via SSH
You can also connect to a remote database when SSH is enabled. Create a `job` configurable item in the config file
```
jobs:
- name: ssh-dump
  dbdriver: mysql
  dbdsn: user:password@tcp(127.0.0.1:3306)/dbname
  sshhost: mywebsite.com
  sshuser: root
  sshkey: |-
    -----BEGIN OPENSSH PRIVATE KEY-----
    b3BlbnNzaC1rZXktdjEAAAAABG5vbmUAAAAEbm9uZQAAAAAAAAABAAACFwAAAAdzc2gtcn...
    -----END OPENSSH PRIVATE KEY-----
  # the rest of config...
```

For this case, you need to pass three extra config options: `sshhost`, `sshuser` and `sshkey` to tell `onedump` to talk to the remote database via ssh.

### Save DB to storage
It is required to config at least one storage for the dump job. For example, we want it to dump a local DB and save the contents in a local directory as well as an S3 bucket.

```
jobs:
- name: local-dump
  dbdriver: mysql
  dbdsn: root@tcp(127.0.0.1)/db
  storage:
    local:
      - path: /Users/jack/Desktop/db.sql
    s3:
      - bucket: mybucket
        key: db-backup/mydb.sql
        region: ap-southeast-2
        access-key-id: MYKEY...
        secret-access-key: AWSSECRET..
```

### Setting cron job
Run onedump with cron mode by passing cron experssions.

```
$ onedump -f /path/to/config.yaml -c 21h
```

## The Native MySQL dumper

The MySQL native dumper provides a user experience similar to `mysqldump`. However, it doesn't implement all the features of `mysqldump`. It is suitable for most basic use cases but has some limitations:

1. It doesn't support MySQL Spatial Data Types. For example If you use spatial data types like `GEOMETRY`, `POINT` or `POLYGON`, use `mysqldump` as the dumper.

1. It doesn't support all `mysqldump` options. Currently it supports `--skip-add-drop-table` and `--skip-add-locks`

If the native MySQL dumper doesn't meet your needs, switching to mysqldump is easy. Just update `dbdriver` in the configuration file to `mysqldump`:
```
jobs:
- dbdriver: mysqldump
  ...
```