<p align="center">
<img src="docs/logo.png" alt="Onedump" title="Onedump" />
</p>

[![GoDoc](https://godoc.org/github.com/liweiyi88/onedump?status.svg)](https://godoc.org/github.com/liweiyi88/onedump)
![tests](https://github.com/liweiyi88/onedump/actions/workflows/tests.yaml/badge.svg)
[![codecov](https://codecov.io/gh/liweiyi88/onedump/branch/main/graph/badge.svg?token=ROIDLHX41V)](https://codecov.io/gh/liweiyi88/onedump)
[![Go Report Card](https://goreportcard.com/badge/github.com/liweiyi88/onedump)](https://goreportcard.com/report/github.com/liweiyi88/onedump)
[![License](https://img.shields.io/badge/license-MIT-blue.svg)](https://github.com/liweiyi88/onedump/blob/main/LICENSE.md)

Onedump is a database dump and backup tool. It can dump different databases to different storages with a simple configuration file or cli commands.

## Usage

`onedump` has just one simple command to load a config file and dump DB contents based on the configuration. It has two ways of loading the config file.

### Load configuration file from local directory
After installing onedump, you should be able to run it as a simple cli command. For example:

```
$ onedump -f /path/to/config.yaml
```

The config.yaml contains all DB backup jobs in yaml format. For all configurable items. see [configuration](./docs/CONFIG_REF.md)

### Load configuration from an S3 bucket
Moreover, instead of loading your config file from a local directory, you can also store it in an AWS S3 bucket. Run the cli command to load the config file from an S3 bucket
```
$ onedump -f backup-config/config.yaml --s3-bucket mybucket
```
In this case, you pass the `--s3-bucket` option to indicate onedump that it should load the configuration content from an s3 bucket called `mybucket`. Then  onedump will treat the file path option `backup-config/config.yaml` as the s3 key. By default, onedump will use any AWS environment variables to interact with S3, if environment variables are not found, then it will use the credentials of the default profile in your `~/.aws/credentials` file. To overwirte these default credentials, you can pass `--aws-key`, `--aws-region` and `--aws-secret` options.

### Recommendation

Loading the configuration from a local directory is handy when you have control of a machine and want to run `onedump` as a normal cli command. However, you are responsible to make sure the config file is stored securely in that machine or maybe you are responsible for encryption at rest yourself.

On the other hand, loading the configuration from an S3 bucket is better when security is your concern (encryption, versioning and fine-grain permission control etc) and when it is not convenient to have a persistent volume to store your config file (e.g. run it via a docker container). Instead of downloading the config file from S3 to a local directory. `onedump` will load the config directly to memory from S3 via AWS API.

## How it works
The primary use case of `onedump` is to run one command from one configuration file that dumps database contents from different database drivers to different destinations.

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

### Save DB contents to storage
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

### Extra features
#### Compression
Compress your DB content in gzip format. Set the `gzip` config to `true` (it is `false` by default)
```
jobs:
- name: local-dump
  gzip: true
  # the rest of config...
```
#### Unique filename
If you want to keep previous DB dump files in the storage. Set the `unique` config to `true` (it is `false` by default). For example:
```
jobs:
- name: local-dump
  unique: true
  # the rest of config...
```
`onedump` will add a unique prefix to the dump file, by doing so, it won't overwrite previous dump files.


## Database drivers
| Driver | Status |
| --- | --- |
| MySQL | ✅ Suported |
| PostgreSQL | 🚧 WIP |

## Storages
| Storage | Status |
| --- | --- |
| Local | ✅ Suported |
| S3 | ✅ Suported |
| Googl Drive | 🚧 WIP |
| Dropbox | 🚧 WIP |