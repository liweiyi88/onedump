<p align="center">
<img src="docs/logo.png" alt="Onedump" title="Onedump" />
</p>

[English](README.md) | [简体中文](README.zh-CN.md) | [日本語](README.ja.md)

[![Mentioned in Awesome Go](https://awesome.re/mentioned-badge.svg)](https://github.com/avelino/awesome-go)
[![GoDoc](https://godoc.org/github.com/liweiyi88/onedump?status.svg)](https://godoc.org/github.com/liweiyi88/onedump)
![tests](https://github.com/liweiyi88/onedump/actions/workflows/tests.yaml/badge.svg)
[![codecov](https://codecov.io/gh/liweiyi88/onedump/branch/main/graph/badge.svg?token=ROIDLHX41V)](https://codecov.io/gh/liweiyi88/onedump)
[![Go Report Card](https://goreportcard.com/badge/github.com/liweiyi88/onedump)](https://goreportcard.com/report/github.com/liweiyi88/onedump)
[![License](https://img.shields.io/badge/license-MIT-blue.svg)](https://github.com/liweiyi88/onedump/blob/main/LICENSE.md)

Onedump 是一款数据库管理工具，可简化跨多个数据库和存储目标的备份与恢复任务。

## 功能
* 将不同来源的数据库备份到不同目标。
* 零依赖的 MySQL 转储（内置原生 MySQL 转储器）。
* 支持有外部依赖的转储器（`mysqldump` 和 `pg_dump`）。
* 将 MySQL binlog 备份到 AWS S3。
* 从 binlog 恢复 MySQL。
* MySQL 慢日志解析器。
* 可恢复、并发的 SFTP 文件传输。
* 从 S3 存储桶加载配置。
* Slack 通知。
* 提供包含所有依赖项并持续维护的 Docker 镜像。

## 目录

* [安装](#安装)
  - [二进制文件](#二进制文件)
  - [Docker 镜像](#docker-镜像)
* [前置条件](#前置条件)
* [运行 Onedump](#运行-onedump)
* [工作原理](#工作原理)
* [原生 MySQL 转储器](#原生-mysql-转储器)
* [慢日志解析器](#慢日志解析器)
* [将 MySQL binlog 备份到 AWS S3](#将-mysql-binlog-备份到-aws-s3)
* [MySQL binlog 恢复](#mysql-binlog-恢复)
* [可恢复、并发的 SFTP 文件传输](#可恢复并发的-sftp-文件传输)
* [贡献](#贡献)

### 支持的源数据库
- MySQL
- PostgreSQL

### 支持的存储目标

- 本地文件系统
- AWS S3
- Google Drive
- Dropbox
- SFTP


## 安装
`onedump` 通过发布流程同时提供二进制文件和 Docker 镜像。

### 二进制文件
`onedump` 二进制文件可从 https://github.com/liweiyi88/onedump/releases 获取。请使用适合您操作系统的最新版本。
下载二进制文件并将其移动到 `$PATH` 环境变量中的目录（例如 `/usr/local/bin/onedump`）后，为其授予可执行权限（例如 `sudo chmod +x /usr/local/bin/onedump`）。随后即可运行：
```
$ onedump
```

### Docker 镜像

Docker 镜像也可从 [Docker Hub](https://hub.docker.com/r/julianli/onedump/tags) 获取。

#### 何时使用 Docker 镜像
1. 您希望在 Kubernetes、ECS 或其他容器环境中运行 `onedump`。

2. 目前，`onedump` 默认不提供原生 PostgreSQL 转储器。因此，使用 PostgreSQL 时，需要在同一台机器上安装 `pg_dump`。Docker 镜像已包含 `pg_dump`，无需额外手动安装即可更方便地运行 `onedump`。

3. 当前的原生 MySQL 转储器无法满足您的需求，需要使用 `mysqldump` 时。

#### 使用特定版本的 pg_dump
Docker 镜像包含 `mysql` 客户端、`postgresql15-client` 和 `postgresql16-client`。默认使用 `postgresql16-client`。不过，您可以传入环境变量 `PG_VERSION`，在 `15` 和 `16` 之间切换 PostgreSQL 客户端版本。例如：`docker run -e PG_VERSION=15 julianli/onedump:v1.5.0-arm64 -f config.yaml`。

> 虽然我们同时维护 `ARM64` 和 `AMD64` Docker 镜像，但生产环境中的 Linux 机器通常需要 `AMD64` 镜像。例如：`julianli/onedump:v1.5.0-amd64`*

## 前置条件

`onedump` 提供原生 MySQL 转储器，允许您使用 `onedump` 二进制文件转储 MySQL 数据库内容，而无需在机器上安装 `mysqldump`。

如果需要更高级的 MySQL 转储功能或要转储 PostgreSQL，您可能需要使用 `mysqldump` 或 `pg_dump` 转储器，并在机器上安装以下依赖项。

MySQL：`mysql-client`<br>
PostgreSQL：`postgresql-client`

> 如果使用我们维护的 Docker 镜像，这些工具已默认包含。如需进一步自定义，也可以扩展或自行构建 Docker 镜像。

## 运行 Onedump

`onedump` 只有一个简单命令，用于加载配置文件并根据配置转储数据库内容。它有两种加载配置文件的方式。

### 方式 1：从本地目录加载配置文件
安装 Onedump 后，您应当可以把它作为简单的 CLI 命令运行。例如：

```
$ onedump -f /path/to/config.yaml
```

config.yaml 以 YAML 格式包含所有数据库备份作业。有关全部可配置项，请参阅[配置](./docs/CONFIG_REF.md)。

### 方式 2：从 S3 存储桶加载配置
除了从本地目录加载配置文件，您还可以将配置文件存储在 AWS S3 存储桶中。运行以下 CLI 命令，从 S3 存储桶加载配置文件：
```
$ onedump -f backup-config/config.yaml --s3-bucket mybucket
```
在此示例中，您通过 `--s3-bucket` 选项告诉 Onedump 从名为 `mybucket` 的 S3 存储桶加载配置内容。Onedump 随后会将文件路径选项 `backup-config/config.yaml` 视为 S3 键。默认情况下，Onedump 会使用可用的 AWS 环境变量与 S3 交互；如果未找到环境变量，则使用 `~/.aws/credentials` 文件中默认配置文件的凭据。若要覆盖这些默认凭据，可以传入 `--aws-key`、`--aws-region` 和 `--aws-secret` 选项。

### 配置示例

有关全部可配置项和说明，请参阅[配置](./docs/CONFIG_REF.md)。

#### 将本地数据库转储到 2 个本地目录
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

#### 通过 SSH 转储远程数据库，并保存到本地目录和 S3 存储桶
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
        session-token: <session-token> # optional, specify the value if you assume a role.
```

#### 多个转储写入不同存储
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
        session-token: <session-token> # optional, specify the value if you assume a role.
```
#### 控制并发作业的最大数量

可以通过在配置文件中设置 `maxjobs` 选项（默认为 10 个作业）来控制并发运行的作业数。例如：

```
maxjobs: 20
jobs:
- name: local-dump
  dbdriver: mysql
  ...
```

#### Slack 通知

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

### 建议

当您可以控制一台机器并希望把 `onedump` 作为普通 CLI 命令运行时，从本地目录加载配置很方便。不过，您需要负责确保配置文件安全地存储在该机器上，也可能需要自行负责静态加密。

另一方面，如果您关注安全性（加密、版本控制、精细的权限控制等），或不便使用持久卷存储配置文件（例如通过 Docker 容器运行），从 S3 存储桶加载配置会更合适。`onedump` 不会先把配置文件从 S3 下载到本地目录，而是通过 AWS API 直接将配置加载到内存中。

## 工作原理
`onedump` 的主要用例是通过一个配置文件运行一条命令。它从不同的数据库驱动程序转储数据库，并写入不同目标。

### 连接数据库
`onedump` 通过两种方式连接数据库：直接网络访问或 SSH。

#### 通过网络访问连接数据库
无论是转储本地数据库，还是转储运行 Onedump 的机器可直接连接的数据库主机，都可以在配置文件中创建一个 `job` 配置项：

```
jobs:
- name: exec-dump
  dbdriver: mysql
  dbdsn: user:password@tcp(10.10.10.1)/dbname
  # the rest of config...
```

`dbdriver` 和 `dbdsn` 是连接数据库所需的必填字段。在此示例中，数据库主机位于私有网络中，IP 地址为 `10.10.10.1`。当运行 `onedump` 的机器处于同一私有网络时，就可以连接该数据库。

#### 通过 SSH 连接数据库
启用 SSH 时也可以连接远程数据库。在配置文件中创建一个 `job` 配置项：
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

在此示例中，需要传入三个额外的配置选项：`sshhost`、`sshuser` 和 `sshkey`，以指示 `onedump` 通过 SSH 与远程数据库通信。

### 将数据库保存到存储
每个转储作业都必须至少配置一种存储。例如，我们希望转储本地数据库，并将内容同时保存到本地目录和 S3 存储桶。

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
        session-token: <session-token> # optional, specify the value if you assume a role.
```

### 设置 cron 作业
通过传入 cron 表达式，以 cron 模式运行 Onedump。

```
$ onedump -f /path/to/config.yaml -c 21h
```

## 原生 MySQL 转储器

原生 MySQL 转储器提供与 `mysqldump` 相似的用户体验。不过，它并未实现 `mysqldump` 的全部功能，适用于大多数基本用例，但有以下限制：

1. 不支持 MySQL 空间数据类型。例如，如果使用 `GEOMETRY`、`POINT` 或 `POLYGON` 等空间数据类型，请使用 `mysqldump` 转储器。

1. 不支持全部 `mysqldump` 选项。目前支持 `--skip-add-drop-table` 和 `--skip-add-locks`。

如果原生 MySQL 转储器无法满足需求，可以轻松切换到 mysqldump。只需在配置文件中将 `dbdriver` 更新为 `mysqldump`：
```
jobs:
- dbdriver: mysqldump
  ...
```

## 慢日志解析器
`onedump` 还内置了一个功能（通过 `onedump slow [flags]` 运行），用于解析 MySQL 慢日志，并按以下结构输出 JSON：

```
{ok: bool, error: string, results: []SlowResult}
```

例如：
```json
{"ok":true,"error":"","results":[{"time":"2023-10-15T12:36:05.987654Z","user":"admin[admin]","host_ip":"[192.168.1.101]","query_time":12.890323,"lock_time":0.001456,"rows_sent":100,"rows_examined":100000,"thread_id":0,"errno":0,"killed":0,"bytes_received":0,"bytes_sent":0,"read_first":0,"read_last":0,"read_key":0,"read_next":0,"read_prev":0,"read_rnd":0,"read_rnd_next":0,"sort_merge_passes":0,"sort_range_count":0,"sort_rows":0,"sort_scan_count":0,"created_tmp_disk_tables":0,"created_tmp_tables":0,"count_hit_tmp_table_size":0,"start":"","end":"","query":"SELECT customer_id, COUNT(*) as order_count FROM orders GROUP BY customer_id HAVING order_count > ?"}]}
```


### 用法
```bash
// parse a single file
$onedump slow -f /path/to/file/slow.log

// parse a folder
$onedump slow -f /path/to/folder

// Parse a folder or file by searching for filenames that match the pattern
$onedump slow -f /path/to/folder-or-file -p="*slow.log"

// Mask query values with ?
$onedump slow -f /path/to/file -m="true"
```
## 将 MySQL binlog 备份到 AWS S3

`binlog sync-s3` 命令可将 MySQL 二进制日志（binlog）文件备份到 AWS S3 存储桶。该功能尤其适合实现时间点恢复。

有关详细用法，请参阅[文档](./docs/binlog/sync-s3.md)。

## MySQL binlog 恢复
`binlog restore` 命令可作为时间点恢复流程的一部分。它会重放 binlog 中的事件，并将数据恢复到指定时间点。

有关详细用法，请参阅[文档](./docs/binlog/restore.md)。


## 可恢复、并发的 SFTP 文件传输

`sync sftp` 命令提供了一种高效方式，可通过 SFTP 协议将文件从源位置传输到远程目标。它支持多种选项，以适应不同用例。

有关详细用法，请参阅[文档](./docs/sync/sftp.md)。

## 贡献
有关开发规范，请参阅[开发指南](./docs/development.md)。
