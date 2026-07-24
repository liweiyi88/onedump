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

Onedump は、複数のデータベースと保存先にまたがるバックアップおよび復元作業を効率化するデータベース管理ツールです。

## 機能
* さまざまなソースのデータベースを、さまざまな保存先へバックアップ。
* 依存関係のない MySQL ダンプ（組み込みのネイティブ MySQL ダンパー）。
* 外部依存のあるダンパー（`mysqldump` と `pg_dump`）をサポート。
* MySQL binlog を AWS S3 へバックアップ。
* binlog から MySQL を復元。
* MySQL スローログパーサー。
* 再開可能で並行実行に対応した SFTP ファイル転送。
* S3 バケットから設定を読み込み。
* Slack 通知。
* すべての依存関係を含む、メンテナンス済みの Docker イメージ。

## 目次

* [インストール](#インストール)
  - [バイナリ](#バイナリ)
  - [Docker イメージ](#docker-イメージ)
* [前提条件](#前提条件)
* [Onedump の実行](#onedump-の実行)
* [仕組み](#仕組み)
* [ネイティブ MySQL ダンパー](#ネイティブ-mysql-ダンパー)
* [スローログパーサー](#スローログパーサー)
* [MySQL binlog の AWS S3 へのバックアップ](#mysql-binlog-の-aws-s3-へのバックアップ)
* [MySQL binlog の復元](#mysql-binlog-の復元)
* [再開可能で並行実行に対応した SFTP ファイル転送](#再開可能で並行実行に対応した-sftp-ファイル転送)
* [コントリビューション](#コントリビューション)

### 対応するソースデータベース
- MySQL
- PostgreSQL

### 対応する保存先

- ローカルファイルシステム
- AWS S3
- Google Drive
- Dropbox
- SFTP


## インストール
`onedump` は、リリースプロセスを通じてバイナリと Docker イメージの両方を提供します。

### バイナリ
`onedump` のバイナリは https://github.com/liweiyi88/onedump/releases から入手できます。お使いの OS に適した最新バージョンを使用してください。
バイナリをダウンロードし、`$PATH` 環境変数に含まれるフォルダー（例：`/usr/local/bin/onedump`）へ移動した後、実行権限を付与します（例：`sudo chmod +x /usr/local/bin/onedump`）。これで次のように実行できます。
```
$ onedump
```

### Docker イメージ

Docker イメージは [Docker Hub](https://hub.docker.com/r/julianli/onedump/tags) からも入手できます。

#### Docker イメージを使用する場面
1. Kubernetes、ECS、またはその他のコンテナ環境で `onedump` を実行する場合。

2. 現在、`onedump` はネイティブの PostgreSQL ダンパーを標準では提供していません。そのため、PostgreSQL では同じマシンに `pg_dump` をインストールする必要があります。Docker イメージには `pg_dump` が含まれているため、追加の手動インストールなしで `onedump` を簡単に実行できます。

3. 現在のネイティブ MySQL ダンパーが要件を満たさず、`mysqldump` が必要な場合。

#### 特定の pg_dump バージョンを使用する
Docker イメージには、`mysql` クライアント、`postgresql15-client`、`postgresql16-client` が含まれています。デフォルトでは `postgresql16-client` を使用しますが、環境変数 `PG_VERSION` を渡すことで、PostgreSQL クライアントのバージョンを `15` と `16` の間で切り替えられます。例：`docker run -e PG_VERSION=15 julianli/onedump:v1.5.0-arm64 -f config.yaml`。

> `ARM64` と `AMD64` の両方の Docker イメージをメンテナンスしていますが、通常、本番環境の Linux マシンで必要なのは `AMD64` イメージです。例：`julianli/onedump:v1.5.0-amd64`*

## 前提条件

`onedump` はネイティブ MySQL ダンパーを提供しており、`onedump` バイナリを使用して MySQL データベースの内容をダンプできます。マシンに `mysqldump` をインストールする必要はありません。

より高度な MySQL ダンプ機能が必要な場合、または PostgreSQL をダンプする場合は、`mysqldump` または `pg_dump` ダンパーを使用できます。その際は、マシンに以下の依存関係をインストールする必要があります。

MySQL：`mysql-client`<br>
PostgreSQL：`postgresql-client`

> メンテナンスされている Docker イメージを使用する場合、これらのツールはデフォルトで含まれています。さらにカスタマイズが必要な場合は、Docker イメージを拡張するか、独自にビルドすることもできます。

## Onedump の実行

`onedump` には、設定ファイルを読み込み、その設定に基づいてデータベースの内容をダンプするシンプルなコマンドが 1 つだけあります。設定ファイルの読み込み方法は 2 通りあります。

### 方法 1：ローカルディレクトリから設定ファイルを読み込む
Onedump をインストールすると、シンプルな CLI コマンドとして実行できます。例：

```
$ onedump -f /path/to/config.yaml
```

config.yaml には、すべてのデータベースバックアップジョブが YAML 形式で記述されます。設定可能なすべての項目については、[設定](./docs/CONFIG_REF.md)を参照してください。

### 方法 2：S3 バケットから設定を読み込む
ローカルディレクトリから設定ファイルを読み込む代わりに、AWS S3 バケットへ保存することもできます。S3 バケットから設定ファイルを読み込むには、次の CLI コマンドを実行します。
```
$ onedump -f backup-config/config.yaml --s3-bucket mybucket
```
この場合、`--s3-bucket` オプションで、`mybucket` という S3 バケットから設定内容を読み込むよう Onedump に指示します。Onedump はファイルパスオプション `backup-config/config.yaml` を S3 キーとして扱います。デフォルトでは、Onedump は利用可能な AWS 環境変数を使用して S3 とやり取りします。環境変数が見つからない場合は、`~/.aws/credentials` ファイルのデフォルトプロファイルにある認証情報を使用します。これらのデフォルト認証情報を上書きするには、`--aws-key`、`--aws-region`、`--aws-secret` オプションを渡します。

### 設定例

設定可能なすべての項目と手順については、[設定](./docs/CONFIG_REF.md)を参照してください。

#### ローカル DB を 2 つのローカルディレクトリへダンプする
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

#### SSH 経由でリモート DB をダンプし、ローカルディレクトリと S3 バケットへ保存する
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

#### 複数のダンプを別々の保存先へ出力する
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
#### 並行ジョブの最大数を制御する

設定ファイルで `maxjobs` オプション（デフォルトは 10 ジョブ）を設定すると、同時に実行するジョブ数を制御できます。例：

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

### 推奨事項

マシンを管理でき、`onedump` を通常の CLI コマンドとして実行したい場合は、ローカルディレクトリから設定を読み込む方法が便利です。ただし、そのマシン上で設定ファイルを安全に保存し、必要に応じて保存時の暗号化を行う責任は利用者にあります。

一方、セキュリティ（暗号化、バージョン管理、きめ細かな権限制御など）が重要な場合や、設定ファイルを保存する永続ボリュームを用意しにくい場合（Docker コンテナで実行する場合など）は、S3 バケットから設定を読み込む方が適しています。`onedump` は、設定ファイルを S3 からローカルディレクトリへダウンロードせず、AWS API を通じてメモリへ直接読み込みます。

## 仕組み
`onedump` の主なユースケースは、設定ファイルを指定して 1 つのコマンドを実行することです。異なるドライバーのデータベースをダンプし、異なる保存先へ出力します。

### データベースへの接続
`onedump` は、直接ネットワークアクセスまたは SSH の 2 つの方法でデータベースへ接続します。

#### ネットワークアクセスでデータベースへ接続する
ローカル DB をダンプする場合でも、Onedump を実行するマシンから直接接続できる DB ホストをダンプする場合でも、設定ファイルに `job` 設定項目を作成できます。

```
jobs:
- name: exec-dump
  dbdriver: mysql
  dbdsn: user:password@tcp(10.10.10.1)/dbname
  # the rest of config...
```

`dbdriver` と `dbdsn` は、データベースへの接続に必要な必須フィールドです。この例では、DB ホストは IP アドレス `10.10.10.1` のプライベートネットワーク内にあります。`onedump` を実行するマシンが同じプライベートネットワーク内にあれば、DB に接続できます。

#### SSH 経由でデータベースへ接続する
SSH が有効な場合は、リモートデータベースへ接続することもできます。設定ファイルに `job` 設定項目を作成します。
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

この場合、`sshhost`、`sshuser`、`sshkey` という 3 つの追加設定オプションを渡し、`onedump` が SSH 経由でリモートデータベースと通信するよう指定します。

### DB をストレージへ保存する
ダンプジョブには、少なくとも 1 つの保存先を設定する必要があります。たとえば、ローカル DB をダンプし、その内容をローカルディレクトリと S3 バケットの両方へ保存します。

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

### cron ジョブを設定する
cron 式を渡して、Onedump を cron モードで実行します。

```
$ onedump -f /path/to/config.yaml -c 21h
```

## ネイティブ MySQL ダンパー

ネイティブ MySQL ダンパーは、`mysqldump` に似た使用感を提供します。ただし、`mysqldump` のすべての機能を実装しているわけではありません。基本的な用途の大半には適していますが、次の制限があります。

1. MySQL の空間データ型をサポートしていません。たとえば、`GEOMETRY`、`POINT`、`POLYGON` などの空間データ型を使用する場合は、`mysqldump` をダンパーとして使用してください。

1. `mysqldump` のすべてのオプションをサポートしていません。現在は `--skip-add-drop-table` と `--skip-add-locks` をサポートしています。

ネイティブ MySQL ダンパーが要件を満たさない場合は、簡単に mysqldump へ切り替えられます。設定ファイルの `dbdriver` を `mysqldump` に更新するだけです。
```
jobs:
- dbdriver: mysqldump
  ...
```

## スローログパーサー
`onedump` には、MySQL スローログを解析し、次の構造で JSON を出力する組み込み機能（`onedump slow [flags]` で実行）もあります。

```
{ok: bool, error: string, results: []SlowResult}
```

例：
```json
{"ok":true,"error":"","results":[{"time":"2023-10-15T12:36:05.987654Z","user":"admin[admin]","host_ip":"[192.168.1.101]","query_time":12.890323,"lock_time":0.001456,"rows_sent":100,"rows_examined":100000,"thread_id":0,"errno":0,"killed":0,"bytes_received":0,"bytes_sent":0,"read_first":0,"read_last":0,"read_key":0,"read_next":0,"read_prev":0,"read_rnd":0,"read_rnd_next":0,"sort_merge_passes":0,"sort_range_count":0,"sort_rows":0,"sort_scan_count":0,"created_tmp_disk_tables":0,"created_tmp_tables":0,"count_hit_tmp_table_size":0,"start":"","end":"","query":"SELECT customer_id, COUNT(*) as order_count FROM orders GROUP BY customer_id HAVING order_count > ?"}]}
```


### 使用方法
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
## MySQL binlog の AWS S3 へのバックアップ

`binlog sync-s3` コマンドを使用すると、MySQL のバイナリログ（binlog）ファイルを AWS S3 バケットへバックアップできます。これは、ポイントインタイムリカバリを実現する場合に特に役立ちます。

詳しい使用方法は、[ドキュメント](./docs/binlog/sync-s3.md)を参照してください。

## MySQL binlog の復元
`binlog restore` コマンドは、ポイントインタイムリカバリ処理の一部として使用できます。binlog のイベントを再生し、指定した時点までデータを復元します。

詳しい使用方法は、[ドキュメント](./docs/binlog/restore.md)を参照してください。


## 再開可能で並行実行に対応した SFTP ファイル転送

`sync sftp` コマンドは、SFTP プロトコルを使用して、ソースからリモートの保存先へファイルを効率的に転送する方法を提供します。さまざまなユースケースに対応する複数のオプションをサポートしています。

詳しい使用方法は、[ドキュメント](./docs/sync/sftp.md)を参照してください。

## コントリビューション
開発ガイドラインについては、[開発ガイド](./docs/development.md)を参照してください。
