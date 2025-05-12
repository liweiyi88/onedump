## MySQL binlog backup to AWS S3

The `binlog sync-s3` command allows you to store your MySQL binlog files in an AWS S3 bucket. Binlog backups are useful when you need point-in-time recovery.

### Usages

Before running the command, you need to export the following environment variables:

```bash
# e.g. user:password@tcp(127.0.0.1)/
export DATABASE_DSN="database-dsn" \
export AWS_ACCESS_KEY_ID="aws_access_key_id" \
export AWS_REGION="ap-southeast-2" \
export AWS_SECRET_ACCESS_KEY="aws_secret_access_key"
```


#### Upload all binlog files to an AWS S3 bucket

```
onedump binlog sync-s3 --s3-bucket="your-bucket" --s3-prefix="binlogs"
```

#### Upload all binlog files to an AWS S3 bucket without re-transferring existing files

```
onedump binlog sync-s3 --s3-bucket="your-bucket" --s3-prefix="binlogs" --checksum=true
```

### Save sync results in a log

The sync result log file is named `onedump-binlog-sync.log` and will be saved in the same directory as the binlogs.

```
onedump binlog sync-s3 --s3-bucket="your-bucket" --s3-prefix="binlogs" --save-log=true
```

If you want to save it to a specific file, then use `--save-log=true` with `--log-file=/path/to/the/file.log`

```
onedump binlog sync-s3 --s3-bucket="your-bucket" --s3-prefix="binlogs" --save-log=true --log-file=/path/to/the/file.log
```

#### View all available options
Run `onedump binlog sync-s3 --help` see all available options.