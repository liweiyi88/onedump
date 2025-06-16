# Development Guides

## Testing MySQL database restore locally

To test the restore process locally, first use the download command to retrieve backups from the S3 bucket:

```sh
# Export the required environment variables before running the command
export AWS_ACCOUNT_ID=""
export AWS_ACCESS_KEY_ID=""
export AWS_SECRET_ACCESS_KEY=""
export AWS_REGION=""

go run . download s3 --bucket=[bucket_name] --prefix=[prefix] --dir=[local/path]
```

Next, start a MySQL container using the `testutils/docker/docker-compose.yml` file and import the initial database dump:

```sh
docker exec -i binlog-restore mysql -uroot -proot < [local/path/all-databasedump]
```

Finally, run the following command to restore the database:

```sh
go run . binlog restore --dry-run=true --dir=[local/path] --dump-file=[local/path/all-databasedump] | sudo docker exec -i binlog-restore mysql -uroot -proot
```