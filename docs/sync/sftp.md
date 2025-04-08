## Resumable and concurrent SFTP file transfers

The `sync sftp` command provides an efficient way to transfer files from a source to a remote destination using the SFTP protocol. It supports various options to accommodate different use cases.

### Common use cases

#### Single file transfer
```
onedump sync sftp \
--source="/path/to/file.txt" \
--destination="/var/lib/mysql" \
--ssh-host="remote.com" \
--ssh-user="root" \
--ssh-key="base64 content or ssh file name or ssh private key raw content"
```

#### Folder files transfer
> Destination must be a folder if source is a folder, otherwise command will return error

```
onedump sync sftp \
--source="/path/to/folder/" \
--destination="/var/lib/mysql" \
--ssh-host="remote.com" \
--ssh-user="root" \
--ssh-key="base64 content or ssh file name or ssh private key raw content"
```

#### Avoid re-transferring files

Using `--checksum=true` ensures that files already transferred are not sent again.

Checksums are calculated and stored in `checksum.onedump` within the source directory.

#### Max retry attempts

The command automatically retries and resumes failed transfers indefinitely for recoverable errors until the files are successfully transferred. Use `--max-attempts` to specify when the command should stop retrying.

#### View all available options
Run `onedump sync sftp --help` see all available options.