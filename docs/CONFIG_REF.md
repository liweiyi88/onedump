# Configuration reference
Below is an example that shows all configurable items.

```
jobs:
- name: ssh-dump #dump job name is required.
  dbdriver: mysql #db driver is required.
  dbdsn: user:password@tcp(127.0.0.1:3306)/dbname # dbdsn is required. you should replace, <user>, <password>, <127.0.0.1:3306> and <dbname> with your real db credentials
  gzip: true #optional, false by default
  unique: true #optional, false by default
  options: #optional, database dump options, depends on different drivers.
  - --skip-comments
  - --no-create-info
  sshhost: mywebsite.com #required when connect via ssh
  sshuser: root #required when connect via ssh
  sshkey: |-
    -----BEGIN OPENSSH PRIVATE KEY-----
    b3BlbnNzaC1rZXktdjEAAAAABG5vbmUAAAAEbm9uZQAAAAAAAAABAAACFwAAAAdzc2gtcn...
    -----END OPENSSH PRIVATE KEY----- #required when connect via ssh, be careful with the indentation.
  storage:
    local: # save dump file to local dirs
      - path: /Users/jack/Desktop/dbbackup.sql
    s3: # save dump file to a s3 bucket, replace the credentials with your own one.
      - bucket: mybucket
        key: db-backup/dbbackup.sql
        region: ap-southeast-2
        access-key-id: <accesskeyid>
        secret-access-key: <secretkey>
    gdrive: # save dump file to google drive via service account. You need to firstly create a project in your Goold Cloud account, then create the service account and create a service account key. Finally enable drive api in your project.
      - filename: dbbackup.sql # required, just the file name, not the full path.
        folderid: 13GbhhbpBeJmUIzm9lET63nXgWgdh3Tly # As we use service account, in order to use a folder from your own drive, you need to firstly share the folder with the service account email. Secondly, get the folderid from browser url, see https://robindirksen.com/blog/where-do-i-get-google-drive-folder-id
        email: myproject@onedump.iam.gserviceaccount.com # the service account email
        privatekey: "-----BEGIN PRIVATE KEY-----\nMIIE....OqH4=\n-----END PRIVATE KEY-----\n" # get the private key from the service account
```
