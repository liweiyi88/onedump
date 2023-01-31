# Configuration reference
## All configurable items

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
    gdrive:
      - filename: dbbackup.sql # required, just the file name, not the full path.
        folderid: 13GbhhbpBeJmUIzm9lET63nXgWgdh3Tly
        email: myproject@onedump.iam.gserviceaccount.com
        privatekey: "-----BEGIN PRIVATE KEY-----\nMIIE....OqH4=\n-----END PRIVATE KEY-----\n"
    dropbox:
      - accesstoken: sl.BX7SOnErNkYJ...
        path: /home/mydump.sql
```

# How to get storage credentials

## Google drive
The google drive integration is done via service account. Therefore you need to firstly create a project in your Goold Cloud account, then create the service account and create a service account key. Finally enable drive api in your project.

`folderid`: As we use service account, in order to use a folder from your own drive, you need to firstly share the folder with the service account email. Secondly, get the folderid from browser url, see https://robindirksen.com/blog/where-do-i-get-google-drive-folder-id

`email`: It is the service account email not your personal email. You should be able to get the service account email via the Goolge Cloud service account page.

`privatekey`: You get the private key from the service account page.

## Dropbox

In order to get `accesstoken` from Dropbox. We need to complete the following step.

1. Go to https://www.dropbox.com/lp/developers click App console button on the top right.
1. Create app and choose `Full Dropbox`. The app should at least has `files.content.write` permission
1. Once the app has been created, click "Generate" button under "Generated access token" section.

Note, The access token generated from the settings page is no longer a long lived token and there is no way to complete the oauth flow without browser. Have asked dropbox https://www.dropboxforum.com/t5/Discuss-Dropbox-Developer-API/How-to-get-refresh-token-without-User-interaction/m-p/655155/highlight/true#M3148 and see how it goes in the future.
