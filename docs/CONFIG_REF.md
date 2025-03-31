# Configuration reference
## All configurable items
```
jobs:
- name: local-dump
  dbdriver: postgresql
    dbdsn: postgres://<user>:<password>@<host>:<port>/<dbname>
    gzip: true
    options:
    storage:
      local:
        - path: /Users/jack/Desktop/postgresql-dump.sql
- name: ssh-dump #dump job name is required.
  dbdriver: mysql #db driver is required. The driver is a dump implementation, available drivers: mysql (the native mysql dumper) , postgresql, mysqldump and pgdump
  dbdsn: user:password@tcp(127.0.0.1:3306)/dbname # dbdsn is required. you should replace, <user>, <password>, <127.0.0.1:3306> and <dbname> with your real db credentials
  gzip: true #optional, false by default
  unique: true #optional, false by default
  options: #optional, database dump options, depends on different drivers.
  - --skip-comments
  - --no-create-info
  sshhost: mywebsite.com #required when connect via ssh
  sshuser: root #required when connect via ssh
  # sshkey supports base64 encoded string, a file or the raw content.
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
      - refreshtoken: sl.BX7SOnErNkYJ...
        clientid: fsdfdsf123
        clientsecret: abdfdli86123
        path: /home/mydump.sql
    sftp:
        # the remote file path
      - path: /var/lib/mysql/dbbackup.sql
        sshhost: remote.com
        sshuser: root
        # sshkey supports base64 encoded string, a file name or the raw content.
        sshkey: |-
          -----BEGIN OPENSSH PRIVATE KEY-----
          b3BlbnNzaC1rZXktdjEAAAAABG5vbmUAAAAEbm9uZQAAAAAAAAABAAACFwAAAAdzc2gtcn...
          -----END OPENSSH PRIVATE KEY-----
```

# How to get storage credentials

## Google drive
The google drive integration is done via service account. Therefore you need to firstly create a project in your Goold Cloud account, then create the service account and create a service account key. Finally enable drive api in your project.

`folderid`: As we use service account, in order to use a folder from your own drive, you need to firstly share the folder with the service account email. Secondly, get the folderid from browser url, see https://robindirksen.com/blog/where-do-i-get-google-drive-folder-id

`email`: It is the service account email not your personal email. You should be able to get the service account email via the Goolge Cloud service account page.

`privatekey`: You get the private key from the service account page.

## Dropbox

`refreshtoken`: It is the only non-expired token that we needed to get access token. In order to get `refreshtoken` from Dropbox. We need to complete the following step.

1. Go to https://www.dropbox.com/lp/developers click App console button on the top right.
1. Create app and choose `Full Dropbox`. The app should at least has `files.content.write` permission
1. Once the app has been created, visit `https://www.dropbox.com/oauth2/authorize?client_id=<client_id>&token_access_type=offline&response_type=code`, use the `App key` value from the app page to replace `<client_id>` in the url. Click continue and allow to get a auth code and copy the auth code somewhere for next step.
1. Run the following `curl` command (Note, replace `<auth_code>` form last step, use the value of `App Key` and `App secret` to replace `<client_id>` and `<client_secret>` )
```
curl curl https://api.dropbox.com/oauth2/token \
    -d code=<auth_code> \
    -d grant_type=authorization_code \
    -d client_id=<client_id> \
    -d client_secret=<client_secret>
```
Then you will get the non-expired `refresh_token` from the response payload.


Note, The access token generated from the settings page is no longer a long lived token. We have to go though the manual step to get the refresh token. Besides, the `refresh_token` is a non-expired token. Check the [Q&A](https://www.dropboxforum.com/t5/Dropbox-API-Support-Feedback/Re-How-to-get-refresh-token-without-User-interaction/m-p/655435/highlight/true#M29847) for more details.

`clientid`: You can get this from your app page.

`clientsecret`: You can get this from your app page.