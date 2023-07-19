package gdrive

import (
	"context"
	"fmt"
	"io"

	"golang.org/x/oauth2/google"
	"golang.org/x/oauth2/jwt"
	drive "google.golang.org/api/drive/v3"

	"github.com/liweiyi88/onedump/fileutil"
)

type GDrive struct {
	// email of your google cloud service account
	Email string `yaml:"email" json:"client_email,omitempty"`
	// private key of your google cloud service account
	PrivateKey string `yaml:"privatekey" json:"private_key,omitempty"`
	FileName   string `yaml:"filename"`
	FolderId   string `yaml:"folderid"`
}

func (gdrive *GDrive) Save(reader io.Reader, gzip bool, unique bool) error {
	conf := &jwt.Config{
		Email:      gdrive.Email,
		PrivateKey: []byte(gdrive.PrivateKey),
		Scopes: []string{
			drive.DriveScope,
		},
		TokenURL: google.JWTTokenURL,
	}

	client := conf.Client(context.Background())

	driveClient, err := drive.New(client)
	if err != nil {
		return fmt.Errorf("could not create drive client error: %v", err)
	}

	path := fileutil.EnsureFileName(gdrive.FileName, gzip, unique)

	driveFile := &drive.File{Name: path}

	if gdrive.FolderId != "" {
		parents := make([]string, 0, 1)
		parents = append(parents, gdrive.FolderId)
		driveFile.Parents = parents
	}

	_, err = driveClient.Files.Create(driveFile).Media(reader).ProgressUpdater(func(now, size int64) { fmt.Printf("%d, %d\r", now, size) }).Do()

	if err != nil {
		return fmt.Errorf("failed to upload file to google drive: %v", err)
	}

	return nil
}
