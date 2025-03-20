package gdrive

import (
	"context"
	"fmt"
	"io"

	"golang.org/x/oauth2/google"
	"golang.org/x/oauth2/jwt"
	drive "google.golang.org/api/drive/v3"
	"google.golang.org/api/option"

	"github.com/liweiyi88/onedump/storage"
)

type GDrive struct {
	// email of your google cloud service account
	Email string `yaml:"email" json:"client_email,omitempty"`
	// private key of your google cloud service account
	PrivateKey string `yaml:"privatekey" json:"private_key,omitempty"`
	FileName   string `yaml:"filename"`
	FolderId   string `yaml:"folderid"`
}

func (gdrive *GDrive) Save(reader io.Reader, pathGenerator storage.PathGeneratorFunc) error {
	conf := &jwt.Config{
		Email:      gdrive.Email,
		PrivateKey: []byte(gdrive.PrivateKey),
		Scopes: []string{
			drive.DriveScope,
		},
		TokenURL: google.JWTTokenURL,
	}

	client := conf.Client(context.Background())

	driveClient, err := drive.NewService(context.Background(), option.WithHTTPClient(client))
	if err != nil {
		return fmt.Errorf("could not create drive client error: %v", err)
	}

	path := pathGenerator(gdrive.FileName)

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
