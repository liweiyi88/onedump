package gdrive

import (
	"strings"
	"testing"

	"github.com/liweiyi88/onedump/storage"
	"github.com/stretchr/testify/assert"
)

func TestSave(t *testing.T) {
	gdrive := &GDrive{
		Email:      "myaccount@onedump.iam.gserviceaccount.com",
		FileName:   "onedump.sql",
		FolderId:   "13GbhhbpBeJmUIzm9lET63nXgWgdh3Tly",
		PrivateKey: "key",
	}

	reader := strings.NewReader("hello gdrive")

	err := gdrive.Save(reader, storage.PathGenerator(true, true))
	assert.NotNil(t, err)
}
