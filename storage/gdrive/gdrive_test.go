package gdrive

import (
	"strings"
	"testing"
)

func TestSave(t *testing.T) {
	gdrive := &GDrive{
		Email:      "myaccount@onedump.iam.gserviceaccount.com",
		FileName:   "onedump.sql",
		FolderId:   "",
		PrivateKey: "key",
	}

	reader := strings.NewReader("hello gdrive")
	err := gdrive.Save(reader, true, true)
	if err == nil {
		t.Errorf("expected googld drive api error")
	}
}
