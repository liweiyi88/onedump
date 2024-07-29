package testutils

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"fmt"
)

func GenerateRSAPrivateKey() (string, error) {
	key, err := rsa.GenerateKey(rand.Reader, 4096)
	if err != nil {
		return "", fmt.Errorf("could not genereate rsa key pair %w", err)
	}

	keyPEM := pem.EncodeToMemory(
		&pem.Block{
			Type:  "RSA PRIVATE KEY",
			Bytes: x509.MarshalPKCS1PrivateKey(key),
		},
	)

	return string(keyPEM), nil
}
