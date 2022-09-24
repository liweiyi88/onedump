package main

import (
	"fmt"
	"log"
	"os"
	"bytes"
	"golang.org/x/crypto/ssh"
)

func main() {
	host := "host:port" // ip:port or domainName:port
	user := "root"

	homeDir, err := os.UserHomeDir()

	if err != nil{
		log.Fatalln(err)
	}

	pKey, err := os.ReadFile(homeDir + "/.ssh/id_rsa")
	if err != nil {
		log.Fatalln("can not read the private key file")
	}

	signer, err := ssh.ParsePrivateKey(pKey)
	if err != nil {
		log.Fatalln("failed to create singer", err)
	}

	conf := &ssh.ClientConfig{
		User:            user,
		HostKeyCallback: ssh.InsecureIgnoreHostKey(),
		Auth: []ssh.AuthMethod{
			ssh.PublicKeys(signer),
		},
	}

	client, err := ssh.Dial("tcp", host, conf)
	if err != nil {
		log.Fatalln("failed to dial: ", err)
	}

	defer client.Close()

	session, err := client.NewSession()
	if err != nil {
		log.Fatalln("failed to start session: ", err)
	}

	defer session.Close()

	var remoteOut bytes.Buffer
	var remoteErr bytes.Buffer

	session.Stdout = &remoteOut
	session.Stderr = &remoteErr
	if err := session.Run("mysqldump -h"); err != nil {
		log.Fatal(remoteErr.String())
	}

	fmt.Println(remoteOut.String())
}