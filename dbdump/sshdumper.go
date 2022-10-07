package dbdump

import (
	"bytes"
	"fmt"
	"log"
	"os"

	"golang.org/x/crypto/ssh"
)

type SSHDumper struct {
	User           string
	Host           string
	Port           string
	PrivateKeyFile string
	DbType         string
	ExecutableDumper
}

func (sshDumper *SSHDumper) Dump(dumpFile string) {
	host := sshDumper.Host + ":" + sshDumper.Port

	pKey, err := os.ReadFile(sshDumper.PrivateKeyFile)
	if err != nil {
		log.Fatalln("can not read the private key file")
	}

	signer, err := ssh.ParsePrivateKey(pKey)
	if err != nil {
		log.Fatalln("failed to create singer", err)
	}

	conf := &ssh.ClientConfig{
		User:            sshDumper.User,
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

	command, err := sshDumper.GetExecutableCommand(dumpFile)
	if err !=nil {
		log.Fatal(err)
	}

	if err := session.Run(command); err != nil {
		log.Fatal(remoteErr.String())
	}

	fmt.Println(remoteOut.String())

	// if provide download option, we download the db to local.
}
