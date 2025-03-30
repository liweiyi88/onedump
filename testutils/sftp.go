//go:build !coverage

package testutils

import (
	"io"
	"log"
	"log/slog"
	"net"
	"os"

	"github.com/pkg/sftp"
	"golang.org/x/crypto/ssh"
)

type fileLister struct {
	files []os.FileInfo
}

func (fl *fileLister) ListAt(list []os.FileInfo, offset int64) (int, error) {
	// Calculate the starting index based on the offset
	start := offset
	if start >= int64(len(fl.files)) {
		return 0, nil // No more files to list
	}

	// List files starting from the offset
	end := min(start+int64(len(list)), int64(len(fl.files)))

	// Copy files into the provided list
	copy(list, fl.files[start:end])

	// Return the number of files copied
	return int(end - start), nil
}

type SftpHanlder struct{}

func (sh *SftpHanlder) Filelist(req *sftp.Request) (sftp.ListerAt, error) {
	path := req.Filepath
	slog.Info("[sftp] listing directory", slog.Any("path", path))

	fileInfo, err := os.Stat(path)
	if err != nil {
		return nil, err
	}

	if fileInfo.IsDir() {
		slog.Info("[sftp] path is a directory return its info", slog.Any("path", path))
		return &fileLister{files: []os.FileInfo{fileInfo}}, nil
	}

	// List the contents of the directory
	files, err := os.ReadDir(path)
	if err != nil {
		return nil, err
	}

	if len(files) == 0 {
		// Return a lister containing just the directory itself
		return &fileLister{files: []os.FileInfo{fileInfo}}, nil
	}

	// Convert the files from []fs.DirEntry to []os.FileInfo
	var fileInfoList []os.FileInfo
	for _, file := range files {
		info, err := file.Info()
		if err != nil {
			return nil, err
		}
		fileInfoList = append(fileInfoList, info)
	}

	return &fileLister{files: fileInfoList}, nil
}

func (sh *SftpHanlder) Filewrite(req *sftp.Request) (io.WriterAt, error) {
	filePath := req.Filepath

	slog.Info("[sftp] writing file", slog.Any("file", filePath))

	file, err := os.Create(filePath)
	if err != nil {
		return nil, err
	}

	return file, nil
}

func (sh *SftpHanlder) Fileread(req *sftp.Request) (io.ReaderAt, error) {
	filePath := req.Filepath
	slog.Info("[sftp] reading file", slog.Any("file", filePath))

	file, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}

	return file, nil
}

func StartSftpServer(address string, privateKey string, numberOfRequests int, onClient func()) error {
	sshConfig := &ssh.ServerConfig{
		PublicKeyCallback: func(c ssh.ConnMetadata, pubKey ssh.PublicKey) (*ssh.Permissions, error) {
			return &ssh.Permissions{
				// Record the public key used for authentication.
				Extensions: map[string]string{
					"pubkey-fp": ssh.FingerprintSHA256(pubKey),
				},
			}, nil
		},
	}

	private, err := ssh.ParsePrivateKey([]byte(privateKey))
	if err != nil {
		return err
	}

	sshConfig.AddHostKey(private)
	listener, err := net.Listen("tcp", address)

	if err != nil {
		return err
	}

	defer func() {
		if err = listener.Close(); err != nil {
			slog.Error("fail to close listner", slog.Any("error", err))
		}
	}()

	go func() {
		onClient()
	}()

	for range numberOfRequests {
		nConn, err := listener.Accept()
		if err != nil {
			return err
		}

		conn, chans, reqs, err := ssh.NewServerConn(nConn, sshConfig)
		if err != nil {
			return err
		}

		slog.Info("SSH logged in", slog.Any("key", conn.Permissions.Extensions["pubkey-fp"]))

		go ssh.DiscardRequests(reqs)

		for newChannel := range chans {
			if newChannel.ChannelType() != "session" {
				newChannel.Reject(ssh.UnknownChannelType, "unknown channel type")
				continue
			}

			channel, requests, err := newChannel.Accept()
			if err != nil {
				return err
			}

			go func() {
				defer func() {
					if err = channel.Close(); err != nil {
						slog.Error("fail to close ssh channel", slog.Any("error", err))
					}
				}()

				HandleSftpRequests(requests, channel)
			}()
		}
	}

	return nil
}

func HandleSftpRequests(requests <-chan *ssh.Request, channel ssh.Channel) {
	for req := range requests {
		if req.Type == "subsystem" && string(req.Payload[4:]) == "sftp" {
			req.Reply(true, nil)

			server := sftp.NewRequestServer(channel, sftp.Handlers{
				FileGet: &SftpHanlder{},
				FilePut: &SftpHanlder{},
				// FileCmd:  FileCmder,
				FileList: &SftpHanlder{},
			})

			if err := server.Serve(); err != nil && err != io.EOF {
				log.Printf("SFTP server exited with error: %v", err)
			}

			return
		}

		req.Reply(false, nil)
	}
}
