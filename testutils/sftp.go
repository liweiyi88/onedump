//go:build !coverage

package testutils

import (
	"io"
	"log"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"runtime"
	"strings"

	"github.com/pkg/sftp"
	"golang.org/x/crypto/ssh"
)

type fileLister struct {
	files []os.FileInfo
}

func (fl *fileLister) ListAt(list []os.FileInfo, offset int64) (int, error) {
	start := offset
	if start >= int64(len(fl.files)) {
		return 0, nil
	}

	end := min(start+int64(len(list)), int64(len(fl.files)))

	copy(list, fl.files[start:end])

	return int(end - start), nil
}

type SftpHandler struct{}

func fromSftpPath(sftpPath string) string {
	if runtime.GOOS == "windows" {
		if len(sftpPath) > 3 && sftpPath[0] == '/' && sftpPath[2] == ':' && sftpPath[3] == '/' {
			// Extract drive letter and remaining path
			drive := strings.ToUpper(string(sftpPath[1]))
			remainingPath := sftpPath[3:]

			// Convert forward slashes to backslashes
			windowsPath := drive + ":" + filepath.FromSlash(remainingPath)
			return windowsPath
		}
	}
	return filepath.FromSlash(sftpPath)
}

func (sh *SftpHandler) Filelist(req *sftp.Request) (sftp.ListerAt, error) {
	requestPath := req.Filepath
	path := fromSftpPath(requestPath)

	slog.Debug("[sftp] listing directory", slog.Any("request path", requestPath), slog.Any("path", path))

	fileInfo, err := os.Stat(path)
	if err != nil {
		return nil, err
	}

	if fileInfo.IsDir() {
		slog.Debug("[sftp] path is a directory return its info", slog.Any("path", path))
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

func (sh *SftpHandler) Filewrite(req *sftp.Request) (io.WriterAt, error) {
	filePath := req.Filepath
	path := fromSftpPath(filePath)

	slog.Debug("[sftp] writing file", slog.Any("request path", filePath), slog.Any("path", path))

	file, err := os.Create(path)
	if err != nil {
		return nil, err
	}

	return file, nil
}

func (sh *SftpHandler) Fileread(req *sftp.Request) (io.ReaderAt, error) {
	filePath := req.Filepath
	path := fromSftpPath(filePath)

	slog.Debug("[sftp] reading file", slog.Any("request path", filePath), slog.Any("path", path))

	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}

	return file, nil
}

func StartSftpServer(address string, numberOfRequests int, onServerReady func(privateKey string)) error {
	privateKey, err := GenerateRSAPrivateKey()
	if err != nil {
		return err
	}

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
		onServerReady(privateKey)
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

		slog.Debug("[ssh] SSH logged in", slog.Any("key", conn.Permissions.Extensions["pubkey-fp"]))

		go ssh.DiscardRequests(reqs)

		for newChannel := range chans {
			if newChannel.ChannelType() != "session" {
				if err := newChannel.Reject(ssh.UnknownChannelType, "unknown channel type"); err != nil {
					slog.Error("fail to reject new channel", slog.Any("error", err))
				}

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
			if err := req.Reply(true, nil); err != nil {
				slog.Error("failed to reply to subsystem request", slog.Any("error", err))
			}

			server := sftp.NewRequestServer(channel, sftp.Handlers{
				FileGet:  &SftpHandler{},
				FilePut:  &SftpHandler{},
				FileList: &SftpHandler{},
			})

			if err := server.Serve(); err != nil && err != io.EOF {
				log.Printf("SFTP server exited with error: %v", err)
			}

			return
		}

		if err := req.Reply(false, nil); err != nil {
			slog.Error("failed to reply with false", slog.Any("error", err))
		}
	}
}
