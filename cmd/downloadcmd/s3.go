package downloadcmd

import (
	"context"

	"github.com/liweiyi88/onedump/env"
	"github.com/liweiyi88/onedump/storage/s3"
	"github.com/spf13/cobra"
)

var (
	bucket, prefix, dir string
)

func init() {
	DownloadS3Cmd.Flags().StringVarP(&bucket, "bucket", "b", "", "AWS S3 bucket name that used for saving binlog files (required)")
	DownloadS3Cmd.Flags().StringVarP(&prefix, "prefix", "p", "", "AWS S3 file prefix (folder) that used for saving binlog files (required)")
	DownloadS3Cmd.Flags().StringVarP(&dir, "dir", "d", "", "A local directory that stores the objects (required)")
	DownloadS3Cmd.MarkFlagRequired("bucket")
	DownloadS3Cmd.MarkFlagRequired("prefix")
	DownloadS3Cmd.MarkFlagRequired("dir")
}

var DownloadS3Cmd = &cobra.Command{
	Use:   "s3",
	Short: "Download files from a AWS S3 bucket to a local folder",
	Long: `Download files from a AWS S3 bucket to a local folder
It requires the following environment variables:
  - AWS_REGION
  - AWS_ACCESS_KEY_ID
  - AWS_SECRET_ACCESS_KEY

AWS_SESSION_TOKEN is optional unless you use a temporary credentials
`,
	RunE: func(cmd *cobra.Command, args []string) error {
		envs, err := env.NewEnvResolver(env.WithAWS()).Resolve()
		if err != nil {
			return err
		}

		credentials := envs.AWSCredentials

		return s3.NewS3(
			bucket,
			"",
			credentials.Region,
			credentials.AccessKeyID,
			credentials.SecretAccessKey,
			credentials.SessionToken).DownloadObjects(context.Background(), prefix, dir)
	},
}
