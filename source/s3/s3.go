package s3

import (
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

var (
	awsS3Client *s3.Client
	start       = time.Now()
)

type s3struct struct{}

var S3 s3struct

func (s s3struct) Insert() {
	log.Println("S3 Insert triggered....")
	configS3()
	f, err := os.Open(os.Getenv("FILE_PATH"))
	if err != nil {
		panic(err)
	}
	defer f.Close()

	var r io.Reader
	r = f

	uploader := manager.NewUploader(awsS3Client)
	result, err := uploader.Upload(context.TODO(), &s3.PutObjectInput{
		Bucket: aws.String(os.Getenv("AWS_S3_BUCKET")),
		Key:    aws.String(os.Getenv("DEST_FILE_NAME")),
		Body:   r,
	})
	if err != nil {
		log.Fatal("error:", err)
	}
	fmt.Println("Successfully Uploaded to :", result.Location)
	fmt.Println(" Total Time taken to upload file to S3 bucket : ", time.Since(start))
}

func configS3() {

	creds := credentials.NewStaticCredentialsProvider(os.Getenv("AWS_ACCESS_KEY_ID"), os.Getenv("AWS_SECRET_KEY"), "")

	cfg, err := config.LoadDefaultConfig(context.TODO(), config.WithCredentialsProvider(creds), config.WithRegion(os.Getenv("AWS_S3_REGION")))
	if err != nil {
		log.Printf("error: %v", err)
		return
	}

	awsS3Client = s3.NewFromConfig(cfg)
}
func Download() {
	configS3()
	newFile, err := os.Create("downloa_s3data")
	if err != nil {
		log.Println(err)
	}
	defer newFile.Close()

	downloader := manager.NewDownloader(awsS3Client)
	numBytes, err := downloader.Download(context.TODO(), newFile, &s3.GetObjectInput{
		Bucket: aws.String(os.Getenv("AWS_S3_BUCKET")),
		Key:    aws.String(os.Getenv("DEST_FILE_NAME")),
	})
	if err != nil {
		log.Println(err)
	}
	fmt.Println("No.of bytes downloaded : ", numBytes)
	fmt.Println(" Total Time taken to download file from S3 bucket : ", time.Since(start))
}
