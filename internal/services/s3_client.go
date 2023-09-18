package services

import (
	"fmt"
	"log"
	"os"
	"sync"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
)

type S3Client struct {
	client *s3.S3

	Keys chan string
}

func NewS3Client() *S3Client {
	sess, err := session.NewSession(&aws.Config{
		Region: aws.String("us-east-1"),
	})

	if err != nil {
		log.Fatal("Error creating session:", err)
		os.Exit(1)
	}

	client := &S3Client{
		client: s3.New(sess),
		Keys:   make(chan string, 100),
	}

	return client
}

func (s *S3Client) ListAllObjects(bucket string, wg *sync.WaitGroup) {
	defer wg.Done()

	opts := &s3.ListObjectsInput{
		Bucket:    aws.String(bucket),
		Delimiter: aws.String("-"),
		Marker:    aws.String("tron.blocks.s3/000054070000/000054070330_0000000003390c3a9417a50bfe3cdb6e6853ec95d0d0b896a4820377a26d0b55_118fd68d63b78da42334f021e5b7f8c97caae5bd891c8af1e4889b9e459f79c1.block.lz4"),
	}

	for {
		resp, err := s.client.ListObjects(opts)
		if err != nil {
			log.Fatal("Error to get objects")
		}

		fmt.Printf("%d object was fetched\n", len(resp.Contents))

		for _, k := range resp.Contents {
			s.Keys <- *k.Key
		}

		if !*resp.IsTruncated {
			close(s.Keys)
			break
		}

		fmt.Println("Pagination..")
		opts.SetMarker(*resp.NextMarker)
	}

	fmt.Println("All s3 objects were received")
}

func (s *S3Client) RenameFile(bucket string, source string, dest string) {
	if err := s.CopyObject(bucket, source, dest); err != nil {
		log.Fatal(err)
	}

	if err := s.DeleteObject(bucket, source); err != nil {
		log.Fatal(err)
	}

	fmt.Printf("File '%s' was renamed successfully to '%s'\n", source, dest)
}

func (s *S3Client) CopyObject(bucket string, source string, dest string) error {
	copySource := bucket + "/" + source
	opts := &s3.CopyObjectInput{
		Bucket:     aws.String(bucket),
		CopySource: aws.String(copySource),
		Key:        aws.String(dest),
	}

	_, err := s.client.CopyObject(opts)
	if err != nil {
		return err
	}

	err = s.client.WaitUntilObjectExists(
		&s3.HeadObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(dest),
		},
	)
	if err != nil {
		return err
	}

	return nil
}

func (s *S3Client) DeleteObject(bucket string, key string) error {
	input := &s3.DeleteObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	}

	_, err := s.client.DeleteObject(input)
	if err != nil {
		return err
	}

	err = s.client.WaitUntilObjectNotExists(&s3.HeadObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return err
	}

	return nil
}
