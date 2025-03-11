package main

import (
	"bytes"
	"context"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
)

type Storage struct {
	client *s3.S3
}

func NewStorage() *Storage {
	sess := session.Must(session.NewSession(&aws.Config{
		Region: aws.String("us-west-2"),
	}))
	return &Storage{
		client: s3.New(sess),
	}
}

func (s *Storage) Upload(bucket, key, filepath string) error {
	file, err := os.Open(filepath)
	if err != nil {
		return fmt.Errorf("failed to open file %q, %v", filepath, err)
	}
	defer file.Close()

	_, err = s.client.PutObject(&s3.PutObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
		Body:   file,
	})
	if err != nil {
		return fmt.Errorf("failed to upload file %q to bucket %q, %v", filepath, bucket, err)
	}

	return nil
}

func (s *Storage) Download(bucket, key, filepath string) error {
	result, err := s.client.GetObject(&s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return fmt.Errorf("failed to download file %q from bucket %q, %v", key, bucket, err)
	}
	defer result.Body.Close()

	body, err := ioutil.ReadAll(result.Body)
	if err != nil {
		return fmt.Errorf("failed to read file %q from bucket %q, %v", key, bucket, err)
	}

	err = ioutil.WriteFile(filepath, body, 0644)
	if err != nil {
		return fmt.Errorf("failed to write file %q, %v", filepath, err)
	}

	return nil
}

func (s *Storage) UploadStream(bucket, key string, data []byte) error {
	_, err := s.client.PutObject(&s3.PutObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
		Body:   bytes.NewReader(data),
	})
	if err != nil {
		return fmt.Errorf("failed to upload stream to bucket %q, %v", bucket, err)
	}

	return nil
}

func (s *Storage) DownloadStream(bucket, key string) ([]byte, error) {
	result, err := s.client.GetObject(&s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to download stream from bucket %q, %v", bucket, err)
	}
	defer result.Body.Close()

	body, err := ioutil.ReadAll(result.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read stream from bucket %q, %v", bucket, err)
	}

	return body, nil
}

func (s *Storage) DownloadDirectory(bucket, prefix, path string) error {
	objects, err := s.client.ListObjectsV2(&s3.ListObjectsV2Input{
		Bucket: aws.String(bucket),
		Prefix: aws.String(prefix),
	})
	if err != nil {
		return fmt.Errorf("failed to list objects in bucket %q with prefix %q, %v", bucket, prefix, err)
	}

	for _, obj := range objects.Contents {
		key := *obj.Key
		filepath := filepath.Join(path, key)
		dir := filepath.Dir(filepath)
		if err := os.MkdirAll(dir, 0755); err != nil {
			return fmt.Errorf("failed to create directory %q, %v", dir, err)
		}

		if err := s.Download(bucket, key, filepath); err != nil {
			return fmt.Errorf("failed to download file %q, %v", key, err)
		}
	}

	return nil
}

func main() {
	storage := NewStorage()

	// Example usage
	err := storage.Upload("my-bucket", "my-key", "path/to/file")
	if err != nil {
		log.Fatalf("failed to upload file: %v", err)
	}

	err = storage.Download("my-bucket", "my-key", "path/to/downloaded/file")
	if err != nil {
		log.Fatalf("failed to download file: %v", err)
	}

	data := []byte("example data")
	err = storage.UploadStream("my-bucket", "my-stream-key", data)
	if err != nil {
		log.Fatalf("failed to upload stream: %v", err)
	}

	streamData, err := storage.DownloadStream("my-bucket", "my-stream-key")
	if err != nil {
		log.Fatalf("failed to download stream: %v", err)
	}
	fmt.Println("Downloaded stream data:", string(streamData))

	err = storage.DownloadDirectory("my-bucket", "my-prefix", "path/to/download/directory")
	if err != nil {
		log.Fatalf("failed to download directory: %v", err)
	}
}
