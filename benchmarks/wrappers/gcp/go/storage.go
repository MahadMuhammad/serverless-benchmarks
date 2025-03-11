package main

import (
	"bytes"
	"context"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"

	"cloud.google.com/go/storage"
	"google.golang.org/api/option"
)

type Storage struct {
	client *storage.Client
}

func NewStorage() *Storage {
	ctx := context.Background()
	client, err := storage.NewClient(ctx, option.WithCredentialsFile("path/to/credentials.json"))
	if err != nil {
		log.Fatalf("failed to create client: %v", err)
	}
	return &Storage{
		client: client,
	}
}

func (s *Storage) Upload(bucket, object, filepath string) error {
	ctx := context.Background()
	file, err := os.Open(filepath)
	if err != nil {
		return fmt.Errorf("failed to open file %q, %v", filepath, err)
	}
	defer file.Close()

	wc := s.client.Bucket(bucket).Object(object).NewWriter(ctx)
	if _, err = io.Copy(wc, file); err != nil {
		return fmt.Errorf("failed to upload file %q to bucket %q, %v", filepath, bucket, err)
	}
	if err := wc.Close(); err != nil {
		return fmt.Errorf("failed to close writer for file %q, %v", filepath, err)
	}

	return nil
}

func (s *Storage) Download(bucket, object, filepath string) error {
	ctx := context.Background()
	rc, err := s.client.Bucket(bucket).Object(object).NewReader(ctx)
	if err != nil {
		return fmt.Errorf("failed to download file %q from bucket %q, %v", object, bucket, err)
	}
	defer rc.Close()

	body, err := ioutil.ReadAll(rc)
	if err != nil {
		return fmt.Errorf("failed to read file %q from bucket %q, %v", object, bucket, err)
	}

	err = ioutil.WriteFile(filepath, body, 0644)
	if err != nil {
		return fmt.Errorf("failed to write file %q, %v", filepath, err)
	}

	return nil
}

func (s *Storage) UploadStream(bucket, object string, data []byte) error {
	ctx := context.Background()
	wc := s.client.Bucket(bucket).Object(object).NewWriter(ctx)
	if _, err := wc.Write(data); err != nil {
		return fmt.Errorf("failed to upload stream to bucket %q, %v", bucket, err)
	}
	if err := wc.Close(); err != nil {
		return fmt.Errorf("failed to close writer for stream, %v", err)
	}

	return nil
}

func (s *Storage) DownloadStream(bucket, object string) ([]byte, error) {
	ctx := context.Background()
	rc, err := s.client.Bucket(bucket).Object(object).NewReader(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to download stream from bucket %q, %v", bucket, err)
	}
	defer rc.Close()

	body, err := ioutil.ReadAll(rc)
	if err != nil {
		return nil, fmt.Errorf("failed to read stream from bucket %q, %v", bucket, err)
	}

	return body, nil
}

func (s *Storage) DownloadDirectory(bucket, prefix, path string) error {
	ctx := context.Background()
	it := s.client.Bucket(bucket).Objects(ctx, &storage.Query{Prefix: prefix})

	for {
		objAttrs, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return fmt.Errorf("failed to list objects in bucket %q with prefix %q, %v", bucket, prefix, err)
		}

		object := objAttrs.Name
		filepath := filepath.Join(path, object)
		dir := filepath.Dir(filepath)
		if err := os.MkdirAll(dir, 0755); err != nil {
			return fmt.Errorf("failed to create directory %q, %v", dir, err)
		}

		if err := s.Download(bucket, object, filepath); err != nil {
			return fmt.Errorf("failed to download file %q, %v", object, err)
		}
	}

	return nil
}

func main() {
	storage := NewStorage()

	// Example usage
	err := storage.Upload("my-bucket", "my-object", "path/to/file")
	if err != nil {
		log.Fatalf("failed to upload file: %v", err)
	}

	err = storage.Download("my-bucket", "my-object", "path/to/downloaded/file")
	if err != nil {
		log.Fatalf("failed to download file: %v", err)
	}

	data := []byte("example data")
	err = storage.UploadStream("my-bucket", "my-stream-object", data)
	if err != nil {
		log.Fatalf("failed to upload stream: %v", err)
	}

	streamData, err := storage.DownloadStream("my-bucket", "my-stream-object")
	if err != nil {
		log.Fatalf("failed to download stream: %v", err)
	}
	fmt.Println("Downloaded stream data:", string(streamData))

	err = storage.DownloadDirectory("my-bucket", "my-prefix", "path/to/download/directory")
	if err != nil {
		log.Fatalf("failed to download directory: %v", err)
	}
}
