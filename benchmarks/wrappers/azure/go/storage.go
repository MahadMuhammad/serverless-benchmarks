package main

import (
	"bytes"
	"context"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"

	"github.com/Azure/azure-storage-blob-go/azblob"
)

type Storage struct {
	serviceURL azblob.ServiceURL
}

func NewStorage(accountName, accountKey string) *Storage {
	credential, err := azblob.NewSharedKeyCredential(accountName, accountKey)
	if err != nil {
		log.Fatalf("failed to create credential: %v", err)
	}

	pipeline := azblob.NewPipeline(credential, azblob.PipelineOptions{})
	URL, _ := url.Parse(fmt.Sprintf("https://%s.blob.core.windows.net", accountName))
	serviceURL := azblob.NewServiceURL(*URL, pipeline)

	return &Storage{
		serviceURL: serviceURL,
	}
}

func (s *Storage) Upload(container, blob, filepath string) error {
	file, err := os.Open(filepath)
	if err != nil {
		return fmt.Errorf("failed to open file %q, %v", filepath, err)
	}
	defer file.Close()

	containerURL := s.serviceURL.NewContainerURL(container)
	blobURL := containerURL.NewBlockBlobURL(blob)

	_, err = azblob.UploadFileToBlockBlob(context.Background(), file, blobURL, azblob.UploadToBlockBlobOptions{})
	if err != nil {
		return fmt.Errorf("failed to upload file %q to container %q, %v", filepath, container, err)
	}

	return nil
}

func (s *Storage) Download(container, blob, filepath string) error {
	containerURL := s.serviceURL.NewContainerURL(container)
	blobURL := containerURL.NewBlockBlobURL(blob)

	response, err := blobURL.Download(context.Background(), 0, azblob.CountToEnd, azblob.BlobAccessConditions{}, false)
	if err != nil {
		return fmt.Errorf("failed to download blob %q from container %q, %v", blob, container, err)
	}
	defer response.Body(azblob.RetryReaderOptions{}).Close()

	body, err := ioutil.ReadAll(response.Body(azblob.RetryReaderOptions{}))
	if err != nil {
		return fmt.Errorf("failed to read blob %q from container %q, %v", blob, container, err)
	}

	err = ioutil.WriteFile(filepath, body, 0644)
	if err != nil {
		return fmt.Errorf("failed to write file %q, %v", filepath, err)
	}

	return nil
}

func (s *Storage) UploadStream(container, blob string, data []byte) error {
	containerURL := s.serviceURL.NewContainerURL(container)
	blobURL := containerURL.NewBlockBlobURL(blob)

	_, err := azblob.UploadBufferToBlockBlob(context.Background(), data, blobURL, azblob.UploadToBlockBlobOptions{})
	if err != nil {
		return fmt.Errorf("failed to upload stream to container %q, %v", container, err)
	}

	return nil
}

func (s *Storage) DownloadStream(container, blob string) ([]byte, error) {
	containerURL := s.serviceURL.NewContainerURL(container)
	blobURL := containerURL.NewBlockBlobURL(blob)

	response, err := blobURL.Download(context.Background(), 0, azblob.CountToEnd, azblob.BlobAccessConditions{}, false)
	if err != nil {
		return nil, fmt.Errorf("failed to download stream from container %q, %v", container, err)
	}
	defer response.Body(azblob.RetryReaderOptions{}).Close()

	body, err := ioutil.ReadAll(response.Body(azblob.RetryReaderOptions{}))
	if err != nil {
		return nil, fmt.Errorf("failed to read stream from container %q, %v", container, err)
	}

	return body, nil
}

func (s *Storage) DownloadDirectory(container, prefix, path string) error {
	containerURL := s.serviceURL.NewContainerURL(container)

	for marker := (azblob.Marker{}); marker.NotDone(); {
		listBlob, err := containerURL.ListBlobsFlatSegment(context.Background(), marker, azblob.ListBlobsSegmentOptions{
			Prefix: prefix,
		})
		if err != nil {
			return fmt.Errorf("failed to list blobs in container %q with prefix %q, %v", container, prefix, err)
		}

		for _, blobInfo := range listBlob.Segment.BlobItems {
			blob := blobInfo.Name
			filepath := filepath.Join(path, blob)
			dir := filepath.Dir(filepath)
			if err := os.MkdirAll(dir, 0755); err != nil {
				return fmt.Errorf("failed to create directory %q, %v", dir, err)
			}

			if err := s.Download(container, blob, filepath); err != nil {
				return fmt.Errorf("failed to download blob %q, %v", blob, err)
			}
		}

		marker = listBlob.NextMarker
	}

	return nil
}

func main() {
	accountName := os.Getenv("AZURE_STORAGE_ACCOUNT")
	accountKey := os.Getenv("AZURE_STORAGE_KEY")
	storage := NewStorage(accountName, accountKey)

	// Example usage
	err := storage.Upload("my-container", "my-blob", "path/to/file")
	if err != nil {
		log.Fatalf("failed to upload file: %v", err)
	}

	err = storage.Download("my-container", "my-blob", "path/to/downloaded/file")
	if err != nil {
		log.Fatalf("failed to download file: %v", err)
	}

	data := []byte("example data")
	err = storage.UploadStream("my-container", "my-stream-blob", data)
	if err != nil {
		log.Fatalf("failed to upload stream: %v", err)
	}

	streamData, err := storage.DownloadStream("my-container", "my-stream-blob")
	if err != nil {
		log.Fatalf("failed to download stream: %v", err)
	}
	fmt.Println("Downloaded stream data:", string(streamData))

	err = storage.DownloadDirectory("my-container", "my-prefix", "path/to/download/directory")
	if err != nil {
		log.Fatalf("failed to download directory: %v", err)
	}
}
