package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"time"

	"cloud.google.com/go/functions/metadata"
)

type Request struct {
	Body string `json:"body"`
}

type Response struct {
	StatusCode int    `json:"statusCode"`
	Body       string `json:"body"`
}

func handler(w http.ResponseWriter, r *http.Request) {
	incomeTimestamp := time.Now().Unix()

	var event map[string]interface{}
	if err := json.NewDecoder(r.Body).Decode(&event); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	meta, err := metadata.FromContext(r.Context())
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	reqID := meta.EventID
	event["request-id"] = reqID
	event["income-timestamp"] = incomeTimestamp

	begin := time.Now()
	result, err := functionHandler(event)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	end := time.Now()

	logData := map[string]interface{}{
		"output": result["result"],
	}
	if measurement, ok := result["measurement"]; ok {
		logData["measurement"] = measurement
	}
	if logs, ok := event["logs"].(map[string]interface{}); ok {
		logData["time"] = end.Sub(begin).Microseconds()
		resultsBegin := time.Now()
		bucket := logs["bucket"].(string)
		if err := uploadLogData(bucket, reqID, logData); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		resultsEnd := time.Now()
		logData["results_time"] = resultsEnd.Sub(resultsBegin).Microseconds()
	} else {
		logData["results_time"] = 0
	}

	isCold := false
	containerID := ""
	if _, err := os.Stat("/tmp/cold_run"); os.IsNotExist(err) {
		isCold = true
		containerID = generateContainerID()
		if err := ioutil.WriteFile("/tmp/cold_run", []byte(containerID), 0644); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
	} else {
		data, err := ioutil.ReadFile("/tmp/cold_run")
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		containerID = string(data)
	}

	coldStartVar := os.Getenv("cold_start")

	response := map[string]interface{}{
		"begin":          begin.UnixNano() / int64(time.Millisecond),
		"end":            end.UnixNano() / int64(time.Millisecond),
		"results_time":   logData["results_time"],
		"is_cold":        isCold,
		"result":         logData,
		"request_id":     reqID,
		"cold_start_var": coldStartVar,
		"container_id":   containerID,
	}

	responseBody, err := json.Marshal(response)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	w.Write(responseBody)
}

func functionHandler(event map[string]interface{}) (map[string]interface{}, error) {
	// Implement your function logic here
	return map[string]interface{}{
		"result": "Hello, World!",
	}, nil
}

func uploadLogData(bucket, reqID string, logData map[string]interface{}) error {
	// Implement your log data upload logic here
	return nil
}

func generateContainerID() string {
	return fmt.Sprintf("%x", time.Now().UnixNano())
}

func main() {
	http.HandleFunc("/", handler)
	log.Fatal(http.ListenAndServe(":8080", nil))
}
