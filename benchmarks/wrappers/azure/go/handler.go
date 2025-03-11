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

	"github.com/Azure/azure-functions-go/azfunc"
)

type Request struct {
	Body string `json:"body"`
}

type Response struct {
	StatusCode int    `json:"statusCode"`
	Body       string `json:"body"`
}

func handler(ctx context.Context, req *azfunc.Request) (*azfunc.Response, error) {
	incomeTimestamp := time.Now().Unix()

	var event map[string]interface{}
	if err := json.Unmarshal(req.Body, &event); err != nil {
		return azfunc.NewResponse(http.StatusBadRequest, nil), err
	}

	reqID := req.Header.Get("X-Request-Id")
	event["request-id"] = reqID
	event["income-timestamp"] = incomeTimestamp

	begin := time.Now()
	result, err := functionHandler(event)
	if err != nil {
		return azfunc.NewResponse(http.StatusInternalServerError, nil), err
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
			return azfunc.NewResponse(http.StatusInternalServerError, nil), err
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
			return azfunc.NewResponse(http.StatusInternalServerError, nil), err
		}
	} else {
		data, err := ioutil.ReadFile("/tmp/cold_run")
		if err != nil {
			return azfunc.NewResponse(http.StatusInternalServerError, nil), err
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
		return azfunc.NewResponse(http.StatusInternalServerError, nil), err
	}

	return azfunc.NewResponse(http.StatusOK, responseBody), nil
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
	azfunc.Start(handler)
}
