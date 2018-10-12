package main

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"time"
)

const (
	dbName              = ""
	dbUser              = ""
	dbPassword          = ""
	dbHost              = ""
	umgEndPoint         = "http://umg.local/massMailer/sendEmail"
	coordinatorEndPoint = "http://umg.local/mailqueue-coordinator-end-point"
)

//UMGResponse structure of response from UMG endpoint
type UMGResponse struct {
	Status string `json:"status"`
}

func sendEmail(mailQueueID int) string {
	jsonData := map[string]int{"mailQueueId": mailQueueID}
	jsonValue, _ := json.Marshal(jsonData)
	request, _ := http.NewRequest("POST", umgEndPoint, bytes.NewBuffer(jsonValue))

	request.Header.Set("Content-Type", "application/json")
	timeout := time.Duration(5 * time.Second)
	client := http.Client{
		Timeout: timeout,
	}
	response, err := client.Do(request)

	if err != nil {
		fmt.Println("Oops, could not contact with umg end point")
	} else {
		data, _ := ioutil.ReadAll(response.Body)
		//extract the json response coming from UMG end point
		var resp = new(UMGResponse)
		err := json.Unmarshal(data, &resp)
		if err != nil {
			fmt.Println("Oops, could not extract status from umg response", err)
		}

		return resp.Status
	}

	return "FAILED"
}

func notifyMailCoordinator(mailQueueID int, status string) {
	jsonData := map[string]int{"mailQueueId": mailQueueID}
	jsonValue, _ := json.Marshal(jsonData)
	request, _ := http.NewRequest("POST", coordinatorEndPoint, bytes.NewBuffer(jsonValue))
	request.Header.Set("Content-Type", "application/json")
	client := &http.Client{}
	_, err := client.Do(request)

	if err != nil {
		fmt.Printf("The HTTP request failed with error %s\n", err)
	}
}

func updateMailQueue(mailQueueID int, status string) {
	dataSourceName := dbUser + ":" + dbPassword + "@tcp(" + dbHost + ":3306)/" + dbName
	db, err := sql.Open("mysql", dataSourceName)
	if err != nil {
		log.Printf("Error connecting database: %v", err.Error())
	}
	defer db.Close()
	sql := "UPDATE mailqueue SET Status = ? WHERE id = ? LIMIT 1"
	stmt, err := db.Prepare(sql)
	if err != nil {
		log.Printf("Error on preparing database update %v", err.Error())
	}
	defer stmt.Close()
	_, err = stmt.Exec(status, mailQueueID)
	if err != nil {
		log.Printf("Error on updating mailqueue status %v", err.Error())
	}
}
