package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/aws/aws-lambda-go/lambda"
	_ "github.com/go-sql-driver/mysql"
	"pack.ag/amqp"
)

const (
	brokerEndPoint      = ""
	brokerAdminUserName = ""
	brokerAdminPassword = ""
	queueName           = ""
)

type Message struct {
	MailQueueID int `json:"mailQueueId"`
}

//Handler process the queue send email and update status of mailqueue item
func Handler() {
	// Create client
	fmt.Println("Connecting amazon mq.....")
	client, err := amqp.Dial(brokerEndPoint,
		amqp.ConnSASLPlain(brokerAdminUserName, brokerAdminPassword),
	)

	if err != nil {
		fmt.Println("Failed to connect amazon mq server")
	}
	defer client.Close()

	// Open a session
	session, err := client.NewSession()
	if err != nil {
		log.Fatal("Creating AMQP session:", err)
	}

	ctx := context.Background()
	{
		// Create a receiver
		receiver, err := session.NewReceiver(
			amqp.LinkSourceAddress(queueName),
			amqp.LinkCredit(10),
		)
		if err != nil {
			log.Fatal("Creating receiver link:", err)
		}
		defer func() {
			ctx, cancel := context.WithTimeout(ctx, 1*time.Second)
			receiver.Close(ctx)
			cancel()
		}()

		message := &Message{}
		for {
			// Receive next message in the queue
			msg, err := receiver.Receive(ctx)
			if err != nil {
				log.Fatal("Reading message from AMQP:", err)
			}

			// Accept message
			msg.Accept()
			err = json.Unmarshal(msg.GetData(), message)
			if err != nil {
				fmt.Println("Could not decode the message:", err.Error())
			}

			//Call new umg end point to send email
			mailQueueID := message.MailQueueID
			fmt.Println("Sending mail with mailQueueId:", mailQueueID)
			status := sendEmail(mailQueueID)

			//Update mailqueue table
			fmt.Println("Updating mail queue with status:", status)
			updateMailQueue(mailQueueID, status)

			//notify the coordinator
			//todo turn this on when coordinator end point is ready to integate
			notifyMailCoordinator(mailQueueID, status)
		}
	}
}

func main() {
	lambda.Start(Handler)
	//Handler()
}
