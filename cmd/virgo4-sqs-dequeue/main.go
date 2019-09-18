package main

import (
	"fmt"
	"log"
	"os"
	"time"
)

//
// main entry point
//
func main() {

	log.Printf("===> %s service staring up <===", os.Args[ 0 ] )

	// Get config params and use them to init service context. Any issues are fatal
	cfg := LoadConfiguration()

	// load our AWS_SQS helper object
	aws, err := NewAwsSqs( AwsSqsConfig{ } )
	if err != nil {
		log.Fatal( err )
	}

	// get the queue handle from the queue name
	inQueueHandle, err := aws.QueueHandle( cfg.InQueueName )
	if err != nil {
		log.Fatal( err )
	}

	fileix := 0
    for {

		//log.Printf("Waiting for messages...")
		start := time.Now()

		// wait for a batch of messages
		messages, err := aws.BatchMessageGet( inQueueHandle, uint( MAX_SQS_BLOCK_COUNT), time.Duration( cfg.PollTimeOut ) * time.Second )
		if err != nil {
			log.Fatal( err )
		}

		// did we receive any?
		sz := len( messages )
		if sz != 0 {

			//log.Printf( "Received %d messages", sz )

			for _, m := range messages {

				// write to a file
				err = writeMessage( fmt.Sprintf( "%s/message.%05d", cfg.OutDir, fileix ), m.Payload )
				if err != nil {
					log.Fatal( err )
				}
				fileix++
			}

			// delete them all
			opStatus, err := aws.BatchMessageDelete( inQueueHandle, messages )
			if err != nil {
				log.Fatal( err )
			}

			// check the operation results
			for ix, op := range opStatus {
				if op == false {
					log.Printf( "WARNING: message %d failed to delete", ix )
				}
			}

			duration := time.Since(start)
			log.Printf("Processed %d messages (%0.2f tps)", sz, float64( sz ) / duration.Seconds() )
		} else {
			log.Printf("No messages received...")
		}
	}
}

func writeMessage( filename string, contents Payload ) error {

	file, err := os.Create( filename )

	if err != nil {
		return( err )
	}
	defer file.Close()

	_, err = file.Write( []byte( contents ) )

	if err != nil {
		return( err )
	}
	//log.Printf("Written %s", filename )
	return nil
}

//
// end of file
//