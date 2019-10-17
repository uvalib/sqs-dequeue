package main

import (
	"flag"
	"log"
)

// ServiceConfig defines all of the service configuration parameters
type ServiceConfig struct {
	InQueueName       string
	MessageBucketName string
	OutDir            string
	PollTimeOut       int64
	MaxCount          uint
}

// LoadConfiguration will load the service configuration from env/cmdline
// and return a pointer to it. Any failures are fatal.
func LoadConfiguration() *ServiceConfig {

	var cfg ServiceConfig
	flag.StringVar(&cfg.InQueueName, "inqueue", "", "Inbound queue name")
	flag.StringVar(&cfg.MessageBucketName, "bucket", "", "Oversize message bucket name")
	flag.StringVar(&cfg.OutDir, "outdir", "", "Output directory name")
	flag.Int64Var(&cfg.PollTimeOut, "pollwait", 15, "Poll wait time (in seconds)")
	flag.UintVar(&cfg.MaxCount, "max", 0, "Maximum number of records to dequeue (0 is all of them)")

	flag.Parse()

	if len(cfg.InQueueName) == 0 {
		log.Fatalf("InQueueName cannot be blank")
	}
	if len(cfg.MessageBucketName) == 0 {
		log.Fatalf("MessageBucketName cannot be blank")
	}
	if len(cfg.OutDir) == 0 {
		log.Printf("OutDir is blank, messages will not be saved")
	}

	log.Printf("[CONFIG] InQueueName          = [%s]", cfg.InQueueName)
	log.Printf("[CONFIG] MessageBucketName    = [%s]", cfg.MessageBucketName)
	log.Printf("[CONFIG] OutDir               = [%s]", cfg.OutDir)
	log.Printf("[CONFIG] PollTimeOut          = [%d]", cfg.PollTimeOut)
	log.Printf("[CONFIG] MaxCount             = [%d]", cfg.MaxCount)

	return &cfg
}

//
// end of file
//
