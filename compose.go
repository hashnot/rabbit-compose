package main

import (
	"flag"
	"github.com/hashnot/rabbit-compose/rabbit"
	"gopkg.in/yaml.v2"
	"io/ioutil"
	"log"
)

func main() {
	fileName := flag.String("f", "rabbit-compose.yaml", "rabbit-compose.yaml file")
	flag.Parse()

	configData, err := ioutil.ReadFile(*fileName)
	failOnError(err, "Error reading config")

	config := new(rabbit.Deployment)

	err = yaml.Unmarshal(configData, config)
	failOnError(err, "Error parsing config")

	err = config.Setup()
	failOnError(err, "Error applying config")
}

func failOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
	}
}
