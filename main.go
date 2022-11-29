package main

import (
	source "data-injection/source"
	utils "data-injection/source/utils"
	"flag"
	"fmt"

	"github.com/joho/godotenv"
	log "github.com/sirupsen/logrus"
)

var (
	destination = flag.String("destination", "", "place to insert the data")
)

func main() {
	flag.Parse()
	//*destination = "rds"
	isValid, err := utils.Validate(*destination)
	if err != nil {
		log.Fatal(err)
	}
	if isValid {
		err = godotenv.Load(".env_" + *destination)
		if err != nil {
			log.Error("Error loading .env file")
		}
	}
	fmt.Println("flag value:", *destination)
	obj := source.Newinterface(*destination)
	if obj != nil {
		obj.Insert()
	}
}
