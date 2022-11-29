package solr

import (
	"bytes"
	utils "data-injection/source/utils"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"time"

	log "github.com/sirupsen/logrus"
)

var (
	BatchSize = 5000
	start     = time.Now()
	totalDoc  = 0
)

type solr struct{}

var Solr solr

func (sr solr) Insert() {
	log.Println("Solr Insert triggered....")
	port, _ := strconv.Atoi(os.Getenv("SOLR_PORT"))
	s, err := Init(os.Getenv("SOLR_HOST"), port, os.Getenv("SOLR_CORE"))
	if err != nil {
		log.Println(err)
		return
	}
	path := os.Getenv("FILE_PATH")
	fileExt := filepath.Ext(path)
	log.Println("ext1:", fileExt)
	var json []byte
	if fileExt == ".csv" {
		// csv file
		data, err := utils.ReadAndParseCsv(path)
		if err != nil {
			panic(fmt.Sprintf("error while handling csv file: %s\n", err))
		}
		json, err = utils.CsvToJson(data)
		if err != nil {
			panic(fmt.Sprintf("error while converting csv to json file: %s\n", err))
		}
	} else {
		// json file
		json, err = os.ReadFile(path)
		if err != nil {
			log.Println(err)
		}
	}
	x := bytes.TrimLeft(json, " \t\r\n")
	isArray := len(x) > 0 && x[0] == '['
	//isObject := len(x) > 0 && x[0] == '{'
	var (
		queue = make(chan map[string]interface{})
		wg    sync.WaitGroup
		i     = 0
		//numWorkers = runtime.NumCPU()
		numWorkers = 10
	)
	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go Worker(fmt.Sprintf("worker-%d", i), queue, &wg, s)
	}
	if isArray {
		log.Println("It's Array.")
		commonData, err := utils.ByteToArray(json)
		if err != nil {
			log.Println(err)
		}
		totalDoc = len(commonData)
		log.Println("No of documents to inserts:", totalDoc)
		log.Println("Inserting ducuments to solr............")
		for _, v := range commonData {
			queue <- v
			i++
		}
	} else {
		log.Println("It's Object.")
		commonData := utils.ByteToObject(json)
		queue <- commonData

	}

	close(queue)
	wg.Wait()
	defer func() {
		log.Println(" Total Workers :", numWorkers)
		log.Println(" Total Documents :", totalDoc)
		log.Println(" Total Time taken :", time.Since(start))
	}()

}

func Worker(id string, lines chan map[string]interface{}, wg *sync.WaitGroup, s *Connection) {
	defer wg.Done()
	i := 0
	var bulkData []map[string]interface{}
	for data := range lines {
		i++
		bulkData = append(bulkData, data)
		if i%BatchSize == 0 {
			msg := make([]map[string]interface{}, len(bulkData))
			if n := copy(msg, bulkData); n != len(bulkData) {
				log.Fatalf("%d docs in batch, but only %d copied", len(bulkData), n)
			}
			Upload(s, msg, id)
			if i%50000 == 0 {
				log.Println(" Total Time taken each 1 lac record :", i, id, time.Since(start))
			}
			bulkData = nil
		}

	}
	if len(bulkData) == 0 {
		return
	}
	msg := make([]map[string]interface{}, len(bulkData))
	copy(msg, bulkData)
	Upload(s, msg, id)
	log.Println(" Total Time taken each 1 lac record :", i, id, time.Since(start))
}

func Upload(s *Connection, data []map[string]interface{}, id string) {
	_, err := s.Update(data, true)
	if err != nil {
		log.Fatal(err)
	}
	//defer wg.Done()
}
