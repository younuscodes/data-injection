package mysql

import (
	"bytes"
	utils "data-injection/source/utils"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	log "github.com/sirupsen/logrus"

	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

var (
	BatchSize = 5000
	start     = time.Now()
	totalDoc  = 0
	table     = ""
)

type mySql struct{}

var Mysql mySql

//type Mysql struct{}

func (m mySql) Insert() {
	// init a Mysql connection
	log.Println("Mysql Insert triggered....")
	dsn := os.Getenv("MYSQL_USERNAME") + ":" + os.Getenv("MYSQL_PASSWORD") + "@tcp(" + os.Getenv("MYSQL_HOST") + ":" + os.Getenv("MYSQL_PORT") + ")" + "/" + os.Getenv("MYSQL_DB") + "?charset=utf8mb4&parseTime=True&loc=Local"
	db, err := gorm.Open(mysql.Open(dsn), &gorm.Config{
		SkipDefaultTransaction: true,
		PrepareStmt:            true,
		Logger:                 logger.Default.LogMode(logger.Error),
	})
	if err != nil {
		log.Println(err)
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
	table = os.Getenv("MYSQL_TABLE")
	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go Worker(fmt.Sprintf("worker-%d", i), queue, &wg, db)
	}
	if isArray {
		log.Println("It's Array.")
		commonData, err := utils.ByteToArray(json)
		if err != nil {
			log.Println(err)
		}
		totalDoc = len(commonData)
		log.Println("No of documents to inserts:", totalDoc)
		log.Println("Inserting ducuments to mysql............")
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

func Worker(id string, lines chan map[string]interface{}, wg *sync.WaitGroup, db *gorm.DB) {
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
			//we can use both Create and CreateInBatches but time taken for both methods are same
			/*db.Table(table).Create(
				msg,
			)*/

			db.Table(table).CreateInBatches(
				msg, len(msg),
			)
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
	/*db.Table(table).Create(
		msg,
	)*/
	db.Table(table).CreateInBatches(
		msg, len(msg),
	)
	log.Println(" Total Time taken each 1 lac record :", i, id, time.Since(start))
}
