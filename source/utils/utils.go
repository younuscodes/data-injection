package utils

import (
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"

	"io"
	"os"
	"strconv"
	"strings"
)

func ReadAndParseCsv(path string) ([][]string, error) {
	csvFile, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("error opening %s\n", path)
	}

	var rows [][]string

	reader := csv.NewReader(csvFile)
	for {
		row, err := reader.Read()
		if err == io.EOF {
			break
		}

		if err != nil {
			return rows, fmt.Errorf("failed to parse csv: %s", err)
		}

		rows = append(rows, row)
	}

	return rows, nil
}

func CsvToJson(rows [][]string) ([]byte, error) {
	var entries []map[string]interface{}
	attributes := rows[0]
	for _, row := range rows[1:] {
		entry := map[string]interface{}{}
		for i, value := range row {
			attribute := attributes[i]
			// split csv header key for nested objects
			objectSlice := strings.Split(attribute, ".")
			internal := entry
			for index, val := range objectSlice {
				// split csv header key for array objects
				key, arrayIndex := arrayContentMatch(val)
				if arrayIndex != -1 {
					if internal[key] == nil {
						internal[key] = []interface{}{}
					}
					internalArray := internal[key].([]interface{})
					if index == len(objectSlice)-1 {
						internalArray = append(internalArray, value)
						internal[key] = internalArray
						break
					}
					if arrayIndex >= len(internalArray) {
						internalArray = append(internalArray, map[string]interface{}{})
					}
					internal[key] = internalArray
					internal = internalArray[arrayIndex].(map[string]interface{})
				} else {
					if index == len(objectSlice)-1 {
						internal[key] = value
						break
					}
					if internal[key] == nil {
						internal[key] = map[string]interface{}{}
					}
					internal = internal[key].(map[string]interface{})
				}
			}
		}
		entries = append(entries, entry)
	}

	bytes, err := json.MarshalIndent(entries, "", "	")
	if err != nil {
		return nil, fmt.Errorf("Marshal error %s\n", err)
	}

	return bytes, nil
}

func arrayContentMatch(str string) (string, int) {
	i := strings.Index(str, "[")
	if i >= 0 {
		j := strings.Index(str, "]")
		if j >= 0 {
			index, _ := strconv.Atoi(str[i+1 : j])
			return str[0:i], index
		}
	}
	return str, -1
}

func ByteToArray(jsonStr []byte) ([]map[string]interface{}, error) {
	var decoded []map[string]interface{}
	err := json.Unmarshal(jsonStr, &decoded)
	if err != nil {
		return nil, err
	}
	/*for k, v := range decoded {
		fmt.Println("Insert values : ", k, v)
	}*/
	/*byteJson, _ := json.Marshal(decoded)
	fmt.Println(byteJson)*/
	return decoded, nil
}

func ByteToObject(jsonStr []byte) map[string]interface{} {
	var decoded map[string]interface{}
	json.Unmarshal(jsonStr, &decoded)
	/*for k, v := range decoded {
		fmt.Println("Insert value : ", k, v)
	}*/
	return decoded
}

func Validate(env string) (bool, error) {
	if env == "solr" || env == "mysql" || env == "postgres" || env == "rds" || env == "s3" {

		return true, nil
	}
	return false, errors.New("can't work with " + env)
}
