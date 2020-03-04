package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"reflect"
	"strconv"
	"time"

	"github.com/influxdata/influxdb/client/v2"
)

func flattening(nested map[string]interface{}, prefix string, IgnoreKeyList []string) (map[string]interface{}, error) {
	flatmap := make(map[string]interface{})

	err := flatten(true, flatmap, nested, prefix, IgnoreKeyList)
	if err != nil {
		return nil, err
	}

	return flatmap, nil
}

func flatten(top bool, flatMap map[string]interface{}, nested interface{}, prefix string, ignorelist []string) error {
	var flag int

	assign := func(newKey string, v interface{}, ignoretag bool) error {
		if ignoretag {
			switch v.(type) {
			case map[string]interface{}, []interface{}:
				v, err := json.Marshal(&v)
				if err != nil {
					fmt.Printf("\n Not able to Marshal data for key:%s=%v", newKey, v)
					return err
				}
				flatMap[newKey] = string(v)
			default:
				flatMap[newKey] = v
			}

		} else {
			switch v.(type) {
			case map[string]interface{}, []interface{}:
				if err := flatten(false, flatMap, v, newKey, ignorelist); err != nil {
					fmt.Printf("\n Not able to flatten data for key:%s=%v", newKey, v)
					return err
				}
			default:
				flatMap[newKey] = v
			}
		}
		return nil
	}

	switch nested.(type) {
	case map[string]interface{}:
		for k, v := range nested.(map[string]interface{}) {

			ok := matchkey(ignorelist, k)

			if ok && prefix == "" {
				flag = 1
			} else if ok && prefix != "" {
				flag = 0
			} else {
				flag = -1
			}

			if flag == 1 {
				err := assign(k, v, true)
				if err != nil {
					return err
				}
			} else if flag == 0 {
				newKey := createkey(top, prefix, k)
				err := assign(newKey, v, true)
				if err != nil {
					return err
				}
			} else {
				newKey := createkey(top, prefix, k)
				err := assign(newKey, v, false)
				if err != nil {
					return err
				}
			}
		}
	case []interface{}:
		for i, v := range nested.([]interface{}) {
			switch v.(type) {
			case map[string]interface{}:
				for tag, value := range v.(map[string]interface{}) {
					ok := matchkey(ignorelist, tag)
					if ok {
						subkey := strconv.Itoa(i) + "." + tag
						newKey := createkey(top, prefix, subkey)
						err := assign(newKey, value, true)
						if err != nil {
							return err
						}
					} else {
						newKey := createkey(top, prefix, strconv.Itoa(i))
						err := assign(newKey, v, false)
						if err != nil {
							return err
						}
					}
				}
			default:
				newKey := createkey(top, prefix, strconv.Itoa(i))
				err := assign(newKey, v, false)
				if err != nil {
					return err
				}
			}

		}
	default:
		return errors.New("Not a valid input: map or slice")
	}

	return nil
}

func createkey(top bool, prefix, subkey string) string {
	key := prefix

	if top {
		key += subkey
	} else {
		key += "." + subkey
	}

	return key
}

func matchkey(ignorelist []string, value string) bool {

	for _, val := range ignorelist {
		if val == value {
			return true
		}
	}

	return false
}

func insertData(msg []byte, ignoreKeyList []string) {
	tags := make(map[string]string)
	field := make(map[string]interface{})
	data := make(map[string]interface{})

	err := json.Unmarshal(msg, &data)

	if err != nil {
		fmt.Printf("\n Not able to Parse data %s", err.Error())
	}

	flatjson, err := flattening(data, "", ignoreKeyList)
	if err != nil {
		fmt.Printf("\n Not able to flatten json %s for:%v", err.Error(), data)
	}

	fmt.Printf("\n Data after flattening: %v", flatjson)

	for key, value := range flatjson {
		if reflect.ValueOf(value).Type().Kind() == reflect.Float64 {
			field[key] = value
		} else if reflect.ValueOf(value).Type().Kind() == reflect.String {
			field[key] = value
		} else if reflect.ValueOf(value).Type().Kind() == reflect.Bool {
			field[key] = value
		} else if reflect.ValueOf(value).Type().Kind() == reflect.Int {
			field[key] = value
		}
	}

	Measurement := "demo"

	clientadmin, err := client.NewHTTPClient(client.HTTPConfig{
		Addr: "http://localhost:8086",
	})
	defer clientadmin.Close()

	query := client.NewQuery("create database go_influxdemo", "", "")
	_, err = clientadmin.Query(query)

	if err != nil {
		fmt.Printf("\n Error in creating database %s", err.Error())
		os.Exit(-1)
	}

	bp, err := client.NewBatchPoints(client.BatchPointsConfig{
		Database:  "go_influxdemo",
		Precision: "ns",
	})

	if err != nil {
		fmt.Printf("\n Error in creating batch point %s", err.Error())
	}

	pt, err := client.NewPoint(Measurement, tags, field, time.Now())
	if err != nil {
		fmt.Printf("\n point error %s", err.Error())
		os.Exit(-1)
	}

	bp.AddPoint(pt)

	if err := clientadmin.Write(bp); err != nil {
		fmt.Printf("\n Write Error %s", err.Error())
	}

}

func main() {
	data := []byte(`{"intdata": [10,24,43,56,45,78],
                      "floatdata": [56.67, 45.68, 78.12],
                      "nested_data": {
                          "key1": "string_data",
                          "key2": [45, 56],
                          "key3": [60.8, 45.78]
                      }}`)
	ignorelist := []string{"floatdata", "key2"}
	fmt.Println(ignorelist)
	insertData(data, ignorelist)
}
