package commons

import (
	"bytes"
	"encoding/json"
	"gopkg.in/yaml.v2"
	"io/ioutil"
	"os"
	"sync"
)

var lock sync.Mutex

//YamlFileToMap Convert yaml fil to map
func YamlFileToMap(configfile string) (*map[interface{}]interface{}, error) {
	yamlmap := make(map[interface{}]interface{})
	yamlFile, err := ioutil.ReadFile(configfile)
	if err != nil {
		return nil, err
	}
	err = yaml.Unmarshal(yamlFile, yamlmap)
	if err != nil {
		return nil, err
	}
	return &yamlmap, nil
}

//MapToYamlString conver map to yaml
func MapToYamlString(yamlmap map[string]interface{}) (string, error) {
	lock.Lock()
	defer lock.Unlock()
	d, err := yaml.Marshal(&yamlmap)
	if err != nil {
		return "", err
	}
	return string(d), nil
}

func ParseJsonFile(filepath string) ([]byte, error) {
	jsonFile, err := os.Open(filepath)
	defer jsonFile.Close()

	if err != nil {
		return nil, err
	}

	jsonbytes, err := ioutil.ReadAll(jsonFile)
	if err != nil {
		return nil, err
	}
	return jsonbytes, nil
}

//struct 转 map
func Struct2Map(content interface{}) (map[string]interface{}, error) {
	var structmap map[string]interface{}
	if marshalContent, err := json.Marshal(content); err != nil {
		return nil, err
	} else {
		d := json.NewDecoder(bytes.NewReader(marshalContent))
		d.UseNumber() // 设置将float64转为一个number
		if err := d.Decode(&structmap); err != nil {
			return nil, err
		} else {
			for k, v := range structmap {
				structmap[k] = v
			}
		}
	}
	return structmap, nil
}
