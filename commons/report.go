package commons

import (
	"encoding/json"
	"io/ioutil"
	"time"
)

type Report struct {
	ReportContent map[string]interface{}
}

func (r Report) Json() (jsonresult string, err error) {
	bodystr, err := json.MarshalIndent(r, "", " ")
	return string(bodystr), err
}

func (r Report) JsonToFile() error {

	now := time.Now().Format("20060102150405000")
	filename := "report_" + now + ".json"
	bodystr, err := json.MarshalIndent(r.ReportContent, "", " ")

	if err != nil {
		return err
	}
	writeerr := ioutil.WriteFile(filename, bodystr, 0666)
	if writeerr != nil {
		return writeerr
	}
	return nil

}
