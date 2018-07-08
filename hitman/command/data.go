package command

import (
	"encoding/json"
	"io/ioutil"
)

const TmpGroupOffsetFile = "tmpGroupOffset"

func SaveTmpGroupOffset(tmpGroupOffset map[string]map[int32]int64) error {
	output, err := json.Marshal(tmpGroupOffset)
	if err != nil {
		return err
	}
	return ioutil.WriteFile(TmpGroupOffsetFile, output, 0644)
}

func LoadTmpGroupOffset() (map[string]map[int32]int64, error) {
	input, err := ioutil.ReadFile(TmpGroupOffsetFile)
	if err != nil {
		return nil, err
	}
	tmpGroupOffset := map[string]map[int32]int64{}
	err = json.Unmarshal(input, &tmpGroupOffset)
	return tmpGroupOffset, err
}
