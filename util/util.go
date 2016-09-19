package util

import (
	"fmt"
	"gopkg.in/yaml.v2"
	"io/ioutil"
)

func ReadConfig(path string, dest interface{}) error {
	bytes, err := ioutil.ReadFile(path)
	if err != nil {
		return fmt.Errorf("error while reading path %q: %s", path, err)
	}

	err = yaml.Unmarshal(bytes, dest)
	if err != nil {
		return fmt.Errorf("error parsing %q: %s", path, err)
	}
	return nil
}
