package util

import (
	"expvar"
	"fmt"
	"io/ioutil"
	"strconv"

	"github.com/dchest/siphash"
	"gopkg.in/yaml.v2"
)

type Stats struct {
	TagMessages *expvar.Int
	TagsIndexed *expvar.Int

	MetricMessages *expvar.Int
	MetricsIndexed *expvar.Int

	CustomMessages   *expvar.Int
	FullIndexTags    *expvar.Int
	FullIndexMetrics *expvar.Int

	QueriesHandled     *expvar.Int
	QueryTagsByService *expvar.Map

	Progress *expvar.Map

	ServicesByIndex *expvar.Map

	SplitIndexes *expvar.Map

	TextIndex *expvar.Map

	Uptime *expvar.Int
}

func InitStats() *Stats {
	return &Stats{
		TagMessages: expvar.NewInt("TagMessages"),
		TagsIndexed: expvar.NewInt("TagsIndexed"),

		MetricMessages: expvar.NewInt("MetricMessages"),
		MetricsIndexed: expvar.NewInt("MetricsIndexed"),

		CustomMessages:   expvar.NewInt("CustomMessages"),
		FullIndexTags:    expvar.NewInt("FullIndexTags"),
		FullIndexMetrics: expvar.NewInt("FullIndexMetrics"),

		QueriesHandled:     expvar.NewInt("QueriesHandled"),
		QueryTagsByService: expvar.NewMap("QueryTagsByService"),

		Progress: expvar.NewMap("Progress"),

		SplitIndexes: expvar.NewMap("SplitIndexes"),

		TextIndex: expvar.NewMap("TextIndex"),

		ServicesByIndex: expvar.NewMap("ServicesByIndex"),

		Uptime: expvar.NewInt("Uptime"),
	}
}

type ExpInt int

func (i ExpInt) String() string { return strconv.Itoa(int(i)) }

type ExpString string

// this needs to quote the string so the output can be JSONified
func (s ExpString) String() string { return fmt.Sprintf("%q", string(s)) }

func ReadConfig(path string, dest interface{}) error {
	bytes, err := ioutil.ReadFile(path)
	if err != nil {
		return fmt.Errorf("util: error while reading path %q: %s", path, err)
	}

	err = yaml.Unmarshal(bytes, dest)
	if err != nil {
		return fmt.Errorf("util: error parsing %q: %s", path, err)
	}
	return nil
}

func HashStr64(data string) uint64 {
	return siphash.Hash(0, 0, []byte(data))
}
