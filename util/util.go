package util

import (
	"expvar"
	"fmt"
	"gopkg.in/yaml.v2"
	"hash/fnv"
	"io/ioutil"
	"strconv"
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

	ServicesByIndex *expvar.Map

	SplitIndexes *expvar.Map
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

		SplitIndexes: expvar.NewMap("SplitIndexes"),

		ServicesByIndex: expvar.NewMap("ServicesByIndex"),
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
		return fmt.Errorf("error while reading path %q: %s", path, err)
	}

	err = yaml.Unmarshal(bytes, dest)
	if err != nil {
		return fmt.Errorf("error parsing %q: %s", path, err)
	}
	return nil
}

func HashStr32(data string) uint32 {
	h := fnv.New32()
	h.Write([]byte(data))
	return h.Sum32()
}

func HashStr64(data string) uint64 {
	h := fnv.New64()
	h.Write([]byte(data))
	return h.Sum64()
}
