package toc

import (
	"fmt"
	"strings"
	"sync"

	"github.com/kanatohodets/carbonsearch/index/split"
)

type serviceT string
type keyT string
type valueT string

type metricCounter struct {
	count int
}

type splitEntry struct {
	joins   map[split.Join]*metricCounter
	entries map[serviceT]map[keyT]map[valueT]map[*metricCounter]struct{}
}

// this is for humans to look at and figure out what's available for querying
type TableOfContents struct {
	mut   sync.RWMutex
	table map[string]*splitEntry
}

func NewToC() *TableOfContents {
	return &TableOfContents{
		table: map[string]*splitEntry{},
	}
}

//										   index	  service    key        value  # of metrics
func (toc *TableOfContents) GetTable() map[string]map[string]map[string]map[string]int {
	toc.mut.RLock()
	defer toc.mut.RUnlock()

	res := map[string]map[string]map[string]map[string]int{}
	for indexName, splitEntry := range toc.table {
		res[indexName] = map[string]map[string]map[string]int{}
		for typedService, keyMap := range splitEntry.entries {
			service := string(typedService)
			res[indexName][service] = map[string]map[string]int{}
			for typedKey, valueMap := range keyMap {
				key := string(typedKey)
				res[indexName][service][key] = map[string]int{}
				for typedValue, metricCounterMap := range valueMap {
					value := string(typedValue)
					for metricCounter, _ := range metricCounterMap {
						res[indexName][service][key][value] += metricCounter.count
					}
				}
			}
		}
	}
	return res
}

func (toc *TableOfContents) SetJoinMetricCount(index string, join split.Join, metricCount int) {
	toc.mut.Lock()
	defer toc.mut.Unlock()

	se, ok := toc.table[index]
	if !ok {
		panic(fmt.Sprintf("trying to set metric count for an index (%q) the ToC doesn't know about!", index))
	}

	counter, ok := se.joins[join]
	if !ok {
		counter = &metricCounter{}
		se.joins[join] = counter
	}
	counter.count = metricCount
}

func (toc *TableOfContents) AddSplitEntry(index, service string) {
	toc.mut.Lock()
	defer toc.mut.Unlock()

	se, ok := toc.table[index]
	if !ok {
		se = &splitEntry{
			joins:   map[split.Join]*metricCounter{},
			entries: map[serviceT]map[keyT]map[valueT]map[*metricCounter]struct{}{},
		}
	}
	se.entries[serviceT(service)] = map[keyT]map[valueT]map[*metricCounter]struct{}{}
	toc.table[index] = se
}

func (toc *TableOfContents) AddJoinTag(index, service, key, value string, join split.Join) {
	toc.mut.Lock()
	defer toc.mut.Unlock()

	typedService := serviceT(service)
	typedKey := keyT(key)
	typedValue := valueT(value)

	se, ok := toc.table[index]
	if !ok {
		panic(fmt.Sprintf("trying to add a tag to the ToC for an index (%q) the ToC doesn't know about!", index))
	}

	keys, ok := se.entries[typedService]
	if !ok {
		keys = map[keyT]map[valueT]map[*metricCounter]struct{}{}
		se.entries[typedService] = keys
	}

	values, ok := keys[typedKey]
	if !ok {
		values = map[valueT]map[*metricCounter]struct{}{}
		keys[typedKey] = values
	}
	joinsForValue, ok := values[typedValue]
	if !ok {
		joinsForValue = map[*metricCounter]struct{}{}
		values[typedValue] = joinsForValue
	}

	counter, ok := se.joins[join]
	if !ok {
		counter = &metricCounter{}
		se.joins[join] = counter
	}

	joinsForValue[counter] = struct{}{}
}

func (toc *TableOfContents) CompleteKey(index, service, key string) []string {
	toc.mut.RLock()
	defer toc.mut.RUnlock()

	keysForService := toc.getCompleterKeys(index, service)
	results := []string{}
	for completeKey, _ := range keysForService {
		strKey := string(completeKey)
		// if it turns out that the given key a full key already, we should offer value completions
		// we'll keep going in case it's also a prefix of something else
		if strKey == key {
			valueCompletions := toc.CompleteValue(index, service, key, "")
			results = append(results, valueCompletions...)
		} else if strings.HasPrefix(strKey, key) {
			results = append(results, fmt.Sprintf("%s-%s:", service, strKey))
		}
	}
	return results
}

func (toc *TableOfContents) CompleteValue(index, service, key, value string) []string {
	toc.mut.RLock()
	defer toc.mut.RUnlock()

	keysForService := toc.getCompleterKeys(index, service)
	valuesForKey, ok := keysForService[keyT(key)]
	if !ok {
		return []string{}
	}

	results := []string{}
	for completeValue, _ := range valuesForKey {
		strValue := string(completeValue)
		if strings.HasPrefix(strValue, value) {
			results = append(results, fmt.Sprintf("%s-%s:%s", service, key, strValue))
		}
	}
	return results
}

func (toc *TableOfContents) getCompleterKeys(index, service string) map[keyT]map[valueT]map[*metricCounter]struct{} {
	split, ok := toc.table[index]
	if !ok {
		panic(fmt.Sprintf("toc getCompleterKeys was given an index (%q) that it didn't know about. this means that either 1) not enough validation in db.Autocomplete, or 2) the database set of indexes has somehow drifted out of sync with the ones in the ToC, which should be impossible", index))
	}

	keysForService, ok := split.entries[serviceT(service)]
	if !ok {
		panic(fmt.Sprintf("toc getCompleterKeys was given a service (%q) that the associated index (%q) didn't know about. this means that either 1) not enough validation in db.Autocomplete, or 2) database service to index mapping is wrong somehow", service, index))
	}
	return keysForService
}
