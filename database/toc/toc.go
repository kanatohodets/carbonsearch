package toc

import (
	"fmt"
	"strings"
	"sync"

	"github.com/kanatohodets/carbonsearch/index"
	"github.com/kanatohodets/carbonsearch/index/split"
)

type serviceT string
type keyT string
type valueT string

// this is for humans to look at and figure out what's available for querying
type TableOfContents struct {
	mut   sync.RWMutex
	table map[string]indexEntry
}

func NewToC() *TableOfContents {
	return &TableOfContents{
		table: map[string]indexEntry{},
	}
}

//										   index	  service    key        value  # of metrics
func (toc *TableOfContents) GetTable() map[string]map[string]map[string]map[string]int {
	toc.mut.RLock()
	defer toc.mut.RUnlock()

	res := map[string]map[string]map[string]map[string]int{}
	for indexName, indexEntry := range toc.table {
		res[indexName] = map[string]map[string]map[string]int{}
		for typedService, keyMap := range indexEntry.getEntries() {
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

func (toc *TableOfContents) AddTag(indexName string, service, key, value string, hash uint64) {
	toc.mut.Lock()
	defer toc.mut.Unlock()

	ie, ok := toc.table[indexName]
	if !ok {
		panic(fmt.Sprintf("trying to add a tag for an index (%q) the ToC doesn't know about!", indexName))
	}

	ie.AddTag(hash, service, key, value)
}

func (toc *TableOfContents) SetMetricCount(index string, hash uint64, metricCount int) {
	toc.mut.Lock()
	defer toc.mut.Unlock()

	ie, ok := toc.table[index]
	if !ok {
		panic(fmt.Sprintf("trying to set metric count for an index (%q) the ToC doesn't know about!", index))
	}

	ie.SetMetricCount(hash, metricCount)
}

func (toc *TableOfContents) AddIndexServiceEntry(indexType, indexName, service string) {
	toc.mut.Lock()
	defer toc.mut.Unlock()

	ie, ok := toc.table[indexName]
	if !ok {
		// no sense having any text entries in the ToC
		if indexType == "text" {
			return
		}

		if indexType == "split" {
			ie = &splitEntry{
				joins:   map[split.Join]*metricCounter{},
				entries: tagTable{},
			}
		} else if indexType == "full" {
			ie = &fullEntry{
				tags:    map[index.Tag]*metricCounter{},
				entries: tagTable{},
			}
		}
		toc.table[indexName] = ie
	}
	err := ie.AddService(service)
	if err != nil {
		panic(fmt.Sprintf("could not register %v to split entry for index %v: %v", service, indexName, err))
	}
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
	ie, ok := toc.table[index]
	if !ok {
		panic(fmt.Sprintf("toc getCompleterKeys was given an index (%q) that it didn't know about. this means that either 1) not enough validation in db.Autocomplete, or 2) the database set of indexes has somehow drifted out of sync with the ones in the ToC, which should be impossible", index))
	}

	entries := ie.getEntries()

	keysForService, ok := entries[serviceT(service)]
	if !ok {
		panic(fmt.Sprintf("toc getCompleterKeys was given a service (%q) that the associated index (%q) didn't know about. this means that either 1) not enough validation in db.Autocomplete, or 2) database service to index mapping is wrong somehow", service, index))
	}
	return keysForService
}
