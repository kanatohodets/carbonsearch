package database

import (
	"fmt"
	"sync"

	"github.com/kanatohodets/carbonsearch/index/split"
)

type serviceT string
type keyT string
type valueT string

type joinEntry struct {
	metricCount int
}

type splitEntry struct {
	joins   map[split.Join]*joinEntry
	entries map[serviceT]map[keyT]map[valueT]map[*joinEntry]struct{}
}

// this is for humans to look at and figure out what's available for querying
type tableOfContents struct {
	mut   sync.RWMutex
	table map[string]*splitEntry
}

func NewToC() *tableOfContents {
	return &tableOfContents{
		table: map[string]*splitEntry{},
	}
}

//										   index	  service    key        value  # of metrics
func (toc *tableOfContents) GetTable() map[string]map[string]map[string]map[string]int {
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
				for typedValue, joinEntryMap := range valueMap {
					value := string(typedValue)
					for joinEntry, _ := range joinEntryMap {
						res[indexName][service][key][value] += joinEntry.metricCount
					}
				}
			}
		}
	}
	return res
}

func (toc *tableOfContents) SetMetricCount(index string, join split.Join, metricCount int) {
	toc.mut.Lock()
	defer toc.mut.Unlock()

	se, ok := toc.table[index]
	if !ok {
		panic(fmt.Sprintf("trying to set metric count for an index (%q) the ToC doesn't know about!", index))
	}

	je, ok := se.joins[join]
	if !ok {
		je = &joinEntry{}
		se.joins[join] = je
	}
	je.metricCount = metricCount
}

func (toc *tableOfContents) AddSplitEntry(index, service string) {
	toc.mut.Lock()
	defer toc.mut.Unlock()

	se, ok := toc.table[index]
	if !ok {
		se = &splitEntry{
			joins:   map[split.Join]*joinEntry{},
			entries: map[serviceT]map[keyT]map[valueT]map[*joinEntry]struct{}{},
		}
	}
	se.entries[serviceT(service)] = map[keyT]map[valueT]map[*joinEntry]struct{}{}
	toc.table[index] = se
}

func (toc *tableOfContents) AddTag(index, service, key, value string, join split.Join) {
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
		keys = map[keyT]map[valueT]map[*joinEntry]struct{}{}
		se.entries[typedService] = keys
	}

	values, ok := keys[typedKey]
	if !ok {
		values = map[valueT]map[*joinEntry]struct{}{}
		keys[typedKey] = values
	}
	joinsForValue, ok := values[typedValue]
	if !ok {
		joinsForValue = map[*joinEntry]struct{}{}
		values[typedValue] = joinsForValue
	}

	je, ok := se.joins[join]
	if !ok {
		je = &joinEntry{}
		se.joins[join] = je
	}

	joinsForValue[je] = struct{}{}
}
