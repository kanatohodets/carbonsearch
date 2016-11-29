package toc

import (
	"fmt"

	"github.com/kanatohodets/carbonsearch/index"
	"github.com/kanatohodets/carbonsearch/index/split"
)

type metricCounter struct {
	count int
}

type tagTable map[serviceT]map[keyT]map[valueT]map[*metricCounter]struct{}

type indexEntry interface {
	SetMetricCount(uint64, int)
	AddTag(uint64, string, string, string)
	AddService(string) error
	getEntries() tagTable
}

type splitEntry struct {
	joins   map[split.Join]*metricCounter
	entries tagTable
}

func (se *splitEntry) SetMetricCount(hash uint64, metricCount int) {
	join := split.Join(hash)
	counter, ok := se.joins[join]
	if !ok {
		counter = &metricCounter{}
		se.joins[join] = counter
	}
	counter.count = metricCount
}

func (se *splitEntry) AddService(service string) error {
	typedService := serviceT(service)
	_, ok := se.entries[typedService]
	if ok {
		return fmt.Errorf("tried to add %v twice", service)
	}
	se.entries[typedService] = map[keyT]map[valueT]map[*metricCounter]struct{}{}
	return nil
}

func (se *splitEntry) AddTag(hash uint64, service, key, value string) {
	join := split.Join(hash)
	counter, ok := se.joins[join]
	if !ok {
		counter = &metricCounter{}
		se.joins[join] = counter
	}
	addTag(se, service, key, value, counter)
}

func (se *splitEntry) getEntries() tagTable {
	return se.entries
}

type fullEntry struct {
	tags    map[index.Tag]*metricCounter
	entries tagTable
}

func (fe *fullEntry) AddService(service string) error {
	typedService := serviceT(service)
	_, ok := fe.entries[typedService]
	if ok {
		return fmt.Errorf("tried to add %v twice", service)
	}
	fe.entries[typedService] = map[keyT]map[valueT]map[*metricCounter]struct{}{}
	return nil
}

func (fe *fullEntry) getEntries() tagTable {
	return fe.entries
}

func (fe *fullEntry) SetMetricCount(hash uint64, metricCount int) {
	tag := index.Tag(hash)
	counter, ok := fe.tags[tag]
	if !ok {
		counter = &metricCounter{}
		fe.tags[tag] = counter
	}
	counter.count = metricCount
}

func (fe *fullEntry) AddTag(hash uint64, service, key, value string) {
	tag := index.Tag(hash)
	counter, ok := fe.tags[tag]
	if !ok {
		counter = &metricCounter{}
		fe.tags[tag] = counter
	}
	addTag(fe, service, key, value, counter)
}

func addTag(ie indexEntry, service, key, value string, counter *metricCounter) {
	typedService := serviceT(service)
	typedKey := keyT(key)
	typedValue := valueT(value)

	entries := ie.getEntries()
	keys, ok := entries[typedService]
	if !ok {
		keys = map[keyT]map[valueT]map[*metricCounter]struct{}{}
		entries[typedService] = keys
	}

	values, ok := keys[typedKey]
	if !ok {
		values = map[valueT]map[*metricCounter]struct{}{}
		keys[typedKey] = values
	}
	countersForValue, ok := values[typedValue]
	if !ok {
		countersForValue = map[*metricCounter]struct{}{}
		values[typedValue] = countersForValue
	}

	countersForValue[counter] = struct{}{}
}
