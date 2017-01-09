package tag

import (
	"fmt"
	"strings"
)

type ServiceKey uint64
type Key uint64

const serviceToKey = "-"
const keyToValue = ":"

// Parse separates a "service-key:value" tag into "service", "key", and "value". If the tag is malformed an error is returned.
func Parse(tag string) (string, string, string, error) {
	serviceMark, kvMarker, err := getPositions(tag)
	if err != nil {
		return "", "", "", err
	}

	service := tag[0:serviceMark]
	k := tag[serviceMark+1 : kvMarker]
	v := tag[kvMarker+1:]
	return service, k, v, nil
}

// ParseService is sugar on Parse() when you only care about the service
func ParseService(tag string) (string, error) {
	s, _, _, err := Parse(tag)
	return s, err
}

// Validate checks whether the tag can be parsed
func Validate(tag string) error {
	_, _, err := getPositions(tag)
	return err
}

func RelaxedParse(tag string) (string, string, string, error) {
	var service, key, value string
	serviceMark, kvMark, _ := getPositions(tag)
	// no delimiters, so must be a partial service
	if serviceMark == -1 && kvMark == -1 {
		return tag, "", "", nil
	}

	if serviceMark == -1 && kvMark != -1 {
		return "", "", "", fmt.Errorf("this tag (%q) has a key:value delimiter, but no service-key delimiter. this makes no sense!", tag)
	}

	service = tag[0:serviceMark]
	// no kv delimiter (as in ':' in servers-dc:us_east), so remainder of tag is key
	if kvMark == -1 {
		key = tag[serviceMark+1:]
		return service, key, "", nil
	}

	if kvMark < serviceMark {
		return "", "", "", fmt.Errorf("this tag (%q) has delimiter characters in it, but in the wrong order. giving up!", tag)
	}
	if kvMark == serviceMark+1 {
		return "", "", "", fmt.Errorf("this tag (%q) has delimiter characters in it, but they're directly adjacent. giving up!", tag)
	}

	key = tag[serviceMark+1 : kvMark]
	value = tag[kvMark+1:]
	return service, key, value, nil
}

func getPositions(tag string) (int, int, error) {
	serviceDelimiter := strings.Index(tag, serviceToKey)
	kvMarker := strings.Index(tag, keyToValue)

	// has a '-', has a ':', and the ':' has at least one character between it and the '-'
	if serviceDelimiter == -1 || kvMarker == -1 || kvMarker < serviceDelimiter+1 {
		return serviceDelimiter, kvMarker, fmt.Errorf("tag: %q is an invalid tag, should be: service-key:value", tag)
	}

	return serviceDelimiter, kvMarker, nil
}

// NeedsKey returns true if the tag appears to lack the 'key' portion, as in: servers-
func NeedsKey(partialTag string) bool {
	firstServiceToKey := strings.Index(partialTag, serviceToKey)
	return firstServiceToKey == len(partialTag)-1
}

// NeedsValue returns true if the tag appears to lack the 'value' portion, as in: servers-dc:
func NeedsValue(partialTag string) bool {
	firstKeyToValue := strings.Index(partialTag, keyToValue)
	return firstKeyToValue == len(partialTag)-1
}
