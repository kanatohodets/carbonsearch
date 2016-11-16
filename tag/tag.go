package tag

import (
	"fmt"
	"strings"
)

type ServiceKey uint64
type Key uint64

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

func getPositions(tag string) (int, int, error) {
	serviceDelimiter := strings.Index(tag, "-")
	kvMarker := strings.Index(tag, ":")
	hasDots := strings.Index(tag, ".")
	//TODO(nnuss): graphite allows '\' escaping dots
	if hasDots != -1 {
		return 0, 0, fmt.Errorf("tag: %q is an invalid tag: it contains full-stop characters, which will confuse graphite clients", tag)
	}

	// has a '-', has a ':', and the ':' has at least one character between it and the '-'
	if serviceDelimiter == -1 || kvMarker == -1 || kvMarker < serviceDelimiter+1 {
		return 0, 0, fmt.Errorf("tag: %q is an invalid tag, should be: service-key:value", tag)
	}

	return serviceDelimiter, kvMarker, nil
}
