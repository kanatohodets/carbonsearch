package tag

import (
	"fmt"
	"strings"

	"github.com/kanatohodets/carbonsearch/util"
)

type Key uint64

// Parse separates a "service-key:value" tag into "service", "key", and "value". If the tag is malformed an error is returned.
func Parse(tag string) (string, string, string, error) {
	serviceMark, kvMarker, err := validate(tag)
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

// WriteKey returns a hash representing the write identity for this tag. The
// write identity is the portion of the tag that's needed to update a value ("service-key" in "service-key:value")
func WriteKey(tag string) (Key, error) {
	_, kvMark, err := validate(tag)
	if err != nil {
		return 0, err
	}
	serviceKey := tag[0:kvMark]
	return Key(util.HashStr64(serviceKey)), nil
}

func validate(tag string) (int, int, error) {
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
