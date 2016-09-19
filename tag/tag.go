package tag

import (
	"fmt"
	"strings"
)

func Parse(tag string) (string, string, error) {
	serviceDelimiter := strings.Index(tag, "-")
	kvMarker := strings.Index(tag, ":")
	// has a '-', has a ':', and the ':' has at least one character between it and the '-'
	if serviceDelimiter == -1 || kvMarker == -1 || kvMarker < serviceDelimiter+1 {
		return "", "", fmt.Errorf("'%s' is an invalid tag, should be: service-key:value", tag)
	}

	service := tag[0:serviceDelimiter]
	kv := tag[serviceDelimiter+1:]
	return service, kv, nil
}
