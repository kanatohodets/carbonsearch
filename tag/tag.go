package tag

import (
	"fmt"
	"strings"
)

// Parse separates a "service-key:value" tag into "service" and "key:value". If the tag is malformed an error is returned.
func Parse(tag string) (string, string, error) {
	serviceDelimiter := strings.Index(tag, "-")
	kvMarker := strings.Index(tag, ":")
	hasDots := strings.Index(tag, ".")
	if hasDots != -1 {
		return "", "", fmt.Errorf("tag: %q is an invalid tag: it contains full-stop characters, which will confuse graphite clients", tag)
	}

	// has a '-', has a ':', and the ':' has at least one character between it and the '-'
	if serviceDelimiter == -1 || kvMarker == -1 || kvMarker < serviceDelimiter+1 {
		return "", "", fmt.Errorf("tag: %q is an invalid tag, should be: service-key:value", tag)
	}

	service := tag[0:serviceDelimiter]
	kv := tag[serviceDelimiter+1:]
	return service, kv, nil
}
