package tag

import (
	"testing"
)

func TestParse(t *testing.T) {
	validCases := map[string][]string{
		"server-state:live":       []string{"server", "state:live"},
		"discovery-status:live":   []string{"discovery", "status:live"},
		"server-dc:lhr":           []string{"server", "dc:lhr"},
		"lb-pool:www":             []string{"lb", "pool:www"},
		"custom-favorites:btyler": []string{"custom", "favorites:btyler"},
	}

	for valid, expected := range validCases {
		service, kv, err := Parse(valid)
		if err != nil {
			t.Errorf("'%s' failed to parse: %s", valid, err)
			continue
		}

		if service != expected[0] {
			t.Errorf("'%s' ought to have service '%s', but it has '%s' instead", valid, expected[0], service)
		}

		if kv != expected[1] {
			t.Errorf("'%s' ought to have kv '%s', but it has '%s' instead", valid, expected[1], kv)
		}
	}

	invalidCases := []string{
		"asdfasdfaqwerioqwr",
		"::::-:--:;;;:0",
		"dc:lhr",
		"server",
		"btyler:favorites-custom",
	}

	for _, invalid := range invalidCases {
		service, kv, err := Parse(invalid)
		if err == nil {
			t.Errorf("'%s' failed to error while parsing. tokens: '%s' '%s'", invalid, service, kv)
		}
	}
}
