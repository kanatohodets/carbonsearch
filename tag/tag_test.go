package tag

import (
	"testing"
)

func TestParse(t *testing.T) {
	validCases := map[string][]string{
		"server-state:live":                          {"server", "state", "live"},
		"discovery-status:live":                      {"discovery", "status", "live"},
		"server-dc:lhr":                              {"server", "dc", "lhr"},
		"lb-pool:www":                                {"lb", "pool", "www"},
		"custom-favorites:btyler":                    {"custom", "favorites", "btyler"},
		"server-interfaces:eth1:ip_address:10_1_2_3": {"server", "interfaces", "eth1:ip_address:10_1_2_3"},
	}

	for valid, expected := range validCases {
		service, k, v, err := Parse(valid)
		if err != nil {
			t.Errorf("tag test: %q failed to parse: %q", valid, err)
			continue
		}

		if service != expected[0] {
			t.Errorf("tag test: %q ought to have service %q, but it has %q instead", valid, expected[0], service)
		}

		if k != expected[1] {
			t.Errorf("tag test: %q ought to have key %q, but it has %q instead", valid, expected[1], k)
		}

		if v != expected[2] {
			t.Errorf("tag test: %q ought to have value %q, but it has %q instead", valid, expected[2], v)
		}
	}

	invalidCases := []string{
		"asdfasdfaqwerioqwr",
		"::::-:--:;;;:0",
		"dc:lhr",
		"server",
		"btyler:favorites-custom",
		"btyler:favorites-custom",
		"server-interfaces:eth1:ip_address:10.1.2.3",
	}

	for _, invalid := range invalidCases {
		service, k, v, err := Parse(invalid)
		if err == nil {
			t.Errorf("tag test: %q failed to error while parsing. tokens: %q %q %q", invalid, service, k, v)
		}
	}
}

func TestRelaxedParse(t *testing.T) {
	validCases := map[string][]string{
		"":                  {"", "", ""},
		"server":            {"server", "", ""},
		"server-":           {"server", "", ""},
		"server-state":      {"server", "state", ""},
		"server-state:":     {"server", "state", ""},
		"server-state:live": {"server", "state", "live"},
	}

	for valid, expected := range validCases {
		service, k, v, err := RelaxedParse(valid)
		if err != nil {
			t.Errorf("tag test: %q failed to RelaxedParse: %q", valid, err)
			continue
		}

		if service != expected[0] {
			t.Errorf("tag RelaxedParse test: %q ought to have service %q, but it has %q instead", valid, expected[0], service)
		}

		if k != expected[1] {
			t.Errorf("tag RelaxedParse test: %q ought to have key %q, but it has %q instead", valid, expected[1], k)
		}

		if v != expected[2] {
			t.Errorf("tag RelaxedParse test: %q ought to have value %q, but it has %q instead", valid, expected[2], v)
		}
	}

	invalidCases := []string{
		"::::-:--:;;;:0",
		"dc:lhr",
		"btyler:favorites-custom",
		"btyler:favorites-custom",
	}

	for _, invalid := range invalidCases {
		service, k, v, err := RelaxedParse(invalid)
		if err == nil {
			t.Errorf("tag RelaxedParse test: %q failed to error while parsing. tokens: %q %q %q", invalid, service, k, v)
		}
	}
}
