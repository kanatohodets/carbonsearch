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

	errorCases := map[string]string{
		"foobar":        "tag: \"foobar\" is an invalid tag, should be: service-key:value",
		"servers-cpus8": "tag: \"servers-cpus8\" is an invalid tag, should be: service-key:value",
	}

	for badTag, expectedError := range errorCases {
		_, _, _, err := Parse(badTag)
		if err == nil {
			t.Errorf("tag test: %q (a deliberately bad tag) failed to error while parsing", badTag)
		}

		if err.Error() != expectedError {
			t.Errorf("tag test: parsing %q did not yield the expected error. got: %q. expected %q", badTag, err.Error(), expectedError)
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

func TestNeedsKey(t *testing.T) {
	cases := map[string]bool{
		"server-state:live": false,
		"server-state":      false,
		"server":            false,
		"server-":           true,
	}
	for test, expected := range cases {
		res := NeedsKey(test)
		if res != expected {
			t.Errorf("tag NeedsKey test: %q got %v, expected %v", test, res, expected)
		}
	}
}

func TestNeedsValue(t *testing.T) {
	cases := map[string]bool{
		"server":            false,
		"server-":           false,
		"server-state":      false,
		"server-state:":     true,
		"server-state:live": false,
	}
	for test, expected := range cases {
		res := NeedsValue(test)
		if res != expected {
			t.Errorf("tag NeedsValue test: %q got %v, expected %v", test, res, expected)
		}
	}
}
