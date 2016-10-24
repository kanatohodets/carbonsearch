package tag

import (
	"testing"

	"github.com/kanatohodets/carbonsearch/util"
)

func TestParse(t *testing.T) {
	validCases := map[string][]string{
		"server-state:live":                          {"server", "state:live"},
		"discovery-status:live":                      {"discovery", "status:live"},
		"server-dc:lhr":                              {"server", "dc:lhr"},
		"lb-pool:www":                                {"lb", "pool:www"},
		"custom-favorites:btyler":                    {"custom", "favorites:btyler"},
		"server-interfaces:eth1:ip_address:10_1_2_3": {"server", "interfaces:eth1:ip_address:10_1_2_3"},
	}

	for valid, expected := range validCases {
		service, kv, err := Parse(valid)
		if err != nil {
			t.Errorf("tag test: %q failed to parse: %q", valid, err)
			continue
		}

		if service != expected[0] {
			t.Errorf("tag test: %q ought to have service %q, but it has %q instead", valid, expected[0], service)
		}

		if kv != expected[1] {
			t.Errorf("tag test: %q ought to have kv %q, but it has %q instead", valid, expected[1], kv)
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
		service, kv, err := Parse(invalid)
		if err == nil {
			t.Errorf("tag test: %q failed to error while parsing. tokens: %q %q", invalid, service, kv)
		}
	}
}

func TestWriteKey(t *testing.T) {
	h := util.HashStr64
	validCases := map[string]Key{
		"server-state:live":                          Key(h("server-state")),
		"discovery-status:live":                      Key(h("discovery-status")),
		"server-dc:lhr":                              Key(h("server-dc")),
		"lb-pool:www":                                Key(h("lb-pool")),
		"custom-favorites:btyler":                    Key(h("custom-favorites")),
		"server-interfaces:eth1:ip_address:10_1_2_3": Key(h("server-interfaces")),
	}

	for valid, expected := range validCases {
		writeKey, err := WriteKey(valid)
		if err != nil {
			t.Errorf("tag test: %q failed to parse: %q.", valid, err)
			continue
		}

		if writeKey != expected {
			t.Errorf("tag test: the write key for tag %q ought to be %q, but instead it is %q", valid, expected, writeKey)
		}
	}
}
