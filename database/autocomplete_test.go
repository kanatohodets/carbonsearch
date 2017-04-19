package database

import (
	"fmt"
	"testing"

	"github.com/kanatohodets/carbonsearch/consumer/message"
)

func initAutocompleteTest(t *testing.T) *Database {
	db := New(queryLimit, resultLimit, fullService, textService, splitIndexes, stats)

	db.InsertCustom(&message.TagMetric{
		Tags:    []string{"custom-favorites:jdoe", "custom-mood:delighted"},
		Metrics: []string{"unused_in_this_test"},
	})

	populateSplitIndex(t, db, "basic autocomplete queries",
		"fqdn",
		map[string]map[string][]string{
			"unimportant_for_test_1": map[string][]string{
				"metrics": []string{
					"unused_in_this_test",
				},
				"tags": []string{
					"servers-hw:shiny",
					"servers-dc:us-west",
					"servers-status:live",
					"servers-statistically_interesting:yes",
					"servers-roles:foo",
				},
			},
			"unimportant_for_test_2": map[string][]string{
				"metrics": []string{
					"unused_in_this_test",
				},
				"tags": []string{
					"servers-roles:bar",
				},
			},
			"unimportant_for_test_3": map[string][]string{
				"metrics": []string{
					"unused_in_this_test",
				},
				"tags": []string{
					"servers-hw:rusty",
					"servers-dc:us-east",
					"servers-status:borked",
					"servers-roles:qux",
				},
			},
		},
	)
	return db
}

func TestServiceAutocomplete(t *testing.T) {
	db := initAutocompleteTest(t)
	autocompleteTestCase(t, db, "basic service completion", "ser*", []string{"servers-"})
	autocompleteTestCase(t, db, "basic service completion, full index", "cust*", []string{"custom-"})
	autocompleteTestCase(t, db, "no such service (or service prefix)", "foo_service*", []string{})
	autocompleteTestCase(t, db, "no service specified", "*", []string{"servers-", fmt.Sprintf("%s-", fullService), fmt.Sprintf("%s-", textService)})

	autocompleteTestCase(t, db, "service fully specified", "servers*", []string{
		"servers-hw:",
		"servers-dc:",
		"servers-status:",
		"servers-statistically_interesting:",
		"servers-roles:",
	})
	autocompleteTestCase(t, db, "service fully specified, full index", "custom*", []string{
		"custom-favorites:",
		"custom-mood:",
	})

	autocompleteTestCase(t, db, "service fully specified with trailing dash", "servers-*", []string{
		"servers-hw:",
		"servers-dc:",
		"servers-status:",
		"servers-statistically_interesting:",
		"servers-roles:",
	})
	autocompleteTestCase(t, db, "service fully specified with trailing dash, full index", "custom-*", []string{
		"custom-favorites:",
		"custom-mood:",
	})

}

func TestKeyAutocomplete(t *testing.T) {
	db := initAutocompleteTest(t)
	autocompleteTestCase(t, db, "partial key, one result", "servers-statu*", []string{
		"servers-status:",
	})
	autocompleteTestCase(t, db, "partial key, one result, full index", "custom-fav*", []string{
		"custom-favorites:",
	})

	autocompleteTestCase(t, db, "partial key, two results", "servers-stat*", []string{
		"servers-status:",
		"servers-statistically_interesting:",
	})

	autocompleteTestCase(t, db, "key fully specified", "servers-status*", []string{
		"servers-status:live",
		"servers-status:borked",
	})
	autocompleteTestCase(t, db, "key fully specified, full index", "custom-favorites*", []string{
		"custom-favorites:jdoe",
	})

	autocompleteTestCase(t, db, "key fully specified with trailing colon", "servers-status:*", []string{
		"servers-status:live",
		"servers-status:borked",
	})

	autocompleteTestCase(t, db, "key fully specified with trailing colon, full index", "custom-favorites:*", []string{
		"custom-favorites:jdoe",
	})

	//autocompleteTestCase(t, db, "key fully specified, but it's a bad one", "servers-blorg*", []string{})
}

func TestValueAutocomplete(t *testing.T) {
	db := initAutocompleteTest(t)
	autocompleteTestCase(t, db, "value partially specified", "servers-status:bo*", []string{
		"servers-status:borked",
	})

	autocompleteTestCase(t, db, "value partially specified, full index", "custom-mood:del*", []string{
		"custom-mood:delighted",
	})

	autocompleteTestCase(t, db, "value partially specified, trailing dash", "servers-dc:us-*", []string{
		"servers-dc:us-east",
		"servers-dc:us-west",
	})

}

func TestTextAutocomplete(t *testing.T) {
	db := initAutocompleteTest(t)
	autocompleteTestCase(t, db, "text service", textService+"*", []string{textMatchPrefix + "<your_query>"})
	autocompleteTestCase(t, db, "text service with trailing dash", textService+"-*", []string{textMatchPrefix + "<your_query>"})
	autocompleteTestCase(t, db, "text service with partial 'match'", textService+"-ma*", []string{textMatchPrefix + "<your_query>"})
	autocompleteTestCase(t, db, "text service with full 'match' key, no colon", textService+"-match*", []string{textMatchPrefix + "<your_query>"})
	autocompleteTestCase(t, db, "text service with full match and colon (no more completion, you'd mess with their query)", textService+"-match:*", []string{})
}

func TestJunkAutocomplete(t *testing.T) {
	db := initAutocompleteTest(t)
	// sort-of parseable nonsense
	autocompleteTestCase(t, db, "right characters, wrong places", "borked:status-servers*", []string{})
	autocompleteTestCase(t, db, "right characters, wrong places", "borked:-servers*", []string{})
}

func autocompleteTestCase(t *testing.T, db *Database, testName, query string, expectedCompletions []string) {
	expectedSet := map[string]struct{}{}
	for _, completion := range expectedCompletions {
		expectedSet[completion] = struct{}{}
	}

	if len(expectedSet) != len(expectedCompletions) {
		panic(fmt.Sprintf("%v: expected completions had some duplicate entries. this is a bug in the test definition", testName))
	}

	completions := db.Autocomplete(query)
	resultSet := map[string]struct{}{}
	for _, result := range completions {
		_, ok := expectedSet[result]
		if !ok {
			t.Errorf("autocomplete %v: found an unexpected completion result: %q", testName, result)
			return
		}
		resultSet[result] = struct{}{}
	}

	for expected, _ := range expectedSet {
		_, ok := resultSet[expected]
		if !ok {
			t.Errorf("autocomplete %v: expected %q in the results, but it wasn't there", testName, expected)
			return
		}
	}
}
