package database

import (
	"fmt"
	"sort"
	"strings"

	"github.com/kanatohodets/carbonsearch/tag"
)

func (db *Database) Autocomplete(partialTag string) []string {
	var result []string
	globless := strings.TrimSuffix(partialTag, "*")
	service, key, value, err := tag.RelaxedParse(globless)
	if err != nil {
		return []string{}
	}

	idx, ok := db.serviceToIndex[service]
	validService := ok
	// either a prefix of a real service, or a bogus one. in either case
	// 'services' is as deep as our completion should go
	if !validService {
		for existingService, _ := range db.serviceToIndex {
			if strings.HasPrefix(existingService, service) {
				result = append(result, fmt.Sprintf("%s-", existingService))
			}
		}
		sort.Strings(result)
		return result
	}

	// text index is special -- only one 'key', and no values.
	if service == db.textIndexService {
		// don't do any autocompletion if the person has already typed anything
		// in the query bit, or if the query bit is all that's left to write
		if value != "" || tag.NeedsValue(globless) {
			return []string{}
		} else {
			return []string{fmt.Sprintf("%s-match:<your_query>", db.textIndexService)}
		}
	}

	// something like: 'servers-', or 'servers-d', but not 'servers-dc:' or 'servers-dc:us'
	needsKey := tag.NeedsKey(globless) || (value == "" && !tag.NeedsValue(globless))
	if needsKey {
		result = db.toc.CompleteKey(idx.Name(), service, key)
		sort.Strings(result)
		return result
	}

	result = db.toc.CompleteValue(idx.Name(), service, key, value)
	sort.Strings(result)
	return result
}
