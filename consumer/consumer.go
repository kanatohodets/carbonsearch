package consumer

import (
	"sync"

	"github.com/kanatohodets/carbonsearch/database"
)

type Consumer interface {
	Name() string
	Start(*sync.WaitGroup, *database.Database) error
	Stop() error
}
