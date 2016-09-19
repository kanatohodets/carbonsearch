package consumer

import (
	"github.com/kanatohodets/carbonsearch/database"
	"sync"
)

type Consumer interface {
	Name() string
	Start(*sync.WaitGroup, *database.Database) error
	Stop() error
}
