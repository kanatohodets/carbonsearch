package consumer

import (
	"sync"

	"github.com/kanatohodets/carbonsearch/database"
)

type Consumer interface {
	Name() string
	Start(*database.Database) error
	WaitUntilWarm(*sync.WaitGroup) error
	Stop() error
}
