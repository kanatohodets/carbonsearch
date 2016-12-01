package httpapi

import (
	c "github.com/kanatohodets/carbonsearch/consumer"
)

// make sure that it implements the Consumer interface
var _ c.Consumer = &Consumer{}
