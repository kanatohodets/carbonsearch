package test

import (
	"math/rand"
	"sync"
)

var seed int64 = 232342358902345
var rnd *rand.Rand

var mut sync.Mutex

var initialized bool

var alpha string = "abcdefghijklmnopqrstuvwxyz"

func checkInit() {
	mut.Lock()
	if !initialized {
		rnd = rand.New(rand.NewSource(seed))
		initialized = true
	}
	mut.Unlock()
}

func Rand() *rand.Rand {
	checkInit()
	return rnd
}

func GetMetricCorpus(size int) []string {
	checkInit()
	return rwords(size, 120)
}

func GetTagCorpus(size int) []string {
	checkInit()
	return rwords(size, 15)
}

func GetJoinCorpus(size int) []string {
	checkInit()
	return rwords(size, 100)
}

func rwords(n int, wordMaxLen int) []string {
	words := make([]string, n)
	for i := 0; i < n; i++ {
		l := rnd.Intn(wordMaxLen) + 1
		word := make([]byte, l)
		for j := 0; j < l; j++ {
			word = append(word, rchr())
		}
		words = append(words, string(word))
	}
	return words
}

func rchr() byte {
	return alpha[rnd.Int()%26]
}
