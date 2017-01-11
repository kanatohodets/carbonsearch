package httpapi

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/dgryski/carbonzipper/mlog"
	m "github.com/kanatohodets/carbonsearch/consumer/message"
	"github.com/kanatohodets/carbonsearch/database"
	"github.com/kanatohodets/carbonsearch/util"
)

var logger mlog.Level

// Config holds the contents of httpapi.yaml
type Config struct {
	WarmThreshold float32 `yaml:"warm_threshold"`
	Port          int     `yaml:"port"`
	Endpoint      string  `yaml:"endpoint"`
}

// Consumer represents a carbonsearch HTTP API data source: it listens for POST
// requests on '$endpoint/tag', '$endpoint/metric', and '$endpoint/custom'. The
// Consumer uses any received messages to populate the carbonsearch Database.
type Consumer struct {
	port     int
	endpoint string
	wg       *sync.WaitGroup

	warmThreshold float32
	progress      float32
	progressMut   sync.RWMutex
}

// New reads the HTTP API consumer config at the given path, and returns an
// initialized consumer, ready to Start.
func New(configPath string) (*Consumer, error) {
	config := &Config{}
	err := util.ReadConfig(configPath, config)
	if err != nil {
		return nil, err
	}

	if config.WarmThreshold > 0.01 {
		logger.Logf("HTTP consumer: warm threshold set to %v", config.WarmThreshold)
	} else {
		logger.Logf("HTTP consumer: warning, warm_threshold is very low or unset (value: %v). Carbonsearch may start serving requests before much data has been indexed from the HTTP API", config.WarmThreshold)
	}

	return &Consumer{
		port:     config.Port,
		endpoint: config.Endpoint,

		warmThreshold: config.WarmThreshold,
		progress:      0,
		progressMut:   sync.RWMutex{},
	}, nil
}

// WaitUntilWarm blocks until the HTTP consumer receives a message indicating
// that the index should be considered warm
func (h *Consumer) WaitUntilWarm(wg *sync.WaitGroup) error {
	for {
		time.Sleep(5 * time.Second)
		h.progressMut.RLock()
		progress := h.progress
		h.progressMut.RUnlock()
		if progress >= h.warmThreshold {
			logger.Logf("HTTP consumer considered warm (%v meets or exceeds the warmup threshold %v)", progress, h.warmThreshold)
			wg.Done()
			return nil
		}
	}
	return nil
}

// Start starts an HTTP server listening on the configured endpoint, inserting
// messages into Database as they're received.
func (h *Consumer) Start(wg *sync.WaitGroup, db *database.Database) error {
	wg.Add(1)
	h.wg = wg
	go func() {
		http.HandleFunc(h.endpoint+"/progress", func(w http.ResponseWriter, req *http.Request) {
			payload, err := ioutil.ReadAll(req.Body)
			if err != nil {
				logger.Logf("problem reading body :( /progress %s, %s", err, string(payload))
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}

			trimmed := strings.TrimSpace(string(payload))
			progress, err := strconv.ParseFloat(trimmed, 32)
			if err != nil {
				logger.Logf("failed to parse progress value: %v %v", err, string(payload))
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}

			h.progressMut.Lock()
			h.progress = float32(progress)
			h.progressMut.Unlock()
			w.Write([]byte(fmt.Sprintf("OK - progress set to %.2f (threshold: %v)\n", progress, h.warmThreshold)))
		})

		http.HandleFunc(h.endpoint+"/tag", func(w http.ResponseWriter, req *http.Request) {
			payload, err := ioutil.ReadAll(req.Body)
			if err != nil {
				logger.Logf("problem reading body :( /consumer/tag %s, %s", err, string(payload))
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}

			var msg *m.KeyTag
			err = json.Unmarshal(payload, &msg)
			if err != nil {
				logger.Logf("blorg problem unmarshaling /consumer/tag %s, %s", err, string(payload))
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			err = db.InsertTags(msg)
			if err != nil {
				logger.Logf("blorg problem writing data! /consumer/tag %s, %s", err, string(payload))
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
		})

		http.HandleFunc(h.endpoint+"/metric", func(w http.ResponseWriter, req *http.Request) {
			payload, err := ioutil.ReadAll(req.Body)
			if err != nil {
				logger.Logf("problem reading body :( /consumer/metric %s, %s", err, string(payload))
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}

			var msg *m.KeyMetric
			err = json.Unmarshal(payload, &msg)
			if err != nil {
				logger.Logf("blorg problem unmarshaling /consumer/metric %s, %s", err, string(payload))
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			err = db.InsertMetrics(msg)
			if err != nil {
				logger.Logf("blorg problem writing data! /consumer/metric %s, %s", err, string(payload))
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
		})

		http.HandleFunc(h.endpoint+"/custom", func(w http.ResponseWriter, req *http.Request) {
			payload, err := ioutil.ReadAll(req.Body)
			if err != nil {
				logger.Logf("couldn't read the body! /consumer/custom %s, %s", err, string(payload))
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}

			var msg *m.TagMetric
			err = json.Unmarshal(payload, &msg)
			if err != nil {
				logger.Logf("failure to decode! /consumer/custom %s, %s", err, string(payload))
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			err = db.InsertCustom(msg)
			if err != nil {
				logger.Logf("blorg problem writing data! /consumer/custom %s, %s", err, string(payload))
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
		})

		portStr := fmt.Sprintf(":%d", h.port)
		logger.Logf("HTTP consumer Listening on %s\n", portStr)
		logger.Logln(http.ListenAndServe(portStr, nil))
	}()
	return nil
}

// Stop halts the consumer. Note: calling Stop and then later calling Start on the same consumer is undefined.
func (h *Consumer) Stop() error {
	h.wg.Done()
	return nil
}

// Name returns the name of the consumer
func (h *Consumer) Name() string {
	return "httpapi"
}
