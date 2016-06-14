package httpapi

import (
	"encoding/json"
	"fmt"
	m "github.com/kanatohodets/carbonsearch/consumer/message"
	"github.com/kanatohodets/carbonsearch/database"
	"github.com/kanatohodets/carbonsearch/util"
	"io/ioutil"
	"log"
	"net/http"
	"sync"
)

type HTTPConfig struct {
	Port     int    `yaml:"port"`
	Endpoint string `yaml:"endpoint"`
}

type HTTPConsumer struct {
	port     int
	endpoint string
}

func New(configPath string) (*HTTPConsumer, error) {
	config := &HTTPConfig{}
	err := util.ReadConfig(configPath, config)
	if err != nil {
		return nil, err
	}

	return &HTTPConsumer{
		port:     config.Port,
		endpoint: config.Endpoint,
	}, nil
}

func (h *HTTPConsumer) Start(wg *sync.WaitGroup, db *database.Database) error {
	go func() {
		http.HandleFunc(h.endpoint+"/tag", func(w http.ResponseWriter, req *http.Request) {
			payload, err := ioutil.ReadAll(req.Body)
			if err != nil {
				log.Printf("problem reading body :( /consumer/tag %s, %s", err, string(payload))
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}

			var msg *m.KeyTag
			if err := json.Unmarshal(payload, &msg); err != nil {
				log.Printf("blorg problem unmarshaling /consumer/tag %s, %s", err, string(payload))
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			err = db.InsertTags(msg)
			if err != nil {
				log.Printf("blorg problem writing data! /consumer/tag %s, %s", err, string(payload))
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
		})

		http.HandleFunc(h.endpoint+"/metric", func(w http.ResponseWriter, req *http.Request) {
			payload, err := ioutil.ReadAll(req.Body)
			if err != nil {
				log.Printf("problem reading body :( /consumer/metric %s, %s", err, string(payload))
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}

			var msg *m.KeyMetric
			if err := json.Unmarshal(payload, &msg); err != nil {
				log.Printf("blorg problem unmarshaling /consumer/metric %s, %s", err, string(payload))
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			err = db.InsertMetrics(msg)
			if err != nil {
				log.Printf("blorg problem writing data! /consumer/metric %s, %s", err, string(payload))
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
		})

		http.HandleFunc(h.endpoint+"/custom", func(w http.ResponseWriter, req *http.Request) {
			payload, err := ioutil.ReadAll(req.Body)
			if err != nil {
				log.Printf("couldn't read the body! /consumer/custom %s, %s", err, string(payload))
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}

			var msg *m.TagMetric
			if err := json.Unmarshal(payload, &msg); err != nil {
				log.Printf("failure to decode! /consumer/custom %s, %s", err, string(payload))
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			log.Printf("got this msg %v", msg)
			err = db.InsertCustom(msg)
			if err != nil {
				log.Printf("blorg problem writing data! /consumer/custom %s, %s", err, string(payload))
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
		})

		portStr := fmt.Sprintf(":%d", h.port)
		log.Printf("HTTP consumer Listening on %s\n", portStr)
		log.Println(http.ListenAndServe(portStr, nil))
	}()
	return nil
}

func (h *HTTPConsumer) Stop() error {
	return nil
}

func (h *HTTPConsumer) Name() string {
	return "httpapi"
}
