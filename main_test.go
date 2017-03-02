package main

import (
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"

	pb2 "github.com/dgryski/carbonzipper/carbonzipperpb"
	pb3 "github.com/dgryski/carbonzipper/carbonzipperpb3"

	m "github.com/kanatohodets/carbonsearch/consumer/message"
	"github.com/kanatohodets/carbonsearch/database"
	"github.com/kanatohodets/carbonsearch/util"
)

func TestFindHandler(t *testing.T) {
	stats := util.InitStats()
	initTimeBuckets(Config.Buckets + 1)
	db := database.New(
		100,
		1000,
		"custom",
		"text",
		map[string][]string{
			"fqdn": []string{"servers"},
		},
		stats,
	)

	populateDb(db)

	mux := createMux(db, stats, "virt.v1.*.")
	pb2FindTest(t, mux)
	pb3FindTest(t, mux)
	jsonFindTest(t, mux)
}

func pb2FindTest(t *testing.T, mux *http.ServeMux) {
	recorder := httptest.NewRecorder()
	req, err := http.NewRequest("GET", "/metrics/find/?query=virt.v1.*.servers-status:live&format=protobuf", nil)
	if err != nil {
		t.Errorf("pb2 test: could not create http request: %v", err)
		return
	}

	name := "virt.v1.*.servers-status:live"
	path := "host.foohost_prod_example_com.cpu.loadavg"
	leaf := true
	mux.ServeHTTP(recorder, req)
	expected := &pb2.GlobResponse{
		Name: &name,
		Matches: []*pb2.GlobMatch{
			&pb2.GlobMatch{
				Path:   &path,
				IsLeaf: &leaf,
			},
		},
	}

	expectedBytes, err := expected.Marshal()
	if err != nil {
		t.Errorf("pb2 test: failed to marshal expected into bytes: %v", err)
	}

	if !reflect.DeepEqual(expectedBytes, recorder.Body.Bytes()) {
		t.Errorf("pb2 test: bad response! expected %q, got %q", expected.String(), string(recorder.Body.Bytes()))
	}
}

func pb3FindTest(t *testing.T, mux *http.ServeMux) {
	recorder := httptest.NewRecorder()
	req, err := http.NewRequest("GET", "/metrics/find/?query=virt.v1.*.servers-status:live&format=protobuf3", nil)
	if err != nil {
		t.Errorf("pb3 test: could not create http request: %v", err)
		return
	}

	mux.ServeHTTP(recorder, req)
	expected := &pb3.GlobResponse{
		Name: "virt.v1.*.servers-status:live",
		Matches: []*pb3.GlobMatch{
			&pb3.GlobMatch{
				Path:   "host.foohost_prod_example_com.cpu.loadavg",
				IsLeaf: true,
			},
		},
	}

	expectedBytes, err := expected.Marshal()
	if err != nil {
		t.Errorf("pb3 test: failed to marshal expected into bytes: %v", err)
	}

	if !reflect.DeepEqual(expectedBytes, recorder.Body.Bytes()) {
		t.Errorf("pb3 test: bad response! expected %q, got %q", expected.String(), string(recorder.Body.Bytes()))
	}
}

func jsonFindTest(t *testing.T, mux *http.ServeMux) {
	recorder := httptest.NewRecorder()
	req, err := http.NewRequest("GET", "/metrics/find/?query=virt.v1.*.servers-status:live&format=json", nil)
	if err != nil {
		t.Errorf("JSON test: could not create http request: %v", err)
		return
	}

	mux.ServeHTTP(recorder, req)
	expected := `{"name":"virt.v1.*.servers-status:live","matches":[{"path":"host.foohost_prod_example_com.cpu.loadavg","isLeaf":true}]}` + "\n"
	if expected != string(recorder.Body.Bytes()) {
		t.Errorf("JSON test: bad response! expected %q, got %q", expected, string(recorder.Body.Bytes()))
	}
}

func populateDb(db *database.Database) {
	metrics := &m.KeyMetric{
		Key:   "fqdn",
		Value: "foohost.prod.example.com",
		Metrics: []string{
			"host.foohost_prod_example_com.cpu.loadavg",
		},
	}
	tags := &m.KeyTag{
		Key:   "fqdn",
		Value: "foohost.prod.example.com",
		Tags: []string{
			"servers-status:live",
		},
	}
	db.InsertMetrics(metrics)
	db.InsertTags(tags)
	db.MaterializeIndexes()
}
