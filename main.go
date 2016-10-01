package main

// handle virt. namespace metric requests from carbon zipper

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	_ "net/http/pprof"
	"net/url"
	"os"
	"os/signal"
	"runtime"
	"runtime/pprof"
	"strings"
	"sync"

	pb "github.com/dgryski/carbonzipper/carbonzipperpb"

	"github.com/kanatohodets/carbonsearch/consumer"
	"github.com/kanatohodets/carbonsearch/consumer/httpapi"
	"github.com/kanatohodets/carbonsearch/consumer/kafka"
	"github.com/kanatohodets/carbonsearch/database"
	"github.com/kanatohodets/carbonsearch/tag"
	"github.com/kanatohodets/carbonsearch/util"
)

// BuildVersion is provided to be overridden at build time. Eg. go build -ldflags -X 'main.BuildVersion=...'
var BuildVersion = "(development build)"

var db *database.Database

var stats *util.Stats

const virtPrefix = "virt.v1."

// TODO(btyler) convert tags to byte slices right away so hash functions don't need casting
func parseTarget(target string) (map[string][]string, error) {
	/*
		parse something like this:
			'virt.v1.server-state:live.server-hw:intel.lb-pool:www'
		into a map of 'tags' like this:
			{
				"server": [ "server-state:live", "server-hw:intel"],
				"lb": ["lb-pool:www"]
			}

		where a 'tag' is a complete "prefix-key:value" item, such as "server-state:live".

		these will be used to search the "left" side of our indexes: tag -> [$join_key, $join_key...]
	*/

	validExp := strings.HasPrefix(target, virtPrefix)
	if !validExp {
		return nil, fmt.Errorf("main: one of the targets is not a valid virtual metric (must start with %q): %s", virtPrefix, target)
	}

	raw := strings.TrimPrefix(target, virtPrefix)
	//NOTE(btyler) v1 only supports (implicit) 'and': otherwise we need precedence rules and...yuck
	// additionally, you can get 'or' by adding more metrics to your query
	tags := strings.Split(raw, ".")

	tagsByService := make(map[string][]string)
	for _, queryTag := range tags {
		service, _, err := tag.Parse(queryTag)
		if err != nil {
			return nil, err
		}

		stats.QueryTagsByService.Add(service, 1)

		_, ok := tagsByService[service]
		if !ok {
			tagsByService[service] = []string{}
		}

		tagsByService[service] = append(tagsByService[service], queryTag)
	}
	return tagsByService, nil
}

func findHandler(w http.ResponseWriter, req *http.Request) {
	uri, _ := url.ParseRequestURI(req.URL.RequestURI())
	uriQuery := uri.Query()

	stats.QueriesHandled.Add(1)

	targets := uriQuery["target"]
	if len(targets) != 1 {
		http.Error(w, fmt.Sprintf("main: there must be exactly one 'target' url param"), http.StatusBadRequest)
	}

	target := targets[0]
	queryTags, err := parseTarget(target)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	formats := uriQuery["format"]
	if len(formats) != 1 {
		http.Error(w, fmt.Sprintf("main: there must be exactly one 'format' url param"), http.StatusBadRequest)
		return
	}

	format := formats[0]
	if format != "protobuf" && format != "json" {
		http.Error(
			w,
			fmt.Sprintf("main: %q is not a recognized format: known formats are 'protobuf' and 'json'", format),
			http.StatusBadRequest,
		)
		return
	}

	matches, err := db.Query(queryTags)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	var result pb.GlobResponse
	result.Name = &target
	result.Matches = matches

	if format == "protobuf" {
		w.Header().Set("Content-Type", "application/x-protobuf")
		b, _ := result.Marshal()
		w.Write(b)
	} else if format == "json" {
		w.Header().Set("Content-Type", "application/json")
		enc := json.NewEncoder(w)
		err = enc.Encode(result)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
	}
}

func main() {
	configPath := flag.String("config", "config.yaml", "Path to the `config file`.")
	blockingProfile := flag.String("blockProfile", "", "Path to `block profile output file`. Block profiler disabled if empty.")
	cpuProfile := flag.String("cpuProfile", "", "Path to `cpu profile output file`. CPU profiler disabled if empty.")
	flag.Parse()

	if *configPath == "" {
		printUsageErrorAndExit("Can't run without a config file")
	}

	if *blockingProfile != "" {
		f, err := os.Create(*blockingProfile)
		if err != nil {
			log.Fatal(err.Error())
		}
		runtime.SetBlockProfileRate(1)
		defer f.Close()
		defer pprof.Lookup("block").WriteTo(f, 1)
	}

	if *cpuProfile != "" {
		f, err := os.Create(*cpuProfile)
		if err != nil {
			log.Fatal(err)
		}
		pprof.StartCPUProfile(f)
		defer pprof.StopCPUProfile()
	}

	type Config struct {
		Port       int               `yaml:"port"`
		QueryLimit int               `yaml:"query_limit"`
		Consumers  map[string]string `yaml:"consumers"`
	}

	conf := &Config{}
	err := util.ReadConfig(*configPath, conf)
	if err != nil {
		printErrorAndExit(1, "could not read config: %s", err)
	}

	if len(conf.Consumers) == 0 {
		printErrorAndExit(1, "config doesn't have any consumers. carbonsearch won't have anything to search on. Take a peek in %q, see if it looks like it should", *configPath)
	}

	stats = util.InitStats()

	wg := &sync.WaitGroup{}
	db = database.New(conf.QueryLimit, stats)
	quit := make(chan bool)

	constructors := map[string]func(string) (consumer.Consumer, error){
		"kafka": func(confPath string) (consumer.Consumer, error) {
			c, err := kafka.New(confPath)
			return c, err
		},
		"httpapi": func(confPath string) (consumer.Consumer, error) {
			c, err := httpapi.New(confPath)
			return c, err
		},
	}

	consumers := []consumer.Consumer{}
	for consumerName, consumerConfigPath := range conf.Consumers {
		constructor, ok := constructors[consumerName]
		if !ok {
			printErrorAndExit(1, "carbonsearch doesn't know how to create consumer %q. talk to the authors, or remove %q from the list of consumers in %q", consumerName, consumerName, *configPath)
		}
		consumer, err := constructor(consumerConfigPath)
		if err != nil {
			printErrorAndExit(1, "could not create new %s consumer: %s", consumerName, err)
		}

		err = consumer.Start(wg, db)
		if err != nil {
			printErrorAndExit(1, "could not start %s consumer: %s", consumerName, err)
		}

		consumers = append(consumers, consumer)
	}

	go func() {
		http.HandleFunc("/metrics/find", findHandler)
		portStr := fmt.Sprintf(":%d", conf.Port)
		log.Println("Starting carbonsearch", BuildVersion)
		log.Printf("listening on %s\n", portStr)
		log.Println(http.ListenAndServe(portStr, nil))
	}()

	go func() {
		signals := make(chan os.Signal, 1)
		signal.Notify(signals, os.Kill, os.Interrupt)
		<-signals
		log.Println("Shutting down...")
		for _, consumer := range consumers {
			err := consumer.Stop()
			if err != nil {
				log.Printf("Failed to close consumer %s: %s", consumer.Name(), err)
			}
		}

		quit <- true
	}()

	wg.Wait()
}

func printErrorAndExit(code int, format string, values ...interface{}) {
	fmt.Fprintf(os.Stderr, "ERROR: %s\n", fmt.Sprintf(format, values...))
	fmt.Fprintln(os.Stderr)
	os.Exit(code)
}

func printUsageErrorAndExit(format string, values ...interface{}) {
	fmt.Fprintf(os.Stderr, "ERROR: %s\n", fmt.Sprintf(format, values...))
	fmt.Fprintln(os.Stderr)
	fmt.Fprintln(os.Stderr, "Available command line options:")
	flag.PrintDefaults()
	os.Exit(64)
}
