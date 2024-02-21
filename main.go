package main

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/charmbracelet/log"
	"github.com/spf13/pflag"
	"golang.org/x/sync/errgroup"
)

type dumper struct {
	baseURL       string
	target        string
	size          int
	slices        int
	scrollTimeout time.Duration
	httpTimeout   time.Duration
	noCompression bool
	fields        string
	queryString   string
	metadata      bool
	metadataOnly  bool
	throttle      float32
	count         uint64
	random        bool
	verify        string

	query           obj
	out             *bufio.Writer
	scrollTimeoutES string
	cl              httpClient
	start           time.Time
	scrolled        uint64
	dumped          uint64
	scrolledCh      chan json.RawMessage

	totalHitsPending int32
	totalHits        uint64
}

func main() {
	var d dumper
	log.SetTimeFormat("2006-01-02 15:04:05.000")

	flags := pflag.NewFlagSet(os.Args[0], pflag.ContinueOnError)

	usage := func() {
		fmt.Fprint(os.Stderr, `esdump base-url index-target [flags]

Dumps an Elasticsearch index in JSONL (JSON lines) format to standard output.

By default, all documents of the index are dumped. To filter the documents to
dump, you can either:

  - write a query (in the JSON format suitable for the /_search Elasticsearch
	endpoint) to the standard input
  - specify a query in the "query string" format to the -q/--query flag

To avoid stressing the server too much, throttling of the scroll requests is
applied by default. To go faster, turn off throttling with -t0

Arguments:

  base-url      The base URL of the Elasticsearch server (e.g. http://localhost)
                If the port is not specified, 9200 is assumed
  index-target  The name of the index you want to dump. Multi-target syntax is
                also supported (e.g. myindex1,myindex2 or myindex*)

Examples:

  esdump http://localhost myindex > out.jsonl
  esdump http://localhost myindex1,myindex2*
  esdump http://localhost myindex --fields id,date,description
  esdump http://localhost myindex --query "rabbit OR bunny"
  echo '{"query": {"term": {"animal": "rabbit"}}}' | esdump http://localhost myindex
  esdump http://localhost myindex --random --count 1000 > random_sample_1k.jsonl
  esdump https://user:pass@localhost myindex --verify=cacert.pem

Flags:

`)

		flags.PrintDefaults()
	}
	flags.Usage = usage

	flags.StringVarP(&d.fields,
		"fields", "f", "", "comma-separated list of fields to include in the output, or if starting with ^ to exclude")
	flags.StringVarP(&d.queryString,
		"query", "q", "", "filter the documents with a \"query_string\" query")
	flags.Float32VarP(&d.throttle,
		"throttle", "t", 4, "delay factor for adaptive throttling, set 0 to disable throttling")
	flags.Uint64VarP(&d.count,
		"count", "n", 0, "output that many documents maximum (default unlimited)")
	flags.IntVarP(&d.size,
		"scroll-size", "s", 1000, "number of hits per scroll request")
	flags.BoolVarP(&d.metadata,
		"metadata", "m", false, "include hit metadata (_index, _id, _source...), if not set only outputs the contents of _source")
	flags.BoolVarP(&d.metadataOnly,
		"metadata-only", "M", false, "only include hit metadata (_index, _id...), no _source")
	flags.BoolVarP(&d.random,
		"random", "r", false, "dump the documents in a random order")
	flags.BoolVarP(&d.noCompression,
		"no-compression", "z", false, "disable HTTP gzip compression")
	flags.StringVar(&d.verify,
		"verify", "", "certificate file to verify the server's certificate, or \"no\" to skip all TLS verification")
	flags.IntVar(&d.slices,
		"slices", 10, "max number of slices per index")
	flags.DurationVar(&d.scrollTimeout,
		"scroll-timeout", time.Minute, "scroll timeout")
	flags.DurationVar(&d.httpTimeout,
		"http-timeout", time.Minute, "HTTP client timeout")

	flags.SortFlags = false
	flags.Usage = usage

	err := flags.Parse(os.Args[1:])
	if errors.Is(err, pflag.ErrHelp) {
		os.Exit(0)
	}
	if err != nil {
		log.Error("parsing flags", "err", err)
		usage()
		os.Exit(1)
	}

	args := flags.Args()
	if len(args) != 2 {
		log.Error("exactly two arguments expected")
		usage()
		os.Exit(1)
	}

	esURL, err := url.Parse(args[0])
	if err != nil || esURL.Scheme == "" {
		log.Error("first argument must be an URL")
		usage()
		os.Exit(1)
	}

	d.validateFlags(usage)

	if esURL.Port() == "" {
		esURL.Host = fmt.Sprintf("%s:9200", esURL.Host)
	}
	if isLoopback(esURL.Hostname()) {
		log.Info("detected loopback address, disabling compression")
		d.noCompression = true
	}
	d.baseURL = esURL.String()
	d.target = args[1]

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	d.dump(ctx)
}

func (d *dumper) validateFlags(usage func()) {
	var errs []string
	if d.throttle < 0 {
		errs = append(errs, "throttle must be >= 0")
	}
	if d.size < 1 {
		errs = append(errs, "scroll-size must be >= 1")
	}
	if d.slices < 1 {
		errs = append(errs, "slices must be >= 1")
	}
	if d.scrollTimeout < 0 {
		errs = append(errs, "scroll-timeout must be >= 0")
	}
	if d.httpTimeout < 0 {
		errs = append(errs, "http-timeout must be >= 0")
	}
	if d.metadataOnly && d.fields != "" {
		errs = append(errs, "metadata-only and fields are mutually exclusive")
	}
	if len(errs) > 0 {
		for _, err := range errs {
			log.Error(err)
		}
		usage()
		os.Exit(1)
	}
}

func isLoopback(host string) bool {
	ips, err := net.LookupIP(host)
	if err != nil || len(ips) == 0 {
		return false
	}
	for _, ip := range ips {
		if !ip.IsLoopback() {
			return false
		}
	}
	return true
}

type indexShardsResp map[string]struct {
	Settings struct {
		Index struct {
			NumberOfShards string `json:"number_of_shards"`
		} `json:"index"`
	} `json:"settings"`
}

func (d *dumper) getIndexShards(ctx context.Context) map[string]int {
	status, respJSON, err := d.cl.Get(ctx, d.target+"/_settings", "")
	if err != nil {
		log.Fatal("unable to get index settings, are you sure the URL is correct?", "err", err)
	}
	if status == http.StatusNotFound {
		log.Fatal("index target not found, are you sure the URL & target are correct?")
	}
	if status != http.StatusOK {
		log.Fatal("got unexpected status code, are you sure the URL is correct?", "code", status)
	}

	var resp indexShardsResp
	err = json.Unmarshal(respJSON, &resp)
	if err != nil {
		log.Fatal("parsing index settings response", "err", err)
	}

	indexShards := make(map[string]int)

	for idxName, idxSettings := range resp {
		shards, err := strconv.Atoi(idxSettings.Settings.Index.NumberOfShards)
		if err != nil {
			log.Fatal("retrieving number of shards from settings", "err", err)
		}
		if shards <= 0 {
			log.Fatal("invalid number of shards", "shards", shards)
		}
		indexShards[idxName] = shards
	}

	return indexShards
}

func (d *dumper) formatScrollTimeoutES() string {
	// ES doesn't support multiple units like Go time does (e.g. 1h2m3s)
	// It also doesn't support decimal values (e.g. 1.2s)
	// So let's truncate to a whole number of seconds, or milliseconds for small
	// durations
	if d.scrollTimeout < 10*time.Second {
		return fmt.Sprintf("%dms", int(d.scrollTimeout.Milliseconds()))
	}
	return fmt.Sprintf("%ds", int(d.scrollTimeout.Seconds()))
}

func (d *dumper) init() {
	if !strings.HasSuffix(d.baseURL, "/") {
		d.baseURL += "/"
	}
	d.initHTTPClient()
	d.out = bufio.NewWriter(os.Stdout)
	d.scrollTimeoutES = d.formatScrollTimeoutES()
	d.scrolledCh = make(chan json.RawMessage, d.size)
}

func (d *dumper) initScrollers(indexShards map[string]int) []func(context.Context) error {
	var scrollers []func(context.Context) error
	for idxName, shards := range indexShards {
		idxName := idxName
		shards := shards

		slices := d.slices
		if slices > shards {
			slices = shards
		}

		log.Info("dumping", "index", idxName, "shards", shards, "slices", slices)
		for i := 0; i < slices; i++ {
			i := i

			scrollers = append(scrollers, func(ctx context.Context) error {
				return d.scrollSlice(ctx, idxName, i, slices)
			})
		}
	}

	d.totalHitsPending = int32(len(scrollers))

	return scrollers
}

func (d *dumper) dump(ctx context.Context) {
	d.init()
	d.createQuery()

	b, _ := json.MarshalIndent(d.query, "", "    ")
	log.Info("scroll query:")
	fmt.Fprintln(os.Stderr, string(b))

	log.Info("scroll parameters", "timeout", d.scrollTimeoutES, "size", d.size, "throttle", d.throttle)

	indexShards := d.getIndexShards(ctx)

	d.start = time.Now()

	scrollers := d.initScrollers(indexShards)

	workers, ctx := errgroup.WithContext(ctx)
	workers.Go(func() error {
		defer close(d.scrolledCh)
		return d.scroll(ctx, scrollers)
	})
	workers.Go(func() error {
		return d.write(ctx)
	})

	stopDumpStatus := d.dumpStatus()
	err := workers.Wait()
	d.out.Flush()
	stopDumpStatus()

	took := time.Since(d.start)
	speed := float64(d.dumped) / took.Seconds()
	stats := []any{
		"took", took.Round(time.Millisecond),
		"dumped", d.dumped,
		"speed", fmt.Sprintf("%.2f docs/sec", speed),
	}

	if err != nil {
		if errors.Is(err, context.Canceled) {
			log.Warn("dump canceled before completion", stats...)
		} else {
			log.Error("dump failed", append(stats, "err", err)...)
		}
	} else {
		log.Info("dump complete", stats...)
	}
}

func (d *dumper) dumpStatus() func() {
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})

	go func() {
		ticker := time.NewTicker(time.Second)
		for {
			select {
			case <-ctx.Done():
				close(done)
				return
			case <-ticker.C:
				dumped := atomic.LoadUint64(&d.dumped)

				stats := []any{"dumped", dumped}
				if atomic.LoadInt32(&d.totalHitsPending) == 0 {
					totalHits := atomic.LoadUint64(&d.totalHits)

					toDump := totalHits
					if d.count > 0 && toDump >= d.count {
						toDump = d.count
					}

					progress := float64(dumped) / float64(toDump)
					stats = append(stats,
						"total_hits", totalHits,
						"progress", fmt.Sprintf("%.2f%%", progress*100),
					)

				}
				log.Info("dumping...", stats...)
			}
		}
	}()

	return func() {
		cancel()
		<-done
	}
}
