package main

import (
	"bytes"
	"context"
	"flag"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/justwatchcom/elasticsearch_exporter/collector"
	"github.com/kylemcc/prom2cloudwatch"
	"github.com/prometheus/client_golang/prometheus"
)

type logWrapper struct {
	log.Logger
}

func (l *logWrapper) Println(v ...interface{}) {
	var buf bytes.Buffer
	for c, i := range v {
		buf.WriteString(fmt.Sprintf("%v", i))
		if c < len(v)-1 {
			buf.WriteByte(' ')
		}
	}
	level.Info(l.Logger).Log("msg", buf.String())
}

func main() {
	var (
		listenAddress      = flag.String("web.listen-address", ":9108", "Address to listen on for web interface and telemetry.")
		metricsPath        = flag.String("web.telemetry-path", "/metrics", "Path under which to expose metrics.")
		esURI              = flag.String("es.uri", "http://localhost:9200", "HTTP API address of an Elasticsearch node.")
		esTimeout          = flag.Duration("es.timeout", 5*time.Second, "Timeout for trying to get stats from Elasticsearch.")
		esAllNodes         = flag.Bool("es.all", false, "Export stats for all nodes in the cluster.")
		esCA               = flag.String("es.ca", "", "Path to PEM file that conains trusted CAs for the Elasticsearch connection.")
		esClientPrivateKey = flag.String("es.client-private-key", "", "Path to PEM file that conains the private key for client auth when connecting to Elasticsearch.")
		esClientCert       = flag.String("es.client-cert", "", "Path to PEM file that conains the corresponding cert for the private key to connect to Elasticsearch.")
		cwBridgeEnabled    = flag.Bool("cloudwatch.enabled", false, "Enable pushing metrics to AWS CloudWatch")
		cwWhitelistPath    = flag.String("cloudwatch.whitelist.path", "", "Path to a whitelist file containing a list of metrics to push to cloudwatch when -cloudwatch.enabled=true")
	)
	flag.Parse()

	logger := log.NewLogfmtLogger(log.NewSyncWriter(os.Stdout))
	logger = log.With(logger,
		"ts", log.DefaultTimestampUTC,
		"caller", log.DefaultCaller,
	)

	esURL, err := url.Parse(*esURI)
	if err != nil {
		level.Error(logger).Log(
			"msg", "failed to parse es.uri",
			"err", err,
		)
		os.Exit(1)
	}

	// returns nil if not provided and falls back to simple TCP.
	tlsConfig := createTLSConfig(*esCA, *esClientCert, *esClientPrivateKey)

	httpClient := &http.Client{
		Timeout: *esTimeout,
		Transport: &http.Transport{
			TLSClientConfig: tlsConfig,
		},
	}

	prometheus.MustRegister(collector.NewClusterHealth(logger, httpClient, esURL))
	prometheus.MustRegister(collector.NewNodes(logger, httpClient, esURL, *esAllNodes))

	http.Handle(*metricsPath, prometheus.Handler())
	http.HandleFunc("/", IndexHandler(*metricsPath))

	level.Info(logger).Log(
		"msg", "starting elasticsearch_exporter",
		"addr", *listenAddress,
	)

	var wg sync.WaitGroup
	ctx, cancel := context.WithCancel(context.Background())

	if *cwBridgeEnabled {
		level.Info(logger).Log("msg", "starting CloudWatch Bridge")
		var whitelist []string
		if *cwWhitelistPath != "" {
			if l, err := loadCloudwatchWhitelist(*cwWhitelistPath); err != nil {
				level.Error(logger).Log("msg", "error initializing CloudWatch Bridge", "err", err)
				os.Exit(1)
			} else {
				whitelist = l
			}
		}
		cwb, err := prom2cloudwatch.NewBridge(&prom2cloudwatch.Config{
			PrometheusNamespace: "elasticsearch",
			CloudWatchNamespace: "ElasticSearch",
			Logger:              &logWrapper{logger},
			Interval:            5 * time.Second,
			WhitelistOnly:       len(whitelist) > 0,
			Whitelist:           whitelist,
		})
		if err != nil {
			level.Error(logger).Log("msg", "error initializing CloudWatch Bridge", "err", err)
			os.Exit(1)
		}

		wg.Add(1)
		go func() {
			defer wg.Done()
			cwb.Run(ctx)
		}()
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := http.ListenAndServe(*listenAddress, nil); err != nil {
			level.Error(logger).Log(
				"msg", "http server quit",
				"err", err,
			)
		}
		cancel()
	}()

	wg.Wait()
}

// IndexHandler returns a http handler with the correct metricsPath
func IndexHandler(metricsPath string) http.HandlerFunc {
	indexHTML := `
<html>
	<head>
		<title>Elasticsearch Exporter</title>
	</head>
	<body>
		<h1>Elasticsearch Exporter</h1>
		<p>
			<a href='%s'>Metrics</a>
		</p>
	</body>
</html>
`
	index := []byte(fmt.Sprintf(strings.TrimSpace(indexHTML), metricsPath))

	return func(w http.ResponseWriter, r *http.Request) {
		w.Write(index)
	}
}

func loadCloudwatchWhitelist(path string) ([]string, error) {
	f, err := ioutil.ReadFile(path)
	if err != nil {
		return nil, err
	}
	f = bytes.TrimSpace(f)
	return strings.Split(string(f), "\n"), nil
}
