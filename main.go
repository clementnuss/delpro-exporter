package main

import (
	"flag"
	"log"
	"net/http"
	"os"
	"time"

	"github.com/VictoriaMetrics/metrics"
	_ "github.com/joho/godotenv/autoload"
)

var (
	listenAddr = flag.String("web.listen-address", ":9090", "Address to listen on for web interface and telemetry")
	dbHost     = flag.String("db.host", "localhost", "Database host")
	dbPort     = flag.String("db.port", "1433", "Database port")
	dbName     = flag.String("db.name", "DDM", "Database name")
	dbUser     = flag.String("db.user", "sa", "Database user")
)

func main() {
	flag.Parse()

	dbPassword := os.Getenv("SQL_PASSWORD")
	if dbPassword == "" {
		log.Fatal("SQL_PASSWORD environment variable is required")
	}

	exporter := NewDelProExporter(*dbHost, *dbPort, *dbName, *dbUser, dbPassword)

	go func() {
		for {
			exporter.UpdateMetrics()
			time.Sleep(30 * time.Second)
		}
	}()

	http.HandleFunc("/metrics", func(w http.ResponseWriter, r *http.Request) {
		metrics.WritePrometheus(w, false)
	})

	http.HandleFunc("/historical-metrics", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
		exporter.WriteHistoricalMetrics(w)
	})

	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(`<html>
			<head><title>DelPro Exporter</title></head>
			<body>
			<h1>DelPro Exporter</h1>
			<p><a href="/metrics">Current Metrics</a></p>
			<p><a href="/historical-metrics">Historical Metrics with Timestamps</a></p>
			</body>
			</html>`))
	})

	log.Printf("Starting DelPro exporter on %s", *listenAddr)
	log.Fatal(http.ListenAndServe(*listenAddr, nil))
}
