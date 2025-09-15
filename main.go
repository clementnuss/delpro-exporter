package main

import (
	"flag"
	"log"
	"net/http"
	"os"
	"time"

	"github.com/clementnuss/delpro-exporter/internal/exporter"
	_ "github.com/joho/godotenv/autoload"
	"github.com/peterbourgon/ff/v3"
)

func main() {
	// Create a new flag set for ff
	fs := flag.NewFlagSet("delpro-exporter", flag.ExitOnError)

	// Define flags on the custom flag set
	listenAddr := fs.String("listen-address", ":9090", "Address to listen on for web interface and telemetry")
	dbHost := fs.String("db-host", "localhost", "Database host")
	dbPort := fs.String("db-port", "1433", "Database port")
	dbName := fs.String("db-name", "DDM", "Database name")
	dbUser := fs.String("db-user", "sa", "Database user")
	lastOID := fs.Int64("last-oid", 0, "Override last processed OID (if larger than current value)")
	dbTimezone := fs.String("db-timezone", "Europe/Zurich", "Database timezone location for time offset calculations")

	// Parse configuration with ff (supports flags, environment variables, and config file)
	err := ff.Parse(fs, os.Args[1:],
		ff.WithEnvVarPrefix("DELPRO"),
		ff.WithConfigFileFlag("config"),
	)
	if err != nil {
		log.Fatal("Error parsing configuration:", err)
	}

	dbPassword := os.Getenv("SQL_PASSWORD")
	if dbPassword == "" {
		log.Fatal("SQL_PASSWORD environment variable is required")
	}

	// Parse database timezone
	dbLocation, err := time.LoadLocation(*dbTimezone)
	if err != nil {
		log.Fatal("Invalid database timezone:", err)
	}

	delproExporter := exporter.NewDelProExporter(*dbHost, *dbPort, *dbName, *dbUser, dbPassword, dbLocation)
	defer delproExporter.Close()

	// Override last OID if specified and larger than current value
	if *lastOID > 0 {
		delproExporter.SetLastOID(*lastOID)
	}

	go func() {
		for {
			delproExporter.UpdateMetrics()
			time.Sleep(30 * time.Second)
		}
	}()

	http.HandleFunc("/metrics", func(w http.ResponseWriter, r *http.Request) {
		delproExporter.WritePrometheus(w, false)
	})

	http.HandleFunc("/historical-metrics", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
		delproExporter.WriteHistoricalMetrics(r, w)
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
