package metrics

import (
	"bytes"
	"fmt"
	"io"
	"log"
	"strings"
	"time"

	"github.com/VictoriaMetrics/metrics"
	"github.com/clementnuss/delpro-exporter/internal/models"
)

// Exporter handles metrics creation and exposition
type Exporter struct{}

// TimestampWriter wraps an io.Writer and adds timestamps to each metric line
type TimestampWriter struct {
	writer    io.Writer
	timestamp time.Time
	buffer    bytes.Buffer
}

// NewTimestampWriter creates a new timestamp writer
func NewTimestampWriter(w io.Writer, t time.Time) *TimestampWriter {
	return &TimestampWriter{
		writer:    w,
		timestamp: t,
	}
}

// Write intercepts writes and adds timestamps to each metric line
func (tw *TimestampWriter) Write(p []byte) (n int, err error) {
	// Accumulate data in buffer
	tw.buffer.Write(p)

	// Process complete lines
	data := tw.buffer.String()
	lines := strings.Split(data, "\n")

	// Keep the last (potentially incomplete) line in buffer
	if len(lines) > 0 && lines[len(lines)-1] != "" {
		tw.buffer.Reset()
		tw.buffer.WriteString(lines[len(lines)-1])
		lines = lines[:len(lines)-1]
	} else {
		tw.buffer.Reset()
	}

	// Write complete lines with timestamps
	timestampMs := tw.timestamp.UnixMilli()
	for _, line := range lines {
		if strings.TrimSpace(line) != "" {
			_, err = fmt.Fprintf(tw.writer, "%s %d\n", line, timestampMs)
			if err != nil {
				return 0, err
			}
		}
	}

	return len(p), nil
}

// Flush writes any remaining buffered data
func (tw *TimestampWriter) Flush() error {
	if tw.buffer.Len() > 0 {
		line := strings.TrimSpace(tw.buffer.String())
		if line != "" {
			timestampMs := tw.timestamp.UnixMilli()
			_, err := fmt.Fprintf(tw.writer, "%s %d\n", line, timestampMs)
			if err != nil {
				return err
			}
		}
		tw.buffer.Reset()
	}
	return nil
}

// NewExporter creates a new metrics exporter instance
func NewExporter() *Exporter {
	return &Exporter{}
}

// InitializeCountersToZero initializes all gauge metrics to 0 for a given animal record
func (e *Exporter) InitializeCountersToZero(r *models.MilkingRecord) {
	// Initialize main gauge metrics to 0
	metrics.GetOrCreateCounter(r.MetricName(models.MetricMilkSessions)).Set(0)
	metrics.GetOrCreateGauge(r.MetricName(models.MetricMilkYieldTotal), nil).Set(0)
	metrics.GetOrCreateGauge(r.MetricName(models.MetricSomaticCellTotal), nil).Set(0)
	// metrics.GetOrCreateHistogram(r.MetricName(models.MetricMilkingDuration)) // not useful as histograms are not printed when empty // TODO: implement solution
}

// CreateMetricsFromRecords creates VictoriaMetrics from milking records
func (e *Exporter) CreateMetricsFromRecords(s *metrics.Set, w io.Writer, records []*models.MilkingRecord) {
	if s == nil {
		s = metrics.GetDefaultSet()
	}

	for _, r := range records {
		if w == nil {
			log.Printf("new record processed: %v", r)
		}
		s.GetOrCreateCounter(r.MetricName(models.MetricMilkSessions)).Inc()

		// Last milk yield with timestamp
		s.GetOrCreateGauge(r.MetricName(models.MetricLastMilkYield), nil).Set(r.Yield)
		s.GetOrCreateGauge(r.MetricName(models.MetricLastYieldTimestamp), nil).Set(float64(r.EndTime.Unix()))
		s.GetOrCreateGauge(r.MetricName(models.MetricMilkYieldTotal), nil).Add(r.Yield)

		s.GetOrCreateGauge(r.MetricName(models.MetricConductivity), nil).Set(float64(*r.Conductivity))

		// Last milking duration with timestamp
		s.GetOrCreateHistogram(r.MetricName(models.MetricMilkingDuration)).Update(float64(*r.Duration))
		s.GetOrCreateGauge(r.MetricName(models.MetricLastMilkingDuration), nil).Set(float64(*r.Duration))
		s.GetOrCreateGauge(r.MetricName(models.MetricLastDurationTimestamp), nil).Set(float64(r.EndTime.Unix()))

		if r.SomaticCellCount != nil {
			s.GetOrCreateGauge(r.MetricName(models.MetricSomaticCellTotal), nil).Add(float64(*r.SomaticCellCount))
			// Last somatic cell count with timestamp
			s.GetOrCreateGauge(r.MetricName(models.MetricLastSomaticCellTotal), nil).Set(float64(*r.SomaticCellCount))
			s.GetOrCreateGauge(r.MetricName(models.MetricLastSCCTimestamp), nil).Set(float64(r.EndTime.Unix()))
		}

		if r.DaysInLactation != nil {
			s.GetOrCreateGauge(r.MetricName(models.MetricDaysInLactation), nil).Set(float64(*r.DaysInLactation))
		}

		for _, teat := range models.GetAffectedTeats(*r.Incomplete) {
			s.GetOrCreateGauge(r.TeatMetricName(models.MetricIncomplete, teat), nil).Inc()
		}
		// Add concatenated teats metric for easier Grafana visualization
		incompleteTeats := models.GetAffectedTeatsString(*r.Incomplete)
		if incompleteTeats != "none" {
			s.GetOrCreateGauge(r.TeatsMetricName(models.MetricIncompleteTeats, incompleteTeats), nil).Inc()
		}

		for _, teat := range models.GetAffectedTeats(*r.Kickoff) {
			s.GetOrCreateGauge(r.TeatMetricName(models.MetricKickoff, teat), nil).Inc()
		}
		// Add concatenated teats metric for easier Grafana visualization
		kickoffTeats := models.GetAffectedTeatsString(*r.Kickoff)
		if kickoffTeats != "none" {
			s.GetOrCreateGauge(r.TeatsMetricName(models.MetricKickoffTeats, kickoffTeats), nil).Inc()
		}

		if w != nil {
			s.WritePrometheus(NewTimestampWriter(w, r.EndTime))
		}
	}
}

// CreateDeviceUtilizationMetrics creates device utilization metrics
func (e *Exporter) CreateDeviceUtilizationMetrics(utilization map[string]int) {
	for deviceID, sessionCount := range utilization {
		metrics.GetOrCreateGauge(fmt.Sprintf("%s{milk_device_id=%q,data_format_version=%q}", models.MetricDeviceUtilization, deviceID, models.DataFormatVersion), nil).Set(float64(sessionCount))
	}
}

// WriteHistoricalMetricsWithInit writes historical metrics with timestamps, initializing zero values first
func (e *Exporter) WriteHistoricalMetricsWithInit(w io.Writer, records []*models.MilkingRecord) {
	// First, write initialization values for unique animals
	e.writeInitializationValues(w, records)

	// Then write the actual historical metrics
	e.WriteHistoricalMetrics(w, records)
}

// writeInitializationValues writes 0 values with timestamps 10min before the first record for each unique animal
func (e *Exporter) writeInitializationValues(w io.Writer, records []*models.MilkingRecord) {
	if len(records) == 0 {
		return
	}

	// Track unique animals to avoid duplicate initializations
	seenAnimals := make(map[string]*models.MilkingRecord)

	// Find the first record for each unique animal
	for _, record := range records {
		key := record.LabelStr()
		if existing, exists := seenAnimals[key]; !exists || record.EndTime.Before(existing.EndTime) {
			seenAnimals[key] = record
		}
	}

	// Write initialization values for each unique animal
	for _, lastRecord := range seenAnimals {
		// Create timestamp 10 minutes before the first record
		initTimestamp := lastRecord.EndTime.Add(-10 * time.Minute)
		timestampMs := initTimestamp.UnixMilli()

		// Write zero values for main metrics
		fmt.Fprintf(w, "%s 0 %d\n", lastRecord.MetricName(models.MetricMilkSessions), timestampMs)
		fmt.Fprintf(w, "%s 0 %d\n", lastRecord.MetricName(models.MetricMilkYieldTotal), timestampMs)
		fmt.Fprintf(w, "%s 0 %d\n", lastRecord.MetricName(models.MetricSomaticCellTotal), timestampMs)

		// Write zero histogram for milking duration
		e.writeZeroHistogram(w, lastRecord.MetricName(models.MetricMilkingDuration), timestampMs)
	}
}

// writeZeroHistogram writes a zero histogram with all necessary components
func (e *Exporter) writeZeroHistogram(w io.Writer, metricName string, timestampMs int64) {
	// Parse metric name to get base name and labels
	name, labels := splitMetricName(metricName)

	// Write histogram _sum metric with 0 value
	fmt.Fprintf(w, "%s_sum%s 0 %d\n", name, labels, timestampMs)

	// Write histogram _count metric with 0 value
	fmt.Fprintf(w, "%s_count%s 0 %d\n", name, labels, timestampMs)
}

// splitMetricName splits a metric name with labels into name and labels parts
func splitMetricName(metricName string) (string, string) {
	// Find the opening brace
	braceIndex := strings.Index(metricName, "{")
	if braceIndex == -1 {
		// No labels
		return metricName, ""
	}

	name := metricName[:braceIndex]
	labels := metricName[braceIndex:] // Includes the braces
	return name, labels
}

// WriteHistoricalMetrics writes metrics with timestamps in Prometheus exposition format
// Uses one metric set per animal to avoid duplicate data when no changes occur
func (e *Exporter) WriteHistoricalMetrics(w io.Writer, records []*models.MilkingRecord) {
	// Group records by animal registration number
	animalRecords := make(map[string][]*models.MilkingRecord)
	for _, record := range records {
		animalRecords[record.AnimalRegNo] = append(animalRecords[record.AnimalRegNo], record)
	}

	// Process each animal's records separately
	for _, animalData := range animalRecords {
		s := metrics.NewSet()
		e.CreateMetricsFromRecords(s, w, animalData)
	}
}
