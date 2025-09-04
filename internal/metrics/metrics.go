package metrics

import (
	"bytes"
	"fmt"
	"io"
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

// CreateMetricsFromRecords creates VictoriaMetrics from milking records
func (e *Exporter) CreateMetricsFromRecords(s *metrics.Set, w io.Writer, records []models.MilkingRecord) {
	if s == nil {
		s = metrics.GetDefaultSet()
	}

	for _, r := range records {
		s.GetOrCreateGauge(r.MetricName(models.MetricMilkYield), nil).Set(r.Yield)
		s.GetOrCreateCounter(r.MetricName(models.MetricMilkSessions)).Inc()
		s.GetOrCreateGauge(r.MetricName(models.MetricConductivity), nil).Set(float64(*r.Conductivity))
		s.GetOrCreateHistogram(r.MetricName(models.MetricMilkingDuration)).Update(float64(*r.Duration))

		if w != nil {
			s.WritePrometheus(NewTimestampWriter(w, r.EndTime))
		}
	}
}

// CreateDeviceUtilizationMetrics creates device utilization metrics
func (e *Exporter) CreateDeviceUtilizationMetrics(utilization map[string]int) {
	for deviceID, sessionCount := range utilization {
		metrics.GetOrCreateGauge(fmt.Sprintf(`%s{milk_device_id="%s"}`, models.MetricDeviceUtilization, deviceID), nil).Set(float64(sessionCount))
	}
}

// WriteHistoricalMetrics writes metrics with timestamps in Prometheus exposition format
func (e *Exporter) WriteHistoricalMetrics(w io.Writer, records []models.MilkingRecord) {
	s := metrics.NewSet()
	e.CreateMetricsFromRecords(s, w, records)
}
