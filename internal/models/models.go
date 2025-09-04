package models

import (
	"fmt"
	"time"
)

const (
	// Metric names
	MetricMilkYield         = "delpro_milk_yield_liters"
	MetricMilkSessions      = "delpro_milk_sessions_total"
	MetricConductivity      = "delpro_milk_conductivity_avg"
	MetricMilkingDuration   = "delpro_milking_duration_seconds"
	MetricDeviceUtilization = "delpro_device_utilization_sessions_per_hour"

	// Query parameters
	DefaultLookbackWindow   = 24 * time.Hour
	HistoricalLookbackHours = 365 * 24 * time.Hour
)

// MilkingRecord represents a single milking session from the database
type MilkingRecord struct {
	AnimalNumber string    // Farm animal number
	AnimalName   string    // Animal name
	AnimalRegNo  string    // Official registration number
	BreedName    string    // Breed name (translated to French)
	DeviceID     string    // Milking device identifier
	Yield        float64   // Milk yield in liters
	Conductivity *int      // Milk conductivity (optional)
	Duration     *int      // Session duration in seconds (optional)
	BeginTime    time.Time // Session start time
	EndTime      time.Time // Session end time
}

// LabelStr returns formatted Prometheus labels for the record
func (r *MilkingRecord) LabelStr() string {
	return fmt.Sprintf(`animal_number="%s",animal_name="%s",animal_reg_no="%s",breed="%s",milk_device_id="%s"`,
		r.AnimalNumber, r.AnimalName, r.AnimalRegNo, r.BreedName, r.DeviceID)
}

// MetricName returns a fully qualified metric name with labels
func (r *MilkingRecord) MetricName(metric string) string {
	return fmt.Sprintf("%s{%s}", metric, r.LabelStr())
}

