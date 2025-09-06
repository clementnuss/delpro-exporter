package models

import (
	"fmt"
	"time"
)

// Teat represents a cow teat position using bitfield values
type Teat int

const (
	LeftFront  Teat = 1 << iota // 1
	RightFront                  // 2
	LeftRear                    // 4
	RightRear                   // 8
)

// String returns the string representation of the teat
func (t Teat) String() string {
	switch t {
	case LeftFront:
		return "left_front"
	case RightFront:
		return "right_front"
	case LeftRear:
		return "left_rear"
	case RightRear:
		return "right_rear"
	default:
		return "unknown"
	}
}

const (
	// Metric names
	MetricMilkYieldTotal    = "delpro_milk_yield_liters_total"
	MetricLastMilkYield     = "delpro_milk_last_yield_liters"
	MetricMilkSessions      = "delpro_milk_sessions_total"
	MetricConductivity      = "delpro_milk_conductivity_mScm"
	MetricSomaticCellCount  = "delpro_milk_somatic_cell_count"
	MetricMilkingDuration   = "delpro_milking_duration_seconds"
	MetricDeviceUtilization = "delpro_device_utilization_sessions_per_hour"
	MetricIncomplete        = "delpro_milking_incomplete_teat"
	MetricKickoff           = "delpro_milking_kickoff_teat"

	// Query parameters
	DefaultLookbackWindow   = 24 * time.Hour
	HistoricalLookbackHours = 365 * 24 * time.Hour
)

// MilkingRecord represents a single milking session from the database
type MilkingRecord struct {
	OID              int64     // Database OID for tracking processed records
	AnimalNumber     string    // Farm animal number
	AnimalName       string    // Animal name
	AnimalRegNo      string    // Official registration number
	BreedName        string    // Breed name (translated to French)
	DeviceID         string    // Milking device identifier
	Yield            float64   // Milk yield in liters
	Conductivity     *int      // Milk conductivity [mS/cm] (optional)
	Duration         *int      // Session duration in seconds (optional)
	SomaticCellCount *int      // Somatic cell count [cells/ml] (optional)
	Incomplete       *int      // Incomplete milking flag (optional)
	Kickoff          *int      // Kickoff event flag (optional)
	BeginTime        time.Time // Session start time
	EndTime          time.Time // Session end time
}

// LabelStr returns formatted Prometheus labels for the record
func (r *MilkingRecord) LabelStr() string {
	return fmt.Sprintf(`animal_number="%s",animal_name="%s",animal_reg_no="%s",breed="%s",milk_device_id="%s"`,
		r.AnimalNumber, r.AnimalName, r.AnimalRegNo, r.BreedName, r.DeviceID)
}

// TeatLabelStr returns formatted Prometheus labels for teat-specific metrics
func (r *MilkingRecord) TeatLabelStr(teat string) string {
	return fmt.Sprintf(`%s,teat="%s"`, r.LabelStr(), teat)
}

// TeatMetricName returns a fully qualified teat metric name with labels
func (r *MilkingRecord) TeatMetricName(metric, teat string) string {
	return fmt.Sprintf("%s{%s}", metric, r.TeatLabelStr(teat))
}

// GetAffectedTeats returns a slice of teat names based on bitfield value
func GetAffectedTeats(bitfield int) []string {
	var teats []string
	allTeats := []Teat{LeftFront, RightFront, LeftRear, RightRear}

	for _, teat := range allTeats {
		if bitfield&int(teat) != 0 {
			teats = append(teats, teat.String())
		}
	}
	return teats
}

// MetricName returns a fully qualified metric name with labels
func (r *MilkingRecord) MetricName(metric string) string {
	return fmt.Sprintf("%s{%s}", metric, r.LabelStr())
}
