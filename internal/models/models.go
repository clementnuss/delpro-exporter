package models

import (
	"fmt"
	"strings"
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
		return "AvG"
	case RightFront:
		return "AvD"
	case LeftRear:
		return "ArG"
	case RightRear:
		return "ArD"
	default:
		return "unknown"
	}
}

const (
	// Metric names
	MetricMilkSessions          = "delpro_milk_sessions_total"
	MetricMilkYieldTotal        = "delpro_milk_yield_liters_total"
	MetricLastMilkYield         = "delpro_milk_last_yield_liters"
	MetricLastYieldTimestamp    = "delpro_milk_last_yield_timestamp"
	MetricConductivity          = "delpro_milk_conductivity_mScm"
	MetricSomaticCellTotal      = "delpro_milk_somatic_cell_total"
	MetricLastSomaticCellTotal  = "delpro_milk_last_somatic_cell"
	MetricLastSCCTimestamp      = "delpro_milk_last_somatic_cell_timestamp"
	MetricMilkingDuration       = "delpro_milking_duration_seconds"
	MetricLastMilkingDuration   = "delpro_last_milking_duration_seconds"
	MetricLastDurationTimestamp = "delpro_last_milking_duration_timestamp"
	MetricIncomplete            = "delpro_milking_incomplete_teat"
	MetricKickoff               = "delpro_milking_kickoff_teat"
	MetricIncompleteTeats       = "delpro_milking_incomplete_teats"
	MetricKickoffTeats          = "delpro_milking_kickoff_teats"
	MetricDaysInLactation       = "delpro_animal_days_in_lactation"
	MetricDeviceUtilization     = "delpro_device_utilization_sessions_per_hour"

	// Query parameters
	DefaultLookbackWindow   = 24 * time.Hour
	HistoricalLookbackHours = 30 * 24 * time.Hour
)

// MilkingRecord represents a single milking session from the database
type MilkingRecord struct {
	OID              int64     // Database OID for tracking processed records
	AnimalNumber     string    // Farm animal number
	AnimalName       string    // Animal name
	AnimalRegNo      string    // Official registration number
	BreedName        string    // Breed name (translated to French)
	DeviceID         string    // Milking device identifier
	DestinationName  string    // Milk destination name (Tank, Drain, etc.)
	LactationNumber  *int      // Current lactation number (optional)
	DaysInLactation  *int      // Days since lactation start (optional)
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
	lactationNum := "unknown"
	if r.LactationNumber != nil {
		lactationNum = fmt.Sprintf("%d", *r.LactationNumber)
	}
	return fmt.Sprintf(`animal_number="%s",animal_name="%s",animal_reg_no="%s",breed="%s",milk_device_id="%s",destination="%s",lactation="%s"`,
		r.AnimalNumber, r.AnimalName, r.AnimalRegNo, r.BreedName, r.DeviceID, r.DestinationName, lactationNum)
}

// TeatLabelStr returns formatted Prometheus labels for teat-specific metrics
func (r *MilkingRecord) TeatLabelStr(teat string) string {
	return fmt.Sprintf(`%s,teat="%s"`, r.LabelStr(), teat)
}

// TeatsLabelStr returns formatted Prometheus labels for concatenated teats metrics
func (r *MilkingRecord) TeatsLabelStr(teats string) string {
	return fmt.Sprintf(`%s,teats="%s"`, r.LabelStr(), teats)
}

// TeatMetricName returns a fully qualified teat metric name with labels
func (r *MilkingRecord) TeatMetricName(metric, teat string) string {
	return fmt.Sprintf("%s{%s}", metric, r.TeatLabelStr(teat))
}

// TeatsMetricName returns a fully qualified concatenated teats metric name with labels
func (r *MilkingRecord) TeatsMetricName(metric, teats string) string {
	return fmt.Sprintf("%s{%s}", metric, r.TeatsLabelStr(teats))
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

// GetAffectedTeatsString returns a comma-separated string of affected teat names
func GetAffectedTeatsString(bitfield int) string {
	teats := GetAffectedTeats(bitfield)
	if len(teats) == 0 {
		return "none"
	}
	return strings.Join(teats, ",")
}

// MetricName returns a fully qualified metric name with labels
func (r *MilkingRecord) MetricName(metric string) string {
	return fmt.Sprintf("%s{%s}", metric, r.LabelStr())
}
