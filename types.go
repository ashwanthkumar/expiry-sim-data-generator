package main

import (
	"database/sql"
	"database/sql/driver"
	"time"

	"gorm.io/gorm"
)

// Enum like types
// Ref - https://stackoverflow.com/a/68637612/11474419
type InstrumentType string

const (
	CE  InstrumentType = "CE"
	PE  InstrumentType = "PE"
	FUT InstrumentType = "FUT"
)

func (ct *InstrumentType) Scan(value interface{}) error {
	*ct = InstrumentType(value.([]byte))
	return nil
}

func (ct InstrumentType) Value() (driver.Value, error) {
	return string(ct), nil
}

// InstrumentType but can also be null when refering to spot instruments
type NullableInstrumentType struct {
	InstrumentType InstrumentType
	Valid          bool
}

func (n *NullableInstrumentType) Scan(value interface{}) error {
	if value == nil {
		n.InstrumentType, n.Valid = "", false
	} else {
		n.InstrumentType, n.Valid = InstrumentType(value.([]byte)), true
	}
	return nil
}

func (n NullableInstrumentType) Value() (driver.Value, error) {
	if !n.Valid {
		return nil, nil
	}

	return string(n.InstrumentType), nil
}

type Instrument struct {
	gorm.Model

	Symbol          string       `gorm:"index"` // this is usually NIFTY22MARFUT or NIFTY2230316500CE (NIFTY-22-3-03-16500-CE)
	Expiry          sql.NullTime `gorm:"index"`
	IsWeeklyExpiry  bool
	IsMonthlyExpiry bool
	IsSpot          bool

	Strike  sql.NullInt32 `gorm:"index"`
	LotSize sql.NullInt32 // this is primarily used only margin calculation since Kite API has it
	// this is NIFTY/BANKNIFTY usually or name of the underlying scrip for which this instrument is a derivative
	Underlying     string         `gorm:"index"`
	InstrumentType InstrumentType `gorm:"index"`
}

type TickData struct {
	gorm.Model

	// FK
	Instrument Instrument `gorm:"constraint:OnUpdate:CASCADE,OnDelete:CASCADE;"`

	Timestamp time.Time `gorm:"index"`
	OI        uint32    // this is set to 0 for spot instruments
	Volume    uint32    // this is set to 0 for spot instruments
	Open      float64
	High      float64
	Low       float64
	Close     float64
}
