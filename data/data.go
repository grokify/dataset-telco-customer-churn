package data

import (
	"bytes"
	_ "embed"

	"github.com/grokify/gocharts/v2/data/table"
)

const (
	ColumnCustomerID     = "customerID"
	ColumnCreatedTime    = "createdTime"
	ColumnTenure         = "tenure"
	ColumnMonthlyCharges = "MonthlyCharges"
	ColumnTotalCharges   = "TotalCharges"

	TableName = "customers"
)

//go:embed WA_Fn-UseC_-Telco-Customer-Churn.csv
var b []byte

func Data() []byte {
	return b
}

func DataTable() *table.Table {
	if t, err := table.ParseReadSeeker(nil, bytes.NewReader(Data())); err != nil {
		panic(err)
	} else {
		return &t
	}
}
