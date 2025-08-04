package mysql

import (
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/grokify/mogo/database/datasource"
	"github.com/grokify/mogo/database/sqlutil"
	"github.com/grokify/mogo/errors/errorsutil"
	"github.com/grokify/mogo/strconv/strconvutil"
	"github.com/grokify/mogo/time/timeutil"
	"github.com/jmoiron/sqlx"

	"github.com/grokify/dataset-telco-customer-churn/data"
)

func SQLCreateTableCustomers() string {
	return `CREATE TABLE customers (
	customerID varchar(255) NOT NULL,
	Churn ENUM("No","Yes"),
	Contract ENUM("Month-to-month","One year","Two year"),
	Dependents ENUM("No","Yes"),
	DeviceProtection ENUM("No","No internet service","Yes"),
	InternetService ENUM("DSL","Fiber optic","No"),
	MultipleLines ENUM("No","No phone service","Yes"),
	OnlineBackup ENUM("No","No internet service","Yes"),
	OnlineSecurity ENUM("No","No internet service","Yes"),
	PaperlessBilling ENUM("No","Yes"),
	Partner ENUM("No","Yes"),
	PaymentMethod ENUM("Bank transfer (automatic)","Credit card (automatic)","Electronic check","Mailed check"),
	PhoneService ENUM("No","Yes"),
	SeniorCitizen ENUM("0","1"),
	StreamingMovies ENUM("No","No internet service","Yes"),
	StreamingTV ENUM("No","No internet service","Yes"),
	TechSupport ENUM("No","No internet service","Yes"),
	gender ENUM("Female","Male"),
	tenure int DEFAULT 0,
	MonthlyCharges DECIMAL(10,2) DEFAULT 0,
	TotalCharges DECIMAL(10,2) DEFAULT 0,
	createdTime DATETIME NOT NULL,
	PRIMARY KEY (customerID)
);`
}

func ConnectSQLX(ds datasource.DataSource) (*sqlx.DB, error) {
	if dsn, err := ds.Name(); err != nil {
		return nil, err
	} else {
		return sqlx.Connect(ds.Driver, dsn)
	}
}

func InsertData(ds datasource.DataSource) error {
	db, err := ConnectSQLX(ds)
	if err != nil {
		return err
	}
	tbl := data.DataTable()
	cols2 := slices.Clone(tbl.Columns)
	cols2 = append(cols2, data.ColumnCreatedTime)
	insertSQL, err := sqlutil.BuildSQLXInsertSQLNamedParams(data.TableName, cols2)
	if err != nil {
		return err
	}

	insertStmt, err := db.PrepareNamed(insertSQL)
	if err != nil {
		return err
	}

	for _, row := range tbl.Rows {
		doc := tbl.Columns.RowMap(row, false)
		doca, err := docToDocA(doc)
		if err != nil {
			return err
		} else if _, err = insertStmt.Exec(doca); err != nil {
			if err := ErrorExcludeDuplicateEntry(err); err != nil {
				return err
			}
		}
	}

	return nil
}

func docToDocA(doc map[string]string) (map[string]any, error) {
	doca := map[string]any{}
	for colName, v := range doc {
		switch colName {
		case data.ColumnTenure:
			if v, ok := doc[data.ColumnTenure]; ok {
				if v2 := strings.TrimSpace(v); v2 == "" {
					doca[data.ColumnTenure] = int(0)
				} else if v2, err := strconv.Atoi(v); err != nil {
					return doca, errorsutil.Wrapf(err, "colName (%s)", colName)
				} else {
					doca[data.ColumnTenure] = v2
				}
			} else {
				doca[data.ColumnTenure] = int(0)
			}
		case data.ColumnMonthlyCharges:
			if v, ok := doc[data.ColumnMonthlyCharges]; ok {
				if v2 := strings.TrimSpace(v); v2 == "" {
					doca[data.ColumnMonthlyCharges] = 0
				} else if v2, err := strconvutil.Atof(v, true); err != nil {
					return doca, err
				} else {
					doca[data.ColumnMonthlyCharges] = v2
				}
			} else {
				doca[data.ColumnMonthlyCharges] = 0
			}
		case data.ColumnTotalCharges:
			if v, ok := doc[data.ColumnTotalCharges]; ok {
				if v2 := strings.TrimSpace(v); v2 == "" {
					doca[data.ColumnTotalCharges] = 0
				} else if v2, err := strconvutil.Atof(v, true); err != nil {
					return doca, err
				} else {
					doca[data.ColumnTotalCharges] = v2
				}
			} else {
				doca[data.ColumnTotalCharges] = 0
			}
		default:
			doca[colName] = v
		}
	}
	dtNow := time.Now()
	if tenure, ok := doca[data.ColumnTenure]; ok {
		teni := tenure.(int)
		if teni <= 0 {
			doca[data.ColumnCreatedTime] = dtNow
		} else {
			doca[data.ColumnCreatedTime] = dtNow.Add(-1 * timeutil.Day * 30 * time.Duration(teni))
		}
	} else {
		doca[data.ColumnCreatedTime] = dtNow
	}
	return doca, nil
}
