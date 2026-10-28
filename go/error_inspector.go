// Copyright (c) 2026 ADBC Drivers Contributors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//         http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package databricks

import (
	"errors"

	"github.com/apache/arrow-adbc/go/adbc"
	dbsqlerr "github.com/databricks/databricks-sql-go/errors"
)

// sqlStateStatuses maps the SQLSTATEs that identify a missing or existing object to ADBC statuses.
var sqlStateStatuses = map[string]adbc.Status{
	"42P01": adbc.StatusNotFound,      // TABLE_OR_VIEW_NOT_FOUND
	"42704": adbc.StatusNotFound,      // SCHEMA_NOT_FOUND, NO_SUCH_CATALOG_EXCEPTION
	"42883": adbc.StatusNotFound,      // UNRESOLVED_ROUTINE
	"42P07": adbc.StatusAlreadyExists, // TABLE_OR_VIEW_ALREADY_EXISTS
}

type DatabricksErrorInspector struct {
	ingest bool
}

// InspectError maps Databricks execution errors to ADBC errors and preserves SQLSTATE.
func (d DatabricksErrorInspector) InspectError(err error, defaultStatus adbc.Status) adbc.Error {
	adbcErr := adbc.Error{Code: defaultStatus, Msg: err.Error()}
	var executionErr dbsqlerr.DBExecutionError
	for errors.As(err, &executionErr) {
		if sqlState := executionErr.SqlState(); sqlState != "" {
			copy(adbcErr.SqlState[:], sqlState)
			if status, ok := sqlStateStatuses[sqlState]; ok {
				adbcErr.Code = status
			}
			if sqlState == "42703" { // UNRESOLVED_COLUMN
				adbcErr.Code = adbc.StatusInvalidArgument
				if d.ingest {
					// A missing destination column means the existing table's
					// schema conflicts with the ingest input.
					adbcErr.Code = adbc.StatusAlreadyExists
				}
			}
			break
		}
		// A wrapper may have no SQLSTATE even when its cause has one.
		err = errors.Unwrap(executionErr)
	}
	return adbcErr
}
