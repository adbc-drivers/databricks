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
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/apache/arrow-adbc/go/adbc"
	dbsql "github.com/databricks/databricks-sql-go"
	"github.com/stretchr/testify/require"
)

func TestResolveConnectionDSN(t *testing.T) {
	tests := []struct {
		name, dsn, expected string
	}{
		{
			name:     "native decimals by default",
			dsn:      "localhost:443/sql/1.0/warehouses/test",
			expected: "https://localhost:443/sql/1.0/warehouses/test?useArrowNativeDecimal=true",
		},
		{
			name:     "preserve explicit disabled option",
			dsn:      "https://localhost:443/sql/1.0/warehouses/test?useArrowNativeDecimal=false",
			expected: "https://localhost:443/sql/1.0/warehouses/test?useArrowNativeDecimal=false",
		},
		{
			name:     "preserve explicit enabled option and HTTP",
			dsn:      "http://localhost:80/sql/1.0/warehouses/test?useArrowNativeDecimal=true",
			expected: "http://localhost:80/sql/1.0/warehouses/test?useArrowNativeDecimal=true",
		},
		{
			name:     "preserve authentication and encoded parameters",
			dsn:      "token:test-token@localhost:443/sql/1.0/warehouses/test?schema=a%2Bb&catalog=main",
			expected: "https://token:test-token@localhost:443/sql/1.0/warehouses/test?catalog=main&schema=a%2Bb&useArrowNativeDecimal=true",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			database := &databaseImpl{uri: test.dsn}
			dsn, err := database.resolveConnectionDSN()
			require.NoError(t, err)
			require.Equal(t, test.expected, dsn)
			require.Equal(t, test.dsn, database.uri)
		})
	}
}

func TestResolveConnectionDSNRejectsInvalidURIs(t *testing.T) {
	for _, dsn := range []string{
		"localhost:443/%zz",
		"localhost:443/sql/1.0/warehouses/test?schema=%zz",
	} {
		t.Run(dsn, func(t *testing.T) {
			database := &databaseImpl{uri: dsn}
			_, err := database.resolveConnectionDSN()
			requireADBCStatus(t, err, adbc.StatusInvalidArgument)
		})
	}
}

// stubConnector opens connections that are never used; it only lets a test hold
// a non-nil *sql.DB.
type stubConnector struct{}

func (stubConnector) Connect(context.Context) (driver.Conn, error) { return stubConn{}, nil }
func (stubConnector) Driver() driver.Driver                        { return stubDriver{} }

type stubDriver struct{}

func (stubDriver) Open(string) (driver.Conn, error) { return stubConn{}, nil }

// stubConn connects but runs nothing.
type stubConn struct{}

func (stubConn) Prepare(string) (driver.Stmt, error) { return nil, errors.New("stub") }
func (stubConn) Close() error                        { return nil }
func (stubConn) Begin() (driver.Tx, error)           { return nil, errors.New("stub") }

// Options can be set and read while another thread opens a connection. This is
// a data race without the lock, which `go test -race` reports.
func TestOptionsWhileOpening(t *testing.T) {
	ctx := context.Background()
	db := &databaseImpl{}
	db.newPool = func(context.Context) (*sql.DB, error) { return sql.OpenDB(stubConnector{}), nil }

	var wg sync.WaitGroup
	for range 20 {
		wg.Add(3)
		go func() { defer wg.Done(); _ = db.SetOption(ctx, OptionCatalog, "main") }()
		go func() { defer wg.Done(); _, _ = db.GetOption(ctx, OptionCatalog) }()
		go func() {
			defer wg.Done()
			conn, err := db.Open(ctx)
			if err == nil {
				_ = conn.Close(ctx)
			}
		}()
	}
	wg.Wait()
}

func TestGetOrCreatePoolSerialisesStartup(t *testing.T) {
	var started, inFlight, maxInFlight atomic.Int32
	db := &databaseImpl{needsRefresh: true}
	db.newPool = func(context.Context) (*sql.DB, error) {
		started.Add(1)
		n := inFlight.Add(1)
		for {
			m := maxInFlight.Load()
			if n <= m || maxInFlight.CompareAndSwap(m, n) {
				break
			}
		}
		time.Sleep(20 * time.Millisecond)
		inFlight.Add(-1)
		return sql.OpenDB(stubConnector{}), nil
	}

	const callers = 10
	pools := make([]*sql.DB, callers)
	errs := make([]error, callers)
	var wg sync.WaitGroup
	for i := range callers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			pools[i], errs[i] = db.getOrCreatePool(context.Background())
		}()
	}
	wg.Wait()

	for i := range callers {
		require.NoError(t, errs[i])
		require.Same(t, pools[0], pools[i], "every caller shares the one pool")
	}
	require.EqualValues(t, 1, started.Load(), "pool must start once, not once per caller")
	require.EqualValues(t, 1, maxInFlight.Load())
	require.False(t, db.needsRefresh)

	// An option change makes the next call build a new pool and close the old one.
	db.needsRefresh = true
	next, err := db.getOrCreatePool(context.Background())
	require.NoError(t, err)
	require.NotSame(t, pools[0], next)
	require.EqualValues(t, 2, started.Load())
}

func newSessionParamDatabase() *databaseImpl {
	return &databaseImpl{serverHostname: "x.cloud.databricks.com", httpPath: "/sql/1.0/warehouses/w", accessToken: "tok"}
}

func TestSessionParamOptions(t *testing.T) {
	ctx := context.Background()
	db := newSessionParamDatabase()

	require.NoError(t, db.SetOption(ctx, OptionSessionParamPrefix+"ansi_mode", "false"))
	require.NoError(t, db.SetOption(ctx, OptionSessionParamPrefix+"timezone", "UTC"))
	got, err := db.GetOption(ctx, OptionSessionParamPrefix+"ansi_mode")
	require.NoError(t, err)
	require.Equal(t, "false", got)

	_, err = db.GetOption(ctx, OptionSessionParamPrefix+"missing")
	var adbcErr adbc.Error
	require.ErrorAs(t, err, &adbcErr)
	require.Equal(t, adbc.StatusNotFound, adbcErr.Code)

	// An empty name is rejected and leaves the stored parameters alone.
	err = db.SetOption(ctx, OptionSessionParamPrefix, "x")
	require.ErrorAs(t, err, &adbcErr)
	require.Equal(t, adbc.StatusInvalidArgument, adbcErr.Code)
	require.Equal(t, map[string]string{"ansi_mode": "false", "timezone": "UTC"}, db.sessionParams)
}

func TestSessionParamsReachConnectorOptions(t *testing.T) {
	db := newSessionParamDatabase()

	// Query tags are added to the connector's session parameters; they must not
	// leak into the database's own map.
	db.sessionParams = map[string]string{"ansi_mode": "false"}
	db.queryTags = map[string]string{"team": "data"}
	opts, err := db.resolveConnectionOptions()
	require.NoError(t, err)
	_, err = dbsql.NewConnector(opts...)
	require.NoError(t, err)
	require.Equal(t, map[string]string{"ansi_mode": "false"}, db.sessionParams)
}
