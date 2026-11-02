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

func (stubConnector) Connect(context.Context) (driver.Conn, error) { return nil, errors.New("stub") }
func (stubConnector) Driver() driver.Driver                        { return nil }

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
