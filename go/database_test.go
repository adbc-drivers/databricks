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
	"testing"

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
