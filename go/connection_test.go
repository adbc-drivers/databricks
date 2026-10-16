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
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNamespaceOperationsHonorCancellation(t *testing.T) {
	tests := []struct {
		name string
		run  func(context.Context, *connectionImpl) error
	}{
		{"get catalog", func(ctx context.Context, conn *connectionImpl) error {
			_, err := conn.GetCurrentCatalog(ctx)
			return err
		}},
		{"get schema", func(ctx context.Context, conn *connectionImpl) error {
			_, err := conn.GetCurrentDbSchema(ctx)
			return err
		}},
		{"set catalog", func(ctx context.Context, conn *connectionImpl) error {
			return conn.SetCurrentCatalog(ctx, "main")
		}},
		{"set schema", func(ctx context.Context, conn *connectionImpl) error {
			return conn.SetCurrentDbSchema(ctx, "default")
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			capture := &parameterCaptureConn{}
			sql.Register(t.Name(), parameterCaptureDriver{conn: capture})
			db, err := sql.Open(t.Name(), "")
			require.NoError(t, err)
			defer func() { require.NoError(t, db.Close()) }()
			sqlConn, err := db.Conn(context.Background())
			require.NoError(t, err)
			defer func() { require.NoError(t, sqlConn.Close()) }()

			conn := &connectionImpl{conn: sqlConn}
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			require.ErrorContains(t, test.run(ctx, conn), context.Canceled.Error())
			require.Empty(t, conn.catalog)
			require.Empty(t, conn.dbSchema)
			require.Empty(t, capture.queries)
		})
	}
}
