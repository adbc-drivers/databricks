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
	"strings"

	"github.com/apache/arrow-adbc/go/adbc"
)

// sessionParamName returns the parameter name when key is a
// "databricks.session_param.<name>" option, and whether key has that prefix at all.
func sessionParamName(key string) (name string, isSessionParam bool) {
	return strings.CutPrefix(key, OptionSessionParamPrefix)
}

// setSessionParam stores one session parameter. An invalid name is rejected and
// the parameters already set are left unchanged.
func (d *databaseImpl) setSessionParam(name, value string) error {
	if strings.TrimSpace(name) == "" {
		return d.ErrorHelper.Errorf(adbc.StatusInvalidArgument, "session parameter name is required, e.g. '%sansi_mode'", OptionSessionParamPrefix)
	}
	if d.sessionParams == nil {
		d.sessionParams = make(map[string]string)
	}
	d.sessionParams[name] = value
	return nil
}
