// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package cloudsqlsearchcatalog_test

import (
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/googleapis/mcp-toolbox/internal/server"
	"github.com/googleapis/mcp-toolbox/internal/testutils"
	"github.com/googleapis/mcp-toolbox/internal/tools/cloudsql/cloudsqlsearchcatalog"
)

func TestParseFromYamlCloudSQLSearch(t *testing.T) {
	ctx, err := testutils.ContextWithNewLogger()
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
	tcs := []struct {
		desc string
		in   string
		want server.ToolConfigs
	}{
		{
			desc: "mysql example",
			in: `
            kind: tool
            name: example_tool
            type: mysql-search-catalog
            source: my-instance
            description: some description
            `,
			want: server.ToolConfigs{
				"example_tool": cloudsqlsearchcatalog.Config{
					Name:         "example_tool",
					Type:         "mysql-search-catalog",
					Source:       "my-instance",
					Description:  "some description",
					AuthRequired: []string{},
				},
			},
		},
		{
			desc: "mssql example",
			in: `
            kind: tool
            name: example_tool_mssql
            type: mssql-search-catalog
            source: my-mssql-instance
            description: some mssql description
            `,
			want: server.ToolConfigs{
				"example_tool_mssql": cloudsqlsearchcatalog.Config{
					Name:         "example_tool_mssql",
					Type:         "mssql-search-catalog",
					Source:       "my-mssql-instance",
					Description:  "some mssql description",
					AuthRequired: []string{},
				},
			},
		},
	}
	for _, tc := range tcs {
		t.Run(tc.desc, func(t *testing.T) {
			// Parse contents
			_, _, _, got, _, _, err := server.UnmarshalResourceConfig(ctx, testutils.FormatYaml(tc.in))
			if err != nil {
				t.Fatalf("unable to unmarshal: %s", err)
			}
			if diff := cmp.Diff(tc.want, got); diff != "" {
				t.Fatalf("incorrect parse: diff %v", diff)
			}
		})
	}
}

func TestExtractType(t *testing.T) {
	tcs := []struct {
		desc string
		in   string
		want string
	}{
		{
			desc: "mysql table",
			in:   "projects/my-project/locations/global/entryTypes/cloudsql-mysql-table",
			want: "TABLE",
		},
		{
			desc: "mssql database",
			in:   "projects/my-project/locations/global/entryTypes/cloudsql-sqlserver-database",
			want: "DATABASE",
		},
		{
			desc: "postgres view",
			in:   "projects/my-project/locations/global/entryTypes/cloudsql-postgresql-view",
			want: "VIEW",
		},
		{
			desc: "mysql instance",
			in:   "projects/my-project/locations/global/entryTypes/cloudsql-mysql-instance",
			want: "SERVICE",
		},
		{
			desc: "postgres schema",
			in:   "projects/my-project/locations/global/entryTypes/cloudsql-postgresql-schema",
			want: "DATABASE_SCHEMA",
		},
		{
			desc: "no slash returns input",
			in:   "cloudsql-mysql-table",
			want: "cloudsql-mysql-table",
		},
		{
			desc: "unknown type with slash returns empty",
			in:   "projects/my-project/locations/global/entryTypes/unknown-type",
			want: "",
		},
	}
	for _, tc := range tcs {
		t.Run(tc.desc, func(t *testing.T) {
			got := cloudsqlsearchcatalog.ExtractType(tc.in)
			if got != tc.want {
				t.Errorf("ExtractType(%q) = %q, want %q", tc.in, got, tc.want)
			}
		})
	}
}
