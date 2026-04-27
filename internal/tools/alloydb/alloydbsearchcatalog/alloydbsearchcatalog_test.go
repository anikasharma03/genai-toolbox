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

package alloydbsearchcatalog_test

import (
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/googleapis/mcp-toolbox/internal/server"
	"github.com/googleapis/mcp-toolbox/internal/testutils"
	"github.com/googleapis/mcp-toolbox/internal/tools/alloydb/alloydbsearchcatalog"
)

func TestParseFromYamlAlloyDBSearch(t *testing.T) {
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
			desc: "basic example",
			in: `
            kind: tool
            name: example_tool
            type: alloydb-search-catalog
            source: my-instance
            description: some description
            `,
			want: server.ToolConfigs{
				"example_tool": alloydbsearchcatalog.Config{
					Name:         "example_tool",
					Type:         "alloydb-search-catalog",
					Source:       "my-instance",
					Description:  "some description",
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
			desc: "mapped type table",
			in:   "projects/my-project/locations/global/entryTypes/alloydb-table",
			want: "TABLE",
		},
		{
			desc: "mapped type view",
			in:   "projects/my-project/locations/global/entryTypes/alloydb-view",
			want: "VIEW",
		},
		{
			desc: "mapped type schema",
			in:   "projects/my-project/locations/global/entryTypes/alloydb-schema",
			want: "DATABASE_SCHEMA",
		},
		{
			desc: "mapped type instance",
			in:   "projects/my-project/locations/global/entryTypes/alloydb-instance",
			want: "INSTANCE",
		},
		{
			desc: "mapped type cluster",
			in:   "projects/my-project/locations/global/entryTypes/alloydb-cluster",
			want: "CLUSTER",
		},
		{
			desc: "mapped type database",
			in:   "projects/my-project/locations/global/entryTypes/alloydb-database",
			want: "DATABASE",
		},
		{
			desc: "no slash returns input",
			in:   "alloydb-table",
			want: "alloydb-table",
		},
		{
			desc: "unknown type with slash returns empty",
			in:   "projects/my-project/locations/global/entryTypes/unknown-type",
			want: "",
		},
	}
	for _, tc := range tcs {
		t.Run(tc.desc, func(t *testing.T) {
			got := alloydbsearchcatalog.ExtractType(tc.in)
			if got != tc.want {
				t.Errorf("ExtractType(%q) = %q, want %q", tc.in, got, tc.want)
			}
		})
	}
}
