// Copyright 2024 Google LLC
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

package alloydbpg

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"regexp"
	"strings"
	"testing"
	"time"

	"cloud.google.com/go/alloydbconn"
	"github.com/google/uuid"
	"github.com/googleapis/mcp-toolbox/internal/sources"
	"github.com/googleapis/mcp-toolbox/internal/testutils"
	"github.com/googleapis/mcp-toolbox/tests"
	"github.com/jackc/pgx/v5/pgxpool"
)

var (
	AlloyDBPostgresSourceType = "alloydb-postgres"
	AlloyDBPostgresToolType   = "postgres-sql"
	AlloyDBPostgresProject    = os.Getenv("ALLOYDB_POSTGRES_PROJECT")
	AlloyDBPostgresRegion     = os.Getenv("ALLOYDB_POSTGRES_REGION")
	AlloyDBPostgresCluster    = os.Getenv("ALLOYDB_POSTGRES_CLUSTER")
	AlloyDBPostgresInstance   = os.Getenv("ALLOYDB_POSTGRES_INSTANCE")
	AlloyDBPostgresDatabase   = os.Getenv("ALLOYDB_POSTGRES_DATABASE")
	AlloyDBPostgresUser       = os.Getenv("ALLOYDB_POSTGRES_USER")
	AlloyDBPostgresPass       = os.Getenv("ALLOYDB_POSTGRES_PASSWORD")
)

func getAlloyDBPgVars(t *testing.T) map[string]any {
	switch "" {
	case AlloyDBPostgresProject:
		t.Fatal("'ALLOYDB_POSTGRES_PROJECT' not set")
	case AlloyDBPostgresRegion:
		t.Fatal("'ALLOYDB_POSTGRES_REGION' not set")
	case AlloyDBPostgresCluster:
		t.Fatal("'ALLOYDB_POSTGRES_CLUSTER' not set")
	case AlloyDBPostgresInstance:
		t.Fatal("'ALLOYDB_POSTGRES_INSTANCE' not set")
	case AlloyDBPostgresDatabase:
		t.Fatal("'ALLOYDB_POSTGRES_DATABASE' not set")
	case AlloyDBPostgresUser:
		t.Fatal("'ALLOYDB_POSTGRES_USER' not set")
	case AlloyDBPostgresPass:
		t.Fatal("'ALLOYDB_POSTGRES_PASSWORD' not set")
	}
	return map[string]any{
		"type":     AlloyDBPostgresSourceType,
		"project":  AlloyDBPostgresProject,
		"cluster":  AlloyDBPostgresCluster,
		"instance": AlloyDBPostgresInstance,
		"region":   AlloyDBPostgresRegion,
		"database": AlloyDBPostgresDatabase,
		"user":     AlloyDBPostgresUser,
		"password": AlloyDBPostgresPass,
	}
}

// Copied over from  alloydb_pg.go
func getAlloyDBDialOpts(ipType string) ([]alloydbconn.DialOption, error) {
	switch strings.ToLower(ipType) {
	case "private":
		return []alloydbconn.DialOption{alloydbconn.WithPrivateIP()}, nil
	case "public":
		return []alloydbconn.DialOption{alloydbconn.WithPublicIP()}, nil
	default:
		return nil, fmt.Errorf("invalid ipType %s", ipType)
	}
}

// Copied over from  alloydb_pg.go
func initAlloyDBPgConnectionPool(project, region, cluster, instance, ipType, user, pass, dbname string) (*pgxpool.Pool, error) {
	// Configure the driver to connect to the database
	dsn := fmt.Sprintf("user=%s password=%s dbname=%s sslmode=disable", user, pass, dbname)
	config, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("unable to parse connection uri: %w", err)
	}

	// Create a new dialer with options
	dialOpts, err := getAlloyDBDialOpts(ipType)
	if err != nil {
		return nil, err
	}
	d, err := alloydbconn.NewDialer(context.Background(), alloydbconn.WithDefaultDialOptions(dialOpts...))
	if err != nil {
		return nil, fmt.Errorf("unable to parse connection uri: %w", err)
	}

	// Tell the driver to use the AlloyDB Go Connector to create connections
	i := fmt.Sprintf("projects/%s/locations/%s/clusters/%s/instances/%s", project, region, cluster, instance)
	config.ConnConfig.DialFunc = func(ctx context.Context, _ string, instance string) (net.Conn, error) {
		return d.Dial(ctx, i)
	}

	// Interact with the driver directly as you normally would
	pool, err := pgxpool.NewWithConfig(context.Background(), config)
	if err != nil {
		return nil, err
	}
	return pool, nil
}

func TestAlloyDBPgToolEndpoints(t *testing.T) {
	sourceConfig := getAlloyDBPgVars(t)
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	args := []string{"--enable-api"}

	pool, err := initAlloyDBPgConnectionPool(AlloyDBPostgresProject, AlloyDBPostgresRegion, AlloyDBPostgresCluster, AlloyDBPostgresInstance, "public", AlloyDBPostgresUser, AlloyDBPostgresPass, AlloyDBPostgresDatabase)
	if err != nil {
		t.Fatalf("unable to create AlloyDB connection pool: %s", err)
	}

	// Generate a unique ID
	uniqueID := strings.ReplaceAll(uuid.New().String(), "-", "")

	// This will execute after all tool tests complete (success, fail, or t.Fatal)
	t.Cleanup(func() {
		tests.CleanupPostgresTables(t, context.Background(), pool, uniqueID)
	})

	//Create table names using the UUID
	tableNameParam := "param_table_" + uniqueID
	tableNameAuth := "auth_table_" + uniqueID
	tableNameTemplateParam := "template_param_table_" + uniqueID

	// set up data for param tool
	createParamTableStmt, insertParamTableStmt, paramToolStmt, idParamToolStmt, nameParamToolStmt, arrayToolStmt, paramTestParams := tests.GetPostgresSQLParamToolInfo(tableNameParam)
	teardownTable1 := tests.SetupPostgresSQLTable(t, ctx, pool, createParamTableStmt, insertParamTableStmt, tableNameParam, paramTestParams)
	defer teardownTable1(t)

	// set up data for auth tool
	createAuthTableStmt, insertAuthTableStmt, authToolStmt, authTestParams := tests.GetPostgresSQLAuthToolInfo(tableNameAuth)
	teardownTable2 := tests.SetupPostgresSQLTable(t, ctx, pool, createAuthTableStmt, insertAuthTableStmt, tableNameAuth, authTestParams)
	defer teardownTable2(t)

	// Set up table for semanti search
	vectorTableName, tearDownVectorTable := tests.SetupPostgresVectorTable(t, ctx, pool)
	defer tearDownVectorTable(t)

	// Write config into a file and pass it to command
	toolsFile := tests.GetToolsConfig(sourceConfig, AlloyDBPostgresToolType, paramToolStmt, idParamToolStmt, nameParamToolStmt, arrayToolStmt, authToolStmt)
	toolsFile = tests.AddExecuteSqlConfig(t, toolsFile, "postgres-execute-sql")
	tmplSelectCombined, tmplSelectFilterCombined := tests.GetPostgresSQLTmplToolStatement()
	toolsFile = tests.AddTemplateParamConfig(t, toolsFile, AlloyDBPostgresToolType, tmplSelectCombined, tmplSelectFilterCombined, "")

	// Add semantic search tool config
	insertStmt, searchStmt := tests.GetPostgresVectorSearchStmts(vectorTableName)
	toolsFile = tests.AddSemanticSearchConfig(t, toolsFile, AlloyDBPostgresToolType, insertStmt, searchStmt)

	toolsFile = tests.AddPostgresPrebuiltConfig(t, toolsFile)

	// Add AlloyDB Admin sources for search catalog
	sourcesMap, ok := toolsFile["sources"].(map[string]any)
	if !ok {
		t.Fatalf("unable to get sources from config")
	}
	sourcesMap["alloydb-admin-source"] = map[string]any{
		"type": "alloydb-admin",
	}
	sourcesMap["alloydb-oauth-source"] = map[string]any{
		"type":           "alloydb-admin",
		"useClientOAuth": true,
	}

	// Add search catalog tools
	toolsMap, ok := toolsFile["tools"].(map[string]any)
	if !ok {
		t.Fatalf("unable to get tools from config")
	}
	toolsMap["my-search-catalog-tool"] = map[string]any{
		"type":        "alloydb-search-catalog",
		"source":      "alloydb-admin-source",
		"description": "Searches for data assets in catalog",
	}
	toolsMap["my-auth-search-catalog-tool"] = map[string]any{
		"type":        "alloydb-search-catalog",
		"source":      "alloydb-admin-source",
		"description": "Searches for data assets in catalog",
		"authRequired": []string{
			"my-google-auth",
		},
	}
	toolsMap["my-client-auth-search-catalog-tool"] = map[string]any{
		"type":        "alloydb-search-catalog",
		"source":      "alloydb-oauth-source",
		"description": "Searches for data assets in catalog",
	}

	cmd, cleanup, err := tests.StartCmd(ctx, toolsFile, args...)
	if err != nil {
		t.Fatalf("command initialization returned an error: %s", err)
	}
	defer cleanup()

	waitCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	out, err := testutils.WaitForString(waitCtx, regexp.MustCompile(`Server ready to serve`), cmd.Out)
	if err != nil {
		t.Logf("toolbox command logs: \n%s", out)
		t.Fatalf("toolbox didn't start successfully: %s", err)
	}

	// Get configs for tests
	select1Want, failInvocationWant, createTableStatement, mcpSelect1Want := tests.GetPostgresWants()

	// Run tests
	tests.RunToolGetTest(t)
	tests.RunToolInvokeTest(t, select1Want)
	tests.RunMCPToolCallMethod(t, failInvocationWant, mcpSelect1Want)
	tests.RunExecuteSqlToolInvokeTest(t, createTableStatement, select1Want)
	tests.RunToolInvokeWithTemplateParameters(t, tableNameTemplateParam)

	// Run Postgres prebuilt tool tests
	tests.RunPostgresListTablesTest(t, tableNameParam, tableNameAuth, AlloyDBPostgresUser)
	tests.RunPostgresListViewsTest(t, ctx, pool)
	tests.RunPostgresListSchemasTest(t, ctx, pool, AlloyDBPostgresUser, uniqueID)
	tests.RunPostgresListActiveQueriesTest(t, ctx, pool)
	tests.RunPostgresListAvailableExtensionsTest(t)
	tests.RunPostgresListInstalledExtensionsTest(t)
	tests.RunPostgresDatabaseOverviewTest(t, ctx, pool)
	tests.RunPostgresListTriggersTest(t, ctx, pool)
	tests.RunPostgresListIndexesTest(t, ctx, pool)
	tests.RunPostgresListSequencesTest(t, ctx, pool)
	tests.RunPostgresListLocksTest(t, ctx, pool)
	tests.RunPostgresReplicationStatsTest(t, ctx, pool)
	tests.RunPostgresLongRunningTransactionsTest(t, ctx, pool)
	tests.RunPostgresListQueryStatsTest(t, ctx, pool)
	tests.RunPostgresGetColumnCardinalityTest(t, ctx, pool)
	tests.RunPostgresListTableStatsTest(t, ctx, pool)
	tests.RunPostgresListPublicationTablesTest(t, ctx, pool)
	tests.RunPostgresListTableSpacesTest(t)
	tests.RunPostgresListPgSettingsTest(t, ctx, pool)
	tests.RunPostgresListDatabaseStatsTest(t, ctx, pool)
	tests.RunPostgresListRolesTest(t, ctx, pool)
	tests.RunPostgresListStoredProcedureTest(t, ctx, pool)

	// Run search catalog test
	runAlloyDBSearchCatalogTest(t, AlloyDBPostgresProject, AlloyDBPostgresCluster)
}

func TestAlloyDBPgPrebuiltStatementTools(t *testing.T) {
	getAlloyDBPgVars(t)
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	args := []string{"--prebuilt", "alloydb-postgres", "--enable-api"}

	cmd, cleanup, err := tests.StartCmd(ctx, map[string]any{}, args...)
	if err != nil {
		t.Fatalf("command initialization returned an error: %s", err)
	}
	defer cleanup()

	waitCtx, cancelWait := context.WithTimeout(ctx, 10*time.Second)
	defer cancelWait()
	out, err := testutils.WaitForString(waitCtx, regexp.MustCompile(`Server ready to serve`), cmd.Out)
	if err != nil {
		t.Logf("toolbox command logs: \n%s", out)
		t.Fatalf("toolbox didn't start successfully: %s", err)
	}

	toolsToTest := map[string]string{
		"list_autovacuum_configurations": `{}`,
		"list_memory_configurations":     `{}`,
		"list_top_bloated_tables":        `{"limit": 10}`,
		"list_replication_slots":         `{}`,
		"list_invalid_indexes":           `{}`,
		"get_query_plan":                 `{"query": "SELECT 1"}`,
	}

	tests.RunStatementToolsTest(t, toolsToTest)
}

// Test connection with different IP type
func TestAlloyDBPgIpConnection(t *testing.T) {
	sourceConfig := getAlloyDBPgVars(t)

	tcs := []struct {
		name   string
		ipType string
	}{
		{
			name:   "public ip",
			ipType: "public",
		},
		{
			name:   "private ip",
			ipType: "private",
		},
	}
	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			sourceConfig["ipType"] = tc.ipType
			err := tests.RunSourceConnectionTest(t, sourceConfig, AlloyDBPostgresToolType)
			if err != nil {
				t.Fatalf("Connection test failure: %s", err)
			}
		})
	}
}

// Test IAM connection
func TestAlloyDBPgIAMConnection(t *testing.T) {
	getAlloyDBPgVars(t)
	// service account email used for IAM should trim the suffix
	serviceAccountEmail := strings.TrimSuffix(tests.ServiceAccountEmail, ".gserviceaccount.com")

	noPassSourceConfig := map[string]any{
		"type":     AlloyDBPostgresSourceType,
		"project":  AlloyDBPostgresProject,
		"cluster":  AlloyDBPostgresCluster,
		"instance": AlloyDBPostgresInstance,
		"region":   AlloyDBPostgresRegion,
		"database": AlloyDBPostgresDatabase,
		"user":     serviceAccountEmail,
	}

	noUserSourceConfig := map[string]any{
		"type":     AlloyDBPostgresSourceType,
		"project":  AlloyDBPostgresProject,
		"cluster":  AlloyDBPostgresCluster,
		"instance": AlloyDBPostgresInstance,
		"region":   AlloyDBPostgresRegion,
		"database": AlloyDBPostgresDatabase,
		"password": "random",
	}

	noUserNoPassSourceConfig := map[string]any{
		"type":     AlloyDBPostgresSourceType,
		"project":  AlloyDBPostgresProject,
		"cluster":  AlloyDBPostgresCluster,
		"instance": AlloyDBPostgresInstance,
		"region":   AlloyDBPostgresRegion,
		"database": AlloyDBPostgresDatabase,
	}
	tcs := []struct {
		name         string
		sourceConfig map[string]any
		isErr        bool
	}{
		{
			name:         "no user no pass",
			sourceConfig: noUserNoPassSourceConfig,
			isErr:        false,
		},
		{
			name:         "no password",
			sourceConfig: noPassSourceConfig,
			isErr:        false,
		},
		{
			name:         "no user",
			sourceConfig: noUserSourceConfig,
			isErr:        true,
		},
	}
	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			err := tests.RunSourceConnectionTest(t, tc.sourceConfig, AlloyDBPostgresToolType)
			if err != nil {
				if tc.isErr {
					return
				}
				t.Fatalf("Connection test failure: %s", err)
			}
			if tc.isErr {
				t.Fatalf("Expected error but test passed.")
			}
		})
	}
}

func runAlloyDBSearchCatalogTest(t *testing.T, project, cluster string) {
	// Get ID token
	idToken, err := tests.GetGoogleIdToken(t)
	if err != nil {
		t.Fatalf("error getting Google ID token: %s", err)
	}

	// Get access token
	accessToken, err := sources.GetIAMAccessToken(t.Context())
	if err != nil {
		t.Fatalf("error getting access token from ADC: %s", err)
	}
	accessToken = "Bearer " + accessToken

	databaseName := cluster // Using cluster as a placeholder for database name
	tableName := "test-table" // Dummy table name

	// Test tool invoke endpoint
	invokeTcs := []struct {
		name          string
		api           string
		requestHeader map[string]string
		requestBody   io.Reader
		wantKey       string
		isErr         bool
	}{
		{
			name:          "invoke my-search-catalog-tool without body",
			api:           "http://127.0.0.1:5000/api/tool/my-search-catalog-tool/invoke",
			requestHeader: map[string]string{},
			requestBody:   bytes.NewBuffer([]byte(`{}`)),
			isErr:         true,
		},
		{
			name:          "invoke my-search-catalog-tool",
			api:           "http://127.0.0.1:5000/api/tool/my-search-catalog-tool/invoke",
			requestHeader: map[string]string{},
			requestBody:   bytes.NewBuffer([]byte(fmt.Sprintf(`{"prompt":"%s", "types":["TABLE"], "databaseIds":["%s"]}`, tableName, databaseName))),
			wantKey:       "DisplayName",
			isErr:         false,
		},
		{
			name:          "Invoke my-auth-search-catalog-tool with auth token",
			api:           "http://127.0.0.1:5000/api/tool/my-auth-search-catalog-tool/invoke",
			requestHeader: map[string]string{"my-google-auth_token": idToken},
			requestBody:   bytes.NewBuffer([]byte(fmt.Sprintf(`{"prompt":"%s", "types":["TABLE"], "databaseIds":["%s"]}`, tableName, databaseName))),
			wantKey:       "DisplayName",
			isErr:         false,
		},
		{
			name:          "Invoke my-auth-search-catalog-tool with correct project",
			api:           "http://127.0.0.1:5000/api/tool/my-auth-search-catalog-tool/invoke",
			requestHeader: map[string]string{"my-google-auth_token": idToken},
			requestBody:   bytes.NewBuffer([]byte(fmt.Sprintf(`{"prompt":"%s", "types":["TABLE"], "projectIds":["%s"], "databaseIds":["%s"]}`, tableName, project, databaseName))),
			wantKey:       "DisplayName",
			isErr:         false,
		},
		{
			name:          "Invoke my-auth-search-catalog-tool with non-existent project",
			api:           "http://127.0.0.1:5000/api/tool/my-auth-search-catalog-tool/invoke",
			requestHeader: map[string]string{"my-google-auth_token": idToken},
			requestBody:   bytes.NewBuffer([]byte(fmt.Sprintf(`{"prompt":"%s", "types":["TABLE"], "projectIds":["%s-%s"], "databaseIds":["%s"]}`, tableName, project, uuid.NewString(), databaseName))),
			isErr:         true,
		},
		{
			name:          "Invoke my-auth-search-catalog-tool with invalid auth token",
			api:           "http://127.0.0.1:5000/api/tool/my-auth-search-catalog-tool/invoke",
			requestHeader: map[string]string{"my-google-auth_token": "INVALID_TOKEN"},
			requestBody:   bytes.NewBuffer([]byte(fmt.Sprintf(`{"prompt":"%s", "types":["TABLE"], "databaseIds":["%s"]}`, tableName, databaseName))),
			isErr:         true,
		},
		{
			name:          "Invoke my-auth-search-catalog-tool without auth token",
			api:           "http://127.0.0.1:5000/api/tool/my-auth-search-catalog-tool/invoke",
			requestHeader: map[string]string{},
			requestBody:   bytes.NewBuffer([]byte(fmt.Sprintf(`{"prompt":"%s", "types":["TABLE"], "databaseIds":["%s"]}`, tableName, databaseName))),
			isErr:         true,
		},
		{
			name:          "Invoke my-client-auth-search-catalog-tool without auth token",
			api:           "http://127.0.0.1:5000/api/tool/my-client-auth-search-catalog-tool/invoke",
			requestHeader: map[string]string{},
			requestBody:   bytes.NewBuffer([]byte(fmt.Sprintf(`{"prompt":"%s", "types":["TABLE"], "databaseIds":["%s"]}`, tableName, databaseName))),
			isErr:         true,
		},
		{
			name:          "Invoke my-client-auth-search-catalog-tool with auth token",
			api:           "http://127.0.0.1:5000/api/tool/my-client-auth-search-catalog-tool/invoke",
			requestHeader: map[string]string{"Authorization": accessToken},
			requestBody:   bytes.NewBuffer([]byte(fmt.Sprintf(`{"prompt":"%s", "types":["TABLE"], "databaseIds":["%s"]}`, tableName, databaseName))),
			wantKey:       "DisplayName",
			isErr:         false,
		},
	}

	for _, tc := range invokeTcs {
		t.Run(tc.name, func(t *testing.T) {
			req, err := http.NewRequest(http.MethodPost, tc.api, tc.requestBody)
			if err != nil {
				t.Fatalf("unable to create request: %s", err)
			}
			req.Header.Add("Content-type", "application/json")
			for k, v := range tc.requestHeader {
				req.Header.Add(k, v)
			}
			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				t.Fatalf("unable to send request: %s", err)
			}
			defer resp.Body.Close()

			if resp.StatusCode != http.StatusOK {
				if tc.isErr {
					return
				}
				bodyBytes, _ := io.ReadAll(resp.Body)
				t.Fatalf("response status code is not 200, got %d: %s", resp.StatusCode, string(bodyBytes))
			}

			var result map[string]interface{}
			if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
				t.Fatalf("error parsing response body: %s", err)
			}
			resultStr, ok := result["result"].(string)
			if !ok {
				if result["result"] == nil && tc.isErr {
					return
				}
				t.Fatalf("expected 'result' field to be a string, got %T", result["result"])
			}

			var entries []any
			if err := json.Unmarshal([]byte(resultStr), &entries); err != nil {
				t.Fatalf("error unmarshalling result string: %v. Raw string: %s", err, resultStr)
			}

			if !tc.isErr {
				if len(entries) == 0 {
					t.Logf("No entries found, skipping content verification")
					return
				}
				entry, ok := entries[0].(map[string]interface{})
				if !ok {
					t.Fatalf("expected first entry to be a map, got %T", entries[0])
				}
				_, ok = entry[tc.wantKey]
				if !ok {
					t.Fatalf("expected entry to have key '%s', but it was not found", tc.wantKey)
				}
			} else {
				if len(entries) != 0 {
					t.Fatalf("expected 0 entries, but got %d", len(entries))
				}
			}
		})
	}
}
