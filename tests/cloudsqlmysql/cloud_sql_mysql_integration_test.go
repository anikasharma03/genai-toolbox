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

package cloudsqlmysql

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"regexp"
	"slices"
	"strings"
	"testing"
	"time"

	"cloud.google.com/go/cloudsqlconn"
	"cloud.google.com/go/cloudsqlconn/mysql/mysql"
	"github.com/google/uuid"
	"github.com/googleapis/mcp-toolbox/internal/testutils"
	"github.com/googleapis/mcp-toolbox/tests"
)

var (
	CloudSQLMySQLSourceType = "cloud-sql-mysql"
	CloudSQLMySQLToolType   = "mysql-sql"
	CloudSQLMySQLProject    = os.Getenv("CLOUD_SQL_MYSQL_PROJECT")
	CloudSQLMySQLRegion     = os.Getenv("CLOUD_SQL_MYSQL_REGION")
	CloudSQLMySQLInstance   = os.Getenv("CLOUD_SQL_MYSQL_INSTANCE")
	CloudSQLMySQLDatabase   = os.Getenv("CLOUD_SQL_MYSQL_DATABASE")
	CloudSQLMySQLUser       = os.Getenv("CLOUD_SQL_MYSQL_USER")
	CloudSQLMySQLPass       = os.Getenv("CLOUD_SQL_MYSQL_PASS")
)

func getCloudSQLMySQLVars(t *testing.T) map[string]any {
	switch "" {
	case CloudSQLMySQLProject:
		t.Fatal("'CLOUD_SQL_MYSQL_PROJECT' not set")
	case CloudSQLMySQLRegion:
		t.Fatal("'CLOUD_SQL_MYSQL_REGION' not set")
	case CloudSQLMySQLInstance:
		t.Fatal("'CLOUD_SQL_MYSQL_INSTANCE' not set")
	case CloudSQLMySQLDatabase:
		t.Fatal("'CLOUD_SQL_MYSQL_DATABASE' not set")
	case CloudSQLMySQLUser:
		t.Fatal("'CLOUD_SQL_MYSQL_USER' not set")
	case CloudSQLMySQLPass:
		t.Fatal("'CLOUD_SQL_MYSQL_PASS' not set")
	}

	return map[string]any{
		"type":     CloudSQLMySQLSourceType,
		"project":  CloudSQLMySQLProject,
		"instance": CloudSQLMySQLInstance,
		"region":   CloudSQLMySQLRegion,
		"database": CloudSQLMySQLDatabase,
		"user":     CloudSQLMySQLUser,
		"password": CloudSQLMySQLPass,
	}
}

// Copied over from cloud_sql_mysql.go
func initCloudSQLMySQLConnectionPool(project, region, instance, ipType, user, pass, dbname string) (*sql.DB, error) {

	// Create a new dialer with options
	dialOpts, err := tests.GetCloudSQLDialOpts(ipType)
	if err != nil {
		return nil, err
	}

	if !slices.Contains(sql.Drivers(), "cloudsql-mysql") {
		_, err = mysql.RegisterDriver("cloudsql-mysql", cloudsqlconn.WithDefaultDialOptions(dialOpts...))
		if err != nil {
			return nil, fmt.Errorf("unable to register driver: %w", err)
		}
	}

	// Tell the driver to use the Cloud SQL Go Connector to create connections
	dsn := fmt.Sprintf("%s:%s@cloudsql-mysql(%s:%s:%s)/%s", user, pass, project, region, instance, dbname)
	db, err := sql.Open(
		"cloudsql-mysql",
		dsn,
	)
	if err != nil {
		return nil, err
	}
	return db, nil
}

func TestCloudSQLMySQLToolEndpoints(t *testing.T) {
	sourceConfig := getCloudSQLMySQLVars(t)
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	args := []string{"--enable-api"}

	pool, err := initCloudSQLMySQLConnectionPool(CloudSQLMySQLProject, CloudSQLMySQLRegion, CloudSQLMySQLInstance, "public", CloudSQLMySQLUser, CloudSQLMySQLPass, CloudSQLMySQLDatabase)
	if err != nil {
		t.Fatalf("unable to create Cloud SQL connection pool: %s", err)
	}

	// cleanup test environment
	tests.CleanupMySQLTables(t, ctx, pool)

	// create table name with UUID
	tableNameParam := "param_table_" + strings.ReplaceAll(uuid.New().String(), "-", "")
	tableNameAuth := "auth_table_" + strings.ReplaceAll(uuid.New().String(), "-", "")
	tableNameTemplateParam := "template_param_table_" + strings.ReplaceAll(uuid.New().String(), "-", "")

	// set up data for param tool
	createParamTableStmt, insertParamTableStmt, paramToolStmt, idParamToolStmt, nameParamToolStmt, arrayToolStmt, paramTestParams := tests.GetMySQLParamToolInfo(tableNameParam)
	teardownTable1 := tests.SetupMySQLTable(t, ctx, pool, createParamTableStmt, insertParamTableStmt, tableNameParam, paramTestParams)
	defer teardownTable1(t)

	// set up data for auth tool
	createAuthTableStmt, insertAuthTableStmt, authToolStmt, authTestParams := tests.GetMySQLAuthToolInfo(tableNameAuth)
	teardownTable2 := tests.SetupMySQLTable(t, ctx, pool, createAuthTableStmt, insertAuthTableStmt, tableNameAuth, authTestParams)
	defer teardownTable2(t)

	// Write config into a file and pass it to command
	toolsFile := tests.GetToolsConfig(sourceConfig, CloudSQLMySQLToolType, paramToolStmt, idParamToolStmt, nameParamToolStmt, arrayToolStmt, authToolStmt)
	toolsFile = tests.AddMySqlExecuteSqlConfig(t, toolsFile)
	tmplSelectCombined, tmplSelectFilterCombined := tests.GetMySQLTmplToolStatement()
	toolsFile = tests.AddTemplateParamConfig(t, toolsFile, CloudSQLMySQLToolType, tmplSelectCombined, tmplSelectFilterCombined, "")
	toolsFile = tests.AddMySQLPrebuiltToolConfig(t, toolsFile)
	// Add search catalog tool config
	tools, ok := toolsFile["tools"].(map[string]any)
	if !ok {
		t.Fatalf("unable to get tools from config")
	}
	tools["my-search-catalog-tool"] = map[string]any{
		"type":        "mysql-search-catalog",
		"source":      "my-instance",
		"description": "Tool to search the Cloud SQL catalog",
	}
	toolsFile["tools"] = tools

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
	select1Want, mcpMyFailToolWant, createTableStatement, mcpSelect1Want := tests.GetMySQLWants()

	// Run tests
	tests.RunToolGetTest(t)
	tests.RunToolInvokeTest(t, select1Want, tests.DisableArrayTest())
	tests.RunMCPToolCallMethod(t, mcpMyFailToolWant, mcpSelect1Want)
	tests.RunExecuteSqlToolInvokeTest(t, createTableStatement, select1Want)
	tests.RunToolInvokeWithTemplateParameters(t, tableNameTemplateParam)

	// Run specific MySQL tool tests
	const expectedOwner = "'toolbox-identity'@'%'"
	tests.RunMySQLListTablesTest(t, CloudSQLMySQLDatabase, tableNameParam, tableNameAuth, expectedOwner)
	tests.RunMySQLListActiveQueriesTest(t, ctx, pool)
	tests.RunMySQLGetQueryPlanTest(t, ctx, pool, CloudSQLMySQLDatabase, tableNameParam)
	tests.RunMySQLListTableStatsTest(t, ctx, pool, CloudSQLMySQLDatabase, tableNameParam, tableNameAuth)

	t.Run("invoke my-search-catalog-tool", func(t *testing.T) {
		api := "http://127.0.0.1:5000/api/tool/my-search-catalog-tool/invoke"
		body := fmt.Sprintf(`{"prompt":"%s", "types":["TABLE"], "databaseIds":["%s"]}`, tableNameParam, CloudSQLMySQLDatabase)
		req, err := http.NewRequest(http.MethodPost, api, bytes.NewBuffer([]byte(body)))
		if err != nil {
			t.Fatalf("unable to create request: %s", err)
		}
		req.Header.Add("Content-type", "application/json")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("unable to send request: %s", err)
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			bodyBytes, _ := io.ReadAll(resp.Body)
			t.Fatalf("response status code is not 200, got %d: %s", resp.StatusCode, string(bodyBytes))
		}

		var result map[string]interface{}
		if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
			t.Fatalf("error parsing response body: %s", err)
		}
		_, ok := result["result"].(string)
		if !ok {
			t.Fatalf("expected 'result' field to be a string, got %T", result["result"])
		}
	})
}

// Test connection with different IP type
func TestCloudSQLMySQLIpConnection(t *testing.T) {
	sourceConfig := getCloudSQLMySQLVars(t)

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
			err := tests.RunSourceConnectionTest(t, sourceConfig, CloudSQLMySQLToolType)
			if err != nil {
				t.Fatalf("Connection test failure: %s", err)
			}
		})
	}
}

func TestCloudSQLMySQLIAMConnection(t *testing.T) {
	getCloudSQLMySQLVars(t)
	// service account email used for IAM should trim the suffix
	serviceAccountEmail, _, _ := strings.Cut(tests.ServiceAccountEmail, "@")

	noPassSourceConfig := map[string]any{
		"type":     CloudSQLMySQLSourceType,
		"project":  CloudSQLMySQLProject,
		"instance": CloudSQLMySQLInstance,
		"region":   CloudSQLMySQLRegion,
		"database": CloudSQLMySQLDatabase,
		"user":     serviceAccountEmail,
	}
	noUserSourceConfig := map[string]any{
		"type":     CloudSQLMySQLSourceType,
		"project":  CloudSQLMySQLProject,
		"instance": CloudSQLMySQLInstance,
		"region":   CloudSQLMySQLRegion,
		"database": CloudSQLMySQLDatabase,
		"password": "random",
	}
	noUserNoPassSourceConfig := map[string]any{
		"type":     CloudSQLMySQLSourceType,
		"project":  CloudSQLMySQLProject,
		"instance": CloudSQLMySQLInstance,
		"region":   CloudSQLMySQLRegion,
		"database": CloudSQLMySQLDatabase,
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
	for i, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
			defer cancel()

			// Generate a UNIQUE source name for this test case.
			// It ensures the app registers a unique driver name
			// like "cloudsql-mysql-iam-test-0", preventing conflicts.
			uniqueSourceName := fmt.Sprintf("iam-test-%d", i)

			// Construct the tools config manually (Copied from RunSourceConnectionTest)
			toolsFile := map[string]any{
				"sources": map[string]any{
					uniqueSourceName: tc.sourceConfig,
				},
				"tools": map[string]any{
					"my-simple-tool": map[string]any{
						"type":        CloudSQLMySQLToolType,
						"source":      uniqueSourceName,
						"description": "Simple tool to test end to end functionality.",
						"statement":   "SELECT 1;",
					},
				},
			}

			// Start the Toolbox Command
			args := []string{"--enable-api"}
			cmd, cleanup, err := tests.StartCmd(ctx, toolsFile, args...)
			if err != nil {
				t.Fatalf("command initialization returned an error: %s", err)
			}
			defer cleanup()

			// Wait for the server to be ready
			waitCtx, waitCancel := context.WithTimeout(ctx, 10*time.Second)
			defer waitCancel()

			out, err := testutils.WaitForString(waitCtx, regexp.MustCompile(`Server ready to serve`), cmd.Out)
			if err != nil {
				if tc.isErr {
					return
				}
				t.Logf("toolbox command logs: \n%s", out)
				t.Fatalf("Connection test failure: toolbox didn't start successfully: %s", err)
			}

			if tc.isErr {
				t.Fatalf("Expected error but test passed.")
			}
		})
	}
}
