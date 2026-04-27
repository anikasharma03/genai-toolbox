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

package alloydbsearchcatalog

import (
	dataplexapi "cloud.google.com/go/dataplex/apiv1"
	dataplexpb "cloud.google.com/go/dataplex/apiv1/dataplexpb"
	"context"
	"fmt"
	"github.com/goccy/go-yaml"
	"github.com/googleapis/mcp-toolbox/internal/embeddingmodels"
	"github.com/googleapis/mcp-toolbox/internal/sources"
	"github.com/googleapis/mcp-toolbox/internal/tools"
	"github.com/googleapis/mcp-toolbox/internal/util"
	"github.com/googleapis/mcp-toolbox/internal/util/parameters"
	"google.golang.org/api/iterator"
	"net/http"
	"strings"
)

const resourceType string = "alloydb-search-catalog"

func init() {
	if !tools.Register(resourceType, newConfig) {
		panic(fmt.Sprintf("tool type %q already registered", resourceType))
	}
}

func newConfig(ctx context.Context, name string, decoder *yaml.Decoder) (tools.ToolConfig, error) {
	actual := Config{Name: name}
	if err := decoder.DecodeContext(ctx, &actual); err != nil {
		return nil, err
	}
	return actual, nil
}

type compatibleSource interface {
	ProjectID() string
	UseClientAuthorization() bool
	GetCatalogClient(ctx context.Context, tokenString string) (*dataplexapi.CatalogClient, error)
}

type Config struct {
	Name         string                 `yaml:"name" validate:"required"`
	Type         string                 `yaml:"type" validate:"required"`
	Source       string                 `yaml:"source" validate:"required"`
	Description  string                 `yaml:"description"`
	AuthRequired []string               `yaml:"authRequired"`
	Annotations  *tools.ToolAnnotations `yaml:"annotations,omitempty"`
}

// validate interface
var _ tools.ToolConfig = Config{}

func (cfg Config) ToolConfigType() string {
	return resourceType
}

func (cfg Config) Initialize(srcs map[string]sources.Source) (tools.Tool, error) {
	prompt := parameters.NewStringParameter("prompt", "Prompt representing search intention. Do not rewrite the prompt.")
	databaseIds := parameters.NewArrayParameterWithDefault("databaseIds", []any{}, "Array of database IDs.", parameters.NewStringParameter("databaseId", "The IDs of the alloydb database."))
	projectIds := parameters.NewArrayParameterWithDefault("projectIds", []any{}, "Array of project IDs.", parameters.NewStringParameter("projectId", "The IDs of the GCP project."))
	types := parameters.NewArrayParameterWithDefault("types", []any{}, "Array of data types to filter by.", parameters.NewStringParameter("type", "The type of the data. Accepted values are: DATABASE, TABLE, VIEW, CLUSTER, INSTANCE, DATABASE_SCHEMA."))
	pageSize := parameters.NewIntParameterWithDefault("pageSize", 5, "Number of results in the search page.")
	params := parameters.Parameters{prompt, databaseIds, projectIds, types, pageSize}

	if cfg.Description == "" {
		cfg.Description = "Searches for data assets (eg. AlloyDB tables, views, schemas, databases, clusters or instances) in Catalog based on the provided search query."
	}

	t := Tool{
		Config:     cfg,
		Parameters: params,
		manifest: tools.Manifest{
			Description:  cfg.Description,
			Parameters:   params.Manifest(),
			AuthRequired: cfg.AuthRequired,
		},
	}
	return t, nil
}

type Tool struct {
	Config
	Parameters parameters.Parameters
	manifest   tools.Manifest
}

func (t Tool) ToConfig() tools.ToolConfig {
	return t.Config
}

func (t Tool) Authorized(verifiedAuthServices []string) bool {
	return tools.IsAuthorized(t.AuthRequired, verifiedAuthServices)
}

func (t Tool) RequiresClientAuthorization(resourceMgr tools.SourceProvider) (bool, error) {
	source, err := tools.GetCompatibleSource[compatibleSource](resourceMgr, t.Source, t.Name, t.Type)
	if err != nil {
		return false, err
	}
	return source.UseClientAuthorization(), nil
}

func constructSearchQueryHelper(predicate string, operator string, items []string) string {
	if len(items) == 0 {
		return ""
	}

	if len(items) == 1 {
		return predicate + operator + items[0]
	}

	var builder strings.Builder
	builder.WriteString("(")
	for i, item := range items {
		if i > 0 {
			builder.WriteString(" OR ")
		}
		builder.WriteString(predicate)
		builder.WriteString(operator)
		builder.WriteString(item)
	}
	builder.WriteString(")")
	return builder.String()
}

func constructSearchQuery(projectIds []string, databaseIds []string, types []string) string {
	queryParts := []string{}

	if clause := constructSearchQueryHelper("projectid", "=", projectIds); clause != "" {
		queryParts = append(queryParts, clause)
	}

	if clause := constructSearchQueryHelper("parent", ":", databaseIds); clause != "" {
		queryParts = append(queryParts, clause)
	}

	if clause := constructSearchQueryHelper("type", "=", types); clause != "" {
		queryParts = append(queryParts, clause)
	}
	queryParts = append(queryParts, "system=AlloyDB")

	return strings.Join(queryParts, " AND ")
}

type Response struct {
	DisplayName   string
	Description   string
	Type          string
	Resource      string
	DataplexEntry string
}

var typeMap = map[string]string{
	"alloydb-cluster":  "CLUSTER",
	"alloydb-database": "DATABASE",
	"alloydb-instance": "INSTANCE",
	"alloydb-table":    "TABLE",
	"alloydb-view":     "VIEW",
	"alloydb-schema":   "DATABASE_SCHEMA",
}

func ExtractType(resourceString string) string {
	lastIndex := strings.LastIndex(resourceString, "/")
	if lastIndex == -1 {
		return resourceString
	}
	return typeMap[resourceString[lastIndex+1:]]
}

func (t Tool) Invoke(ctx context.Context, resourceMgr tools.SourceProvider, params parameters.ParamValues, accessToken tools.AccessToken) (any, util.ToolboxError) {
	source, err := tools.GetCompatibleSource[compatibleSource](resourceMgr, t.Source, t.Name, t.Type)
	if err != nil {
		return nil, util.NewClientServerError("source used is not compatible with the tool", http.StatusInternalServerError, err)
	}

	paramsMap := params.AsMap()
	pageSize := int32(paramsMap["pageSize"].(int))
	prompt, _ := paramsMap["prompt"].(string)

	projectIdSlice, err := parameters.ConvertAnySliceToTyped(paramsMap["projectIds"].([]any), "string")
	if err != nil {
		return nil, util.NewAgentError(fmt.Sprintf("can't convert projectIds to array of strings: %s", err), err)
	}
	projectIds := projectIdSlice.([]string)

	databaseIdSlice, err := parameters.ConvertAnySliceToTyped(paramsMap["databaseIds"].([]any), "string")
	if err != nil {
		return nil, util.NewAgentError(fmt.Sprintf("can't convert databaseIds to array of strings: %s", err), err)
	}
	databaseIds := databaseIdSlice.([]string)

	typesSlice, err := parameters.ConvertAnySliceToTyped(paramsMap["types"].([]any), "string")
	if err != nil {
		return nil, util.NewAgentError(fmt.Sprintf("can't convert types to array of strings: %s", err), err)
	}
	types := typesSlice.([]string)

	req := &dataplexpb.SearchEntriesRequest{
		Query:          fmt.Sprintf("%s %s", prompt, constructSearchQuery(projectIds, databaseIds, types)),
		Name:           fmt.Sprintf("projects/%s/locations/global", source.ProjectID()),
		PageSize:       pageSize,
		SemanticSearch: true,
	}

	var tokenStr string
	if source.UseClientAuthorization() {
		tokenStr, err = accessToken.ParseBearerToken()
		if err != nil {
			return nil, util.NewClientServerError("failed to parse access token", http.StatusInternalServerError, err)
		}
	}
	catalogClient, err := source.GetCatalogClient(ctx, tokenStr)
	if err != nil {
		return nil, util.NewClientServerError("failed to get catalog client", http.StatusInternalServerError, err)
	}

	it := catalogClient.SearchEntries(ctx, req)
	if it == nil {
		return nil, util.NewClientServerError(fmt.Sprintf("failed to create search entries iterator for project %q", source.ProjectID()), http.StatusInternalServerError, nil)
	}

	var results []Response
	for {
		if pageSize > 0 && len(results) >= int(pageSize) {
			break
		}
		entry, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, util.ProcessGcpError(err)
		}
		entrySource := entry.DataplexEntry.GetEntrySource()
		resp := Response{
			DisplayName:   entrySource.GetDisplayName(),
			Description:   entrySource.GetDescription(),
			Type:          ExtractType(entry.DataplexEntry.GetEntryType()),
			Resource:      entrySource.GetResource(),
			DataplexEntry: entry.DataplexEntry.GetName(),
		}
		results = append(results, resp)
	}
	return results, nil
}

func (t Tool) EmbedParams(ctx context.Context, paramValues parameters.ParamValues, embeddingModelsMap map[string]embeddingmodels.EmbeddingModel) (parameters.ParamValues, error) {
	return parameters.EmbedParams(ctx, t.Parameters, paramValues, embeddingModelsMap, nil)
}

func (t Tool) Manifest() tools.Manifest {
	return t.manifest
}

func (t Tool) GetName() string {
	return t.Name
}

func (t Tool) GetDescription() string {
	return t.Description
}

func (t Tool) GetAuthRequired() []string {
	return t.AuthRequired
}

func (t Tool) GetAnnotations() *tools.ToolAnnotations {
	return tools.GetAnnotationsOrDefault(t.Annotations, tools.NewReadOnlyAnnotations)
}

func (t Tool) GetScopesRequired() []string {
	return nil
}

func (t Tool) GetAuthTokenHeaderName(resourceMgr tools.SourceProvider) (string, error) {
	return "", nil
}

func (t Tool) GetParameters() parameters.Parameters {
	return t.Parameters
}
