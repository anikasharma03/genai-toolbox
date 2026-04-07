---
title: "Knowledge Catalog"
type: docs
description: "Details of the Knowledge Catalog(formerly known as Dataplex) prebuilt configuration."
---

## Knowledge Catalog

*   `--prebuilt` value: `knowledge-catalog`
*   **Environment Variables:**
    *   `KNOWLEDGE_CATALOG_PROJECT`: The GCP project ID.
*   **Permissions:**
    *   **Knowledge Catalog Reader** (`roles/dataplex.viewer`) to search and look up
        entries.
    *   **Knowledge Catalog Editor** (`roles/dataplex.editor`) to modify entries.
*   **Tools:**
    *   `search_entries`: Searches for entries in Knowledge Catalog.
    *   `lookup_entry`: Retrieves a specific entry from Knowledge Catalog.
    *   `search_aspect_types`: Finds aspect types relevant to the query.
