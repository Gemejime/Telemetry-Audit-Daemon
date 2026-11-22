package openapi

import _ "embed"

// SpecYAML holds the embedded OpenAPI 3.1.0 YAML specification for /openapi.yaml.
//
//go:embed openapi.yaml
var SpecYAML []byte
