package {{ .PackageName }}

import (
    "context"
    "errors"
    {{ range $index, $element := .Imports }}
    {{if not $element.IsUserImport }}{{ $element.Name }} "{{ $element.PackagePath }}"{{ end }}
    {{- end }}

    {{ range $index, $element := .Imports }}
    {{if $element.IsUserImport }}{{ $element.Name }} "{{ $element.PackagePath }}"{{ end }}
    {{- end }}
)

