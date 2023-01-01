{{ define "serializerFuncBody" }}
{{ range (slice .Parameters 1) }}
	{{ if eq .SerializerType "Interface" }}
	if err := respSerializer.Interface({{ .Name }}); err != nil {
		return err
	}
	{{ else if eq .SerializerType "Text" }}
	if text, err := {{ .Name }}.{{- if .IsObserver -}}GetIdentity().{{- end -}}MarshalText(); err != nil {
		return err
	} else {
		respSerializer.String(string(text))
	}
	{{ else }}
	respSerializer.{{ .SerializerType }}({{.BasicSerializeTypeName}}({{ .Name }}))
	{{ end }}
{{ end }}
return nil
{{ end }}


{{ define "methodParameters" -}}
ctx context.Context, {{template "methodParametersWithoutCtx" .}}
{{- end }}


{{ define "methodParametersWithoutCtx" -}}
{{- range (slice .Parameters 1) -}}{{- .Name }} {{if .IsPointer -}}*{{- end -}}{{ qualifiedArgType . -}},{{- end -}}
{{- end }}


{{ define "methodReturns" }}
{{- if not (isOneWay .) -}}
		({{- range (stripLastParam .Returns) -}}
			{{if .IsPointer -}}*{{- end -}}{{- qualifiedArgType . -}},
		{{- end -}} error)
	{{- end -}}
{{ end }}


{{ define "decodeMethodArgs" }}
{{- range (slice .Parameters 1) -}}
{{ if .IsObserver }}
arg{{.Name}}Str, err := dec.String()
if err != nil {
	return err
}
arg{{.Name}}Identity := __grain.Identity{}
err = arg{{.Name}}Identity.UnmarshalText([]byte(arg{{.Name}}Str))
if err != nil {
	return err
}

arg{{.Name}} := {{ qualifiedArgType . }}Ref(siloClient, arg{{.Name}}Identity.ID)
{{ else if eq .SerializerType "Interface" }}
arg{{ .Name }} := new({{qualifiedArgType .}})
err = dec.Interface(arg{{ .Name }})
if err != nil {
	return err
}
{{ else if eq .SerializerType "Text" }}
arg{{ .Name }} := new({{qualifiedArgType .}})
arg{{ .Name }}Str, err := dec.String()
if err != nil {
	return err
}
err = arg{{ .Name }}.UnmarshalText([]byte(arg{{ .Name }}Str))
if err != nil {
	return err
}
{{ else }}
arg{{ .Name }}Uncasted, err := dec.{{ .SerializerType }}()
if err != nil {
	return err
}
arg{{ .Name }} := {{ .BasicTypeName }}(arg{{ .Name }}Uncasted)
{{ end }}
{{- end -}}
{{ end }}

{{ define "callArgsWithoutCtx" }}
{{- range (slice .Parameters 1) -}}
{{ if .IsObserver }}
arg{{ .Name }},
{{ else if .IsBasic}}
{{ if .IsPointer }}
&arg{{ .Name }},
{{ else }}
arg{{ .Name }},
{{ end }}
{{ else }}
{{ if .IsPointer }}
arg{{ .Name }},
{{ else }}
*arg{{ .Name }},
{{ end }}
{{ end }}
{{- end -}}
{{ end }}