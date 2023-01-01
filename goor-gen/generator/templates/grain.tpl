{{ define "GrainActivator" }}

type {{ .Name }}Activation interface {
	__grain.Activation
	_{{ .Name }}Activation()
}

func New{{ .Name }}Activation(siloClient __grain.SiloClient, impl {{ qualifiedGrainType . }}) {{ .Name }}Activation {
	return &_{{ .Name }}Activation{
		siloClient: siloClient,
		impl:       impl,
	}
}

type {{ .Name }}Activator interface {
	Activate(ctx context.Context, identity __grain.Identity, services __services.CoreGrainServices) ({{ qualifiedGrainType . }}, error)
}

func Register{{ .Name }}Activator(registrar __descriptor.Registrar, activator {{ .Name }}Activator) {
	registrar.RegisterV2(
		"{{ .Name }}",
		func(ctx context.Context, identity __grain.Identity,
			services __services.CoreGrainServices) (__grain.Activation, error) {
			a, err := activator.Activate(ctx, identity, services)
			if err != nil {
				return nil, err
			}
			return New{{ .Name }}Activation(services.SiloClient(), a), nil
		},
	)
}
{{ end }}


{{ define "grainActivationInvokeMethodCase" }}
{{ template "decodeMethodArgs" . }}

{{ if not (isOneWay .) }}
{{ range $index, $element := stripLastParam .Returns -}}
out{{ $index }},
{{- end -}} err :=
{{- end -}}

__s.impl.{{ .Name }}(ctx, {{ template "callArgsWithoutCtx" . }})

{{ if isOneWay . }}
return nil

{{ else }}
if err != nil {
  return err
}

{{ range $index, $element := stripLastParam .Returns }}
{{ if eq .SerializerType "Interface" }}
if err := respSerializer.Interface(out{{ $index }}); err != nil {
    return err
}
{{ else if eq .SerializerType "Text" }}
if text, err := out{{ $index }}.{{- if .IsObserver -}}GetIdentity().{{- end -}}MarshalText(); err != nil {
    return err
} else {
respSerializer.String(string(text))
}
{{ else }}
respSerializer.{{ .SerializerType }}({{.BasicSerializeTypeName}}(out{{ $index }}))
{{ end }}
{{ end }}
return nil
{{ end }}
{{ end }}

{{ define "GrainActivation" }}
type _{{ .Name }}Activation struct {
	siloClient __grain.SiloClient
	impl       {{ qualifiedGrainType . }}
}

func (__s *_{{ .Name }}Activation) _{{ .Name }}Activation() {}

func (__s *_{{ .Name }}Activation) InvokeMethod(ctx context.Context, method string, sender __grain.Identity,
	dec __grain.Deserializer, respSerializer __grain.Serializer) error {
	siloClient := __s.siloClient
	_ = siloClient
	
	switch method {
    {{ range .Methods }}
    case "{{ .Name }}":
    	{{ template "grainActivationInvokeMethodCase" . }}
    {{ end }}
	}
	return errors.New("unknown method")
}
{{ end }}

{{ define "GrainClient" }}
{{ $grainType := .Name }}

type _{{ $grainType }}Client struct {
	__grain.Identity
	siloClient __grain.SiloClient
}

{{ if not .IsObserver }}
func {{ $grainType }}Ref(siloClient __grain.SiloClient, id string) {{ qualifiedGrainType . }} {
	return &_{{ $grainType }}Client {
		siloClient: siloClient,
		Identity: __grain.Identity{
			GrainType: "{{ $grainType }}",
			ID:        id,
		},
	}
}
{{ end }}

func {{ $grainType }}RefFromIdentity(siloClient __grain.SiloClient, id __grain.Identity) {{ qualifiedGrainType . }} {
	return &_{{ $grainType }}Client {
		siloClient: siloClient,
		Identity: id,
	}
}

{{ range .Methods }}
func (__c *_{{ $grainType }}Client) {{ .Name }}({{ template "methodParameters" . }}) {{ template "methodReturns" . }} {
	{{ if isOneWay . }}
	__c.siloClient.InvokeOneWayMethod(ctx, []__grain.Identity{__c.Identity},
	{{- else -}}
	f := __c.siloClient.InvokeMethodV2(ctx, __c.Identity, 
	{{- end -}}
	"{{ .Name }}", func(respSerializer __grain.Serializer) error {
		{{ template "serializerFuncBody" . }}
	})
	{{ if not (isOneWay .) }}
	{{ range $index,$element := stripLastParam .Returns }}
	var out{{ $index }} {{ if .IsPointer }}*{{- end -}}{{qualifiedArgType .}}
	{{ end }}
	resp, err := f.Await(ctx)
	if err != nil {
		return {{ range $index,$element := stripLastParam .Returns -}}
		{{ if .IsPointer }}nil{{ else }}out{{ $index }}{{ end }},
		{{- end }} err
	}

	err = resp.Get(func(dec __grain.Deserializer) error {
	var err error
	_ = err
	{{ range $index,$element := stripLastParam .Returns -}}
		{{ if .IsObserver }}
			out{{ $index }}Str, err := dec.String()
			if err != nil {
				return err
			}
			out{{ $index }}Identity := __grain.Identity{}
			err = out{{ $index }}Identity.UnmarshalText([]byte(out{{ $index }}Str))
			if err != nil {
				return err
			}

			out{{ $index }} = {{ qualifiedArgType . }}RefFromIdentity(s.siloClient, arg{{.Name}}Identity)
		{{ else if eq .SerializerType "Interface" }}
		{{ if .IsPointer }}
		out{{ $index }} = new({{qualifiedArgType .}})
		{{ end }}
		err = dec.Interface(out{{ $index }})
		if err != nil {
			return err
		}
		{{ else if eq .SerializerType "Text" }}
		{{ if .IsPointer }}
		out{{ $index }} = new({{qualifiedArgType .}})
		{{ end }}
		out{{ $index }}Str, err := dec.String()
		if err != nil {
			return err
		}
		err = out{{ $index }}.UnmarshalText([]byte(out{{ $index }}Str))
		if err != nil {
			return err
		}
		{{ else }}
		out{{ $index }}Uncasted, err := dec.{{ .SerializerType }}()
		if err != nil {
			return err
		}
		out{{ $index }} = {{ .BasicTypeName }}(out{{ $index }}Uncasted)
		{{ end }}
	{{ end }}
		return err
	})
	if err != nil {
		return {{ range $index,$element := stripLastParam .Returns -}}
		{{ if .IsPointer }}nil{{ else }}out{{ $index }}{{ end }},
		{{- end }} err
	}
	return {{ range $index,$element := stripLastParam .Returns -}}out{{ $index }},{{- end }} err
	{{ end }}
}
{{ end }}

{{ end }}

{{ define "Grain" }}
{{ template "GrainActivator" . }}
{{ template "GrainActivation" . }}
{{ template "GrainClient" . }}
{{ end }}
