{{ define "GrainActivator" }}

type {{ .Name }}Activation interface {
	grain.Activation
	_{{ .Name }}Activation()
}

func New{{ .Name }}Activation(siloClient grain.SiloClient, impl {{ qualifiedGrainType . }}) {{ .Name }}Activation {
	return &_{{ .Name }}Activation{
		siloClient: siloClient,
		impl:       impl,
	}
}

type {{ .Name }}RoomGrainActivator interface {
	Activate(ctx context.Context, identity grain.Identity, services services.CoreGrainServices) ({{ qualifiedGrainType . }}, error)
}

func Register{{ .Name }}Activator(registrar descriptor.Registrar, activator {{ .Name }}Activator) {
	registrar.RegisterV2(
		"{{ .Name }}",
		func(ctx context.Context, identity grain.Identity,
			services services.CoreGrainServices) (grain.Activation, error) {
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
{{- range (slice .Parameters 1) -}}
{{ if .IsObserver }}
arg{{.Name}}Str, err := dec.String()
if err != nil {
	return err
}
arg{{.Name}}Identity := grain.Identity{}
err = arg{{.Name}}Identity.UnmarshalText([]byte(arg{{.Name}}Str))
if err != nil {
	return err
}

arg{{.Name}} := {{ qualifiedArgType . }}Ref(s.siloClient, arg{{.Name}}Identity)
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
arg{{.Name}}, err := dec.{{ .SerializerType }}()
if err != nil {
	return err
}
{{ end }}
{{- end -}}

{{ if not (isOneWay .) }}
{{ range $index, $element := stripLastParam .Returns -}}
out{{ $index }},
{{- end -}} err :=
{{- end -}}
s.impl.{{ .Name }}(ctx,
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
{{- end -}})

{{ if isOneWay . }}
return nil

{{ else }}

{{ range $index, $element := stripLastParam .Returns }}
{{ if eq .SerializerType "Interface" }}
if err := respSerializer.Interface(out{{ $index }}); err != nil {
    return err
}
{{ else if eq .SerializerType "Text" }}
if text, err := out{{ $index }}.MarshalText(); err != nil {
    return err
} else {
respSerializer.String(string(text))
}
{{ else }}
respSerializer.{{ .SerializerType }}(out{{ $index }})
{{ end }}
{{ end }}
return nil
{{ end }}
{{ end }}

{{ define "GrainActivation" }}
type _{{ .Name }}Activation struct {
	siloClient grain.SiloClient
	impl       {{ qualifiedGrainType . }}
}

func (s *_{{ .Name }}Activation) _{{ .Name }}Activation() {}

func (s *_{{ .Name }}Activation) InvokeMethod(ctx context.Context, method string, sender grain.Identity,
	d grain.Deserializer, respSerializer grain.Serializer) error {
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
	grain.Identity
	siloClient grain.SiloClient
}

func {{ $grainType }}Ref(siloClient grain.SiloClient, id string) {{ qualifiedGrainType . }} {
	return _{{ $grainType }}Client {
		siloClient: siloClient,
		Identity: grain.Identity{
			GrainType: "{{ $grainType }}",
			ID:        id,
		},
	}
}

{{ range .Methods }}
func (c *_{{ $grainType }}Client) {{ .Name }}(ctx,
	{{- range (slice .Parameters 1) -}}{{- .Name }} {{if .IsPointer -}}*{{- end -}}{{ qualifiedArgType . -}},{{- end -}})
	{{- if not (isOneWay .) -}}
		({{- range (stripLastParam .Returns) -}}
			{{if .IsPointer -}}*{{- end -}}{{- qualifiedArgType . -}},
		{{- end -}} error)
	{{- end -}} {
	{{ if isOneWay . }}
	c.siloClient.InvokeOneWayMethod
	{{- else -}}
	f := c.siloClient.InvokeMethodV2
	{{- end -}}
	(ctx, c.Identity, "{{ $grainType }}", "{{ .Name }}", func(respSerializer grain.Serializer) error {
		{{- range (slice .Parameters 1) }}
			{{ if eq .SerializerType "Interface" }}
			if err := respSerializer.Interface({{ .Name }}); err != nil {
				return err
			}
			{{ else if eq .SerializerType "Text" }}
			if text, err := {{ .Name }}.MarshalText(); err != nil {
				return err
			} else {
				respSerializer.String(string(text))
			}
			{{ else }}
			respSerializer.{{ .SerializerType }}({{ .Name }})
			{{ end }}
		{{- end }}
		return nil
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

	err = resp.Get(func(d grain.Deserializer) error {
	var err error
	{{ range $index,$element := stripLastParam .Returns -}}
		{{ if .IsObserver }}
			out{{ $index }}Str, err := dec.String()
			if err != nil {
				return err
			}
			out{{ $index }}Identity := grain.Identity{}
			err = out{{ $index }}Identity.UnmarshalText([]byte(out{{ $index }}Str))
			if err != nil {
				return err
			}

			out{{ $index }} = {{ qualifiedArgType . }}Ref(s.siloClient, arg{{.Name}}Identity)
		{{ else if eq .SerializerType "Interface" }}
		out{{ $index }} = new({{qualifiedArgType .}})
		err = dec.Interface(out{{ $index }})
		if err != nil {
			return err
		}
		{{ else if eq .SerializerType "Text" }}
		out{{ $index }} = new({{qualifiedArgType .}})
		out{{ $index }}Str, err := dec.String()
		if err != nil {
			return err
		}
		err = out{{ $index }}.UnmarshalText([]byte(out{{ $index }}Str))
		if err != nil {
			return err
		}
		{{ else }}
		out{{ $index }}, err = dec.{{ .SerializerType }}()
		if err != nil {
			return err
		}
		{{ end }}
	{{ end }}
		return err
	})
	if err != nil {
		return {{ range $index,$element := stripLastParam .Returns -}}
		{{ if .IsPointer }}nil{{ else }}out{{ $index }}{{ end }},
		{{- end }} err
	}
	{{ end }}
}
{{ end }}

{{ end }}

{{ define "Grain" }}
{{ template "GrainActivator" . }}
{{ template "GrainActivation" . }}
{{ template "GrainClient" . }}
{{ end }}
